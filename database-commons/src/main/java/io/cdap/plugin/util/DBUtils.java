/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.util;

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.JDBCDriverShim;
import io.cdap.plugin.db.batch.config.DatabaseConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * Utility methods for Database plugins shared by Database plugins.
 */
public final class DBUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);

  public static final Calendar PURE_GREGORIAN_CALENDAR = createPureGregorianCalender();

  // Java by default uses October 15, 1582 as a Gregorian cut over date.
  // Any timestamp created with time less than this cut over date is treated as Julian date.
  // This causes old dates from database such as 0001-01-01 01:00:00 mapped to 0000-12-30
  // Get the pure gregorian calendar so that all dates are treated as gregorian format.
  private static Calendar createPureGregorianCalender() {
    GregorianCalendar gc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    gc.setGregorianChange(new Date(Long.MIN_VALUE));
    return gc;
  }

  /**
   * Performs any Database related cleanup
   *
   * @param driverClass the JDBC driver class
   */
  public static void cleanup(Class<? extends Driver> driverClass) {
    ClassLoader pluginClassLoader = driverClass.getClassLoader();
    if (pluginClassLoader == null) {
      // This could only be null if the classLoader is the Bootstrap/Primordial classloader. This should never be the
      // case since the driver class is always loaded from the plugin classloader.
      LOG.warn("PluginClassLoader is null. Cleanup not necessary.");
      return;
    }
    shutDownMySQLAbandonedConnectionCleanupThread(pluginClassLoader);
    unregisterOracleMBean(pluginClassLoader);
  }

  /**
   * Ensures that the JDBC Driver specified in configuration is available and can be loaded. Also registers it with
   * {@link DriverManager} if it is not already registered.
   */
  public static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> jdbcDriverClass,
                                                          String connectionString, String jdbcPluginName)
    throws IllegalAccessException, InstantiationException, SQLException {

    try {
      DriverManager.getDriver(connectionString);
      return new DriverCleanup(null);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.
      LOG.debug("Plugin Name: {}; Driver Class: {} not found. Registering JDBC driver via shim {} ",
                jdbcPluginName, jdbcDriverClass.getName(), JDBCDriverShim.class.getName());

      final JDBCDriverShim driverShim = new JDBCDriverShim(jdbcDriverClass.newInstance());
      try {
        DBUtils.deregisterAllDrivers(jdbcDriverClass);
      } catch (NoSuchFieldException | ClassNotFoundException e1) {
        LOG.error("Unable to deregister JDBC Driver class {}", jdbcDriverClass);
      }
      DriverManager.registerDriver(driverShim);
      return new DriverCleanup(driverShim);
    }
  }

  @Nullable
  public static Object transformValue(int sqlType, int precision, int scale,
                                      ResultSet resultSet, int columnIndex) throws SQLException {
    Object original = resultSet.getObject(columnIndex);
    if (original != null) {
      switch (sqlType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return ((Number) original).intValue();
        case Types.NUMERIC:
        case Types.DECIMAL:
          return (BigDecimal) original;
        case Types.DATE:
          return resultSet.getDate(columnIndex);
        case Types.TIME:
          return resultSet.getTime(columnIndex);
        case Types.TIMESTAMP:
          return resultSet.getTimestamp(columnIndex, PURE_GREGORIAN_CALENDAR);
        case Types.ROWID:
          return resultSet.getString(columnIndex);
        case Types.BLOB:
          Blob blob = (Blob) original;
          return blob.getBytes(1, (int) blob.length());
        case Types.CLOB:
          Clob clob = (Clob) original;
          return clob.getSubString(1, (int) clob.length());
      }
    }
    return original;
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  public static void deregisterAllDrivers(Class<? extends Driver> driverClass)
    throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    Field field = DriverManager.class.getDeclaredField("registeredDrivers");
    field.setAccessible(true);
    List<?> list = (List<?>) field.get(null);
    for (Object driverInfo : list) {
      Class<?> driverInfoClass = DBUtils.class.getClassLoader().loadClass("java.sql.DriverInfo");
      Field driverField = driverInfoClass.getDeclaredField("driver");
      driverField.setAccessible(true);
      Driver d = (Driver) driverField.get(driverInfo);
      if (d == null) {
        LOG.debug("Found null driver object in drivers list. Ignoring.");
        continue;
      }
      LOG.debug("Removing non-null driver object from drivers list.");
      ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
      if (registeredDriverClassLoader == null) {
        LOG.debug("Found null classloader for default driver {}. Ignoring since this may be using system classloader.",
                  d.getClass().getName());
        continue;
      }
      // Remove all objects in this list that were created using the classloader of the caller.
      if (d.getClass().getClassLoader().equals(driverClass.getClassLoader())) {
        LOG.debug("Removing default driver {} from registeredDrivers", d.getClass().getName());
        list.remove(driverInfo);
      }
    }
  }

  public static <T extends PluginConfig & DatabaseConnectionConfig> void validateJDBCPluginPipeline(
    PipelineConfigurer pipelineConfigurer, T config, String jdbcPluginId) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    if (!config.containsMacro(ConnectionConfig.USER) && !config.containsMacro(ConnectionConfig.PASSWORD) &&
      Objects.isNull(config.getUser()) && Objects.nonNull(config.getPassword())) {
      collector.addFailure("Username is required when password is given.", null)
        .withConfigProperty(ConnectionConfig.USER);
    }

    // if contains macro for jdbc plugin, just return
    if (config.containsMacro(ConnectionConfig.JDBC_PLUGIN_NAME)) {
      collector.getOrThrowException();
      return;
    }

    Class<? extends Driver> jdbcDriverClass = getDriverClass(pipelineConfigurer, config, jdbcPluginId);
    if (jdbcDriverClass == null) {
      collector.addFailure(
        String.format("Unable to load JDBC Driver class for plugin name '%s'.", config.getJdbcPluginName()),
        String.format("Ensure that the plugin '%s' of type '%s' containing the driver has been installed correctly.",
                      config.getJdbcPluginName(), ConnectionConfig.JDBC_PLUGIN_TYPE))
        .withConfigProperty(ConnectionConfig.JDBC_PLUGIN_NAME)
        .withPluginNotFound(jdbcPluginId, config.getJdbcPluginName(), ConnectionConfig.JDBC_PLUGIN_TYPE);
    }

    collector.getOrThrowException();
  }

  public static Class<? extends Driver> getDriverClass(PipelineConfigurer pipelineConfigurer,
                                                       DatabaseConnectionConfig config,
                                                       String jdbcPluginId) {
    return pipelineConfigurer.usePluginClass(
      ConnectionConfig.JDBC_PLUGIN_TYPE,
      config.getJdbcPluginName(),
      jdbcPluginId, PluginProperties.builder().build());
  }

  /**
   * Shuts down a cleanup thread com.mysql.jdbc.AbandonedConnectionCleanupThread that mysql driver fails to destroy
   * If this is not done, the thread keeps a reference to the classloader, thereby causing OOMs or too many open files
   *
   * @param classLoader the unfiltered classloader of the jdbc driver class
   */
  private static void shutDownMySQLAbandonedConnectionCleanupThread(ClassLoader classLoader) {
    try {
      Class<?> mysqlCleanupThreadClass;
      try {
        mysqlCleanupThreadClass = classLoader.loadClass("com.mysql.jdbc.AbandonedConnectionCleanupThread");
      } catch (ClassNotFoundException e) {
        // Ok to ignore, since we may not be running mysql
        LOG.trace("Failed to load MySQL abandoned connection cleanup thread class. Presuming DB App is " +
                    "not being run with MySQL and ignoring", e);
        return;
      }
      Method shutdownMethod = mysqlCleanupThreadClass.getMethod("shutdown");
      shutdownMethod.invoke(null);
      LOG.debug("Successfully shutdown MySQL connection cleanup thread.");
    } catch (Throwable e) {
      // cleanup failed, ignoring silently with a log, since not much can be done.
      LOG.warn("Failed to shutdown MySQL connection cleanup thread. Ignoring.", e);
    }
  }

  private static void unregisterOracleMBean(ClassLoader classLoader) {
    try {
      classLoader.loadClass("oracle.jdbc.driver.OracleDriver");
    } catch (ClassNotFoundException e) {
      LOG.debug("Oracle JDBC Driver not found. Presuming that the DB App is not being run with an Oracle DB. " +
                  "Not attempting to cleanup Oracle MBean.");
      return;
    }
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Hashtable<String, String> keys = new Hashtable<>();
    keys.put("type", "diagnosability");
    keys.put("name",
             classLoader.getClass().getName() + "@" + Integer.toHexString(classLoader.hashCode()).toLowerCase());
    ObjectName oracleJdbcMBeanName;
    try {
      oracleJdbcMBeanName = new ObjectName("com.oracle.jdbc", keys);
    } catch (MalformedObjectNameException e) {
      // This should never happen, since we're constructing the ObjectName correctly
      LOG.debug("Exception while constructing Oracle JDBC MBean Name. Aborting cleanup.", e);
      return;
    }
    try {
      mbs.getMBeanInfo(oracleJdbcMBeanName);
    } catch (InstanceNotFoundException e) {
      LOG.debug("Oracle JDBC MBean not found. No cleanup necessary.");
      return;
    } catch (IntrospectionException | ReflectionException e) {
      LOG.debug("Exception while attempting to retrieve Oracle JDBC MBean. Aborting cleanup.", e);
      return;
    }

    try {
      mbs.unregisterMBean(oracleJdbcMBeanName);
      LOG.debug("Oracle MBean unregistered successfully.");
    } catch (InstanceNotFoundException | MBeanRegistrationException e) {
      LOG.debug("Exception while attempting to cleanup Oracle JDBCMBean. Aborting cleanup.", e);
    }
  }

  private DBUtils() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
