/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.oracle;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.TransactionIsolationLevel;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Configuration for Oracle Database Connector
 */
public class OracleConnectorConfig extends AbstractDBSpecificConnectorConfig {

  private static final String TIME_ZONE_AS_REGION_PROPERTY = "oracle.jdbc.timezoneAsRegion";
  private static final String INTERNAL_LOGON_PROPERTY = "internal_logon";
  private static final String ROLE_NORMAL = "normal";

  public OracleConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
                               String connectionArguments, String database) {
    this(host, port, user, password, jdbcPluginName, connectionArguments, null, database);
  }

  public OracleConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
                               String connectionArguments, String connectionType, String database) {
    this(host, port, user, password, jdbcPluginName, connectionArguments, connectionType, database, null, null, null,
         null, null, null);
  }

  public OracleConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
                               String connectionArguments, String connectionType, String database,
                               String role, Boolean useSSL, @Nullable Boolean treatAsOldTimestamp,
                               @Nullable Boolean treatPrecisionlessNumAsDeci,
                               @Nullable Boolean treatTimestampLTZAsTimestamp,
                               @Nullable Boolean enableXmlType) {

    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
    this.connectionType = connectionType;
    this.database = database;
    this.role = role;
    this.useSSL = useSSL;
    this.treatAsOldTimestamp = treatAsOldTimestamp;
    this.treatPrecisionlessNumAsDeci = treatPrecisionlessNumAsDeci;
    this.treatTimestampLTZAsTimestamp = treatTimestampLTZAsTimestamp;
    this.enableXmlType = enableXmlType;
  }

  @Override
  public String getConnectionString() {
    return OracleConstants.getConnectionString(connectionType, host, getPort(), database, useSSL);
  }

  @Name(OracleConstants.CONNECTION_TYPE)
  @Description("Whether to use an SID or Service Name when connecting to the database.")
  private String connectionType;

  @Name(OracleConstants.ROLE)
  @Description("Login role of the user when connecting to the database.")
  @Nullable
  private String role;

  @Name(OracleConstants.NAME_DATABASE)
  @Description("SID or Service Name to connect to")
  @Macro
  private String database;

  @Name(OracleConstants.USE_SSL)
  @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
  @Nullable
  public Boolean useSSL;

  @Name(OracleConstants.TREAT_AS_OLD_TIMESTAMP)
  @Description("A hidden field to handle timestamp as CDAP's timestamp micros or string as per old behavior.")
  @Nullable
  public Boolean treatAsOldTimestamp;

  @Name(OracleConstants.TREAT_PRECISIONLESSNUM_AS_DECI)
  @Description("A hidden field to handle precision less number as CDAP's decimal per old behavior.")
  @Nullable
  public Boolean treatPrecisionlessNumAsDeci;

  @Name(OracleConstants.TREAT_TIMESTAMP_LTZ_AS_TIMESTAMP)
  @Description("A hidden field to handle mapping of Oracle Timestamp_LTZ data type.")
  @Nullable
  public Boolean treatTimestampLTZAsTimestamp;

  @Name(OracleConstants.ENABLE_XML_TYPE)
  @Description("A hidden field to handle mapping of Oracle XML type.")
  @Nullable
  public Boolean enableXmlType;


  @Override
  protected int getDefaultPort() {
    return 1521;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public String getRole() {
    return role == null ? "normal" : role;
  }

  public String getDatabase() {
    return database;
  }

  public Boolean getSSlMode() {
    // return false if useSSL is null, otherwise return its value
    return useSSL != null && useSSL;
  }

  public Boolean getTreatAsOldTimestamp() {
    return Boolean.TRUE.equals(treatAsOldTimestamp);
  }

  public Boolean getTreatPrecisionlessNumAsDeci() {
    return Boolean.TRUE.equals(treatPrecisionlessNumAsDeci);
  }

  public Boolean getTreatTimestampLTZAsTimestamp() {
    return Boolean.TRUE.equals(treatTimestampLTZAsTimestamp);
  }

  public Boolean getXmlTypeEnabled() {
    return Boolean.TRUE.equals(enableXmlType);
  }

  @Override
  public Properties getConnectionArgumentsProperties() {
    Properties prop = super.getConnectionArgumentsProperties();
    // To solve the "ORA-01882: timezone region not found" issue, see:
    // https://stackoverflow.com/questions/9156379/ora-01882-timezone-region-not-found
    prop.put(TIME_ZONE_AS_REGION_PROPERTY, "false");
    prop.put(INTERNAL_LOGON_PROPERTY, getRole());
    return prop;
  }

  @Override
  public String getTransactionIsolationLevel() {
    //if null default to the highest isolation level possible
    if (transactionIsolationLevel == null) {
      transactionIsolationLevel = TransactionIsolationLevel.Level.TRANSACTION_SERIALIZABLE.name();
    }
    //To solve the problem of ORA-08178: illegal SERIALIZABLE clause specified for user INTERNAL
    //This ensures that the role is mapped to the right serialization level, even w/ incorrect user input
    //if role is SYSDBA or SYSOP it will map to read_committed. else serialized
    return (!getRole().equals(ROLE_NORMAL)) ? TransactionIsolationLevel.Level.TRANSACTION_READ_COMMITTED.name() :
      TransactionIsolationLevel.Level.valueOf(transactionIsolationLevel).name();
  }

  @Override
  public boolean canConnect() {
    return super.canConnect() && !containsMacro(OracleConstants.NAME_DATABASE);
  }

}
