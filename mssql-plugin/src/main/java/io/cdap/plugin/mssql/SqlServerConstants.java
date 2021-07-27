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

package io.cdap.plugin.mssql;

/**
 * MSSQL constants.
 */
public final class SqlServerConstants {

  private SqlServerConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  public static final String PLUGIN_NAME = "SqlServer";

  /**
   * JDBC Driver property name used to specify the SQL Server instance name to connect to. When it is not specified,
   * a connection is made to the default instance.
   */
  public static final String INSTANCE_NAME = "instanceName";

  /**
   * Description of the {@link SqlServerConstants#INSTANCE_NAME} property.
   */
  public static final String INSTANCE_NAME_DESCRIPTION = "SQL Server instance name to connect to. When it is not " +
    "specified, a connection is made  to the default instance. For the case where both the instanceName and port are " +
    "specified, see the notes for port.";

  /**
   * JDBC Driver property name used to specify the number of seconds to wait before a timeout has occurred on a query.
   * The default value is -1, which means infinite timeout. Setting this to 0 also implies to wait indefinitely.
   */
  public static final String QUERY_TIMEOUT = "queryTimeout";

  /**
   * Description of the {@link SqlServerConstants#QUERY_TIMEOUT} property.
   */
  public static final String QUERY_TIMEOUT_DESCRIPTION = "Number of seconds to wait before a timeout has occurred on " +
    "a query. The default value is -1, which means infinite timeout. Setting this to 0 also implies to wait " +
    "indefinitely.";

  /**
   * JDBC Driver property name used to specify which SQL authentication method to use for connection.
   */
  public static final String AUTHENTICATION = "authenticationType";

  /**
   * Description of the {@link SqlServerConstants#AUTHENTICATION} property.
   */
  public static final String AUTHENTICATION_DESCRIPTION = "Indicates which authentication method will be used " +
    "for the connection. Use 'SQL Login'. to connect to a SQL Server using username and password properties. " +
    "Use 'Active Directory Password' to connect to an Azure SQL Database/Data Warehouse using an Azure AD principal " +
    "name and password.";

  /**
   * JDBC Driver property name used to specify the application workload type when connecting to a server.
   * Possible values are 'ReadWrite' and 'ReadOnly'.
   */
  public static final String APPLICATION_INTENT = "applicationIntent";

  /**
   * Description of the {@link SqlServerConstants#APPLICATION_INTENT} property.
   */
  public static final String APPLICATION_INTENT_DESCRIPTION = "Declares the application workload type when " +
    "connecting to a server.";

  /**
   * JDBC Driver property name used to specify the number of seconds the driver should wait before timing out a failed
   * connection. A zero value indicates that the timeout is the default system timeout, which is specified as 15 seconds
   * by default. A non-zero value is the number of seconds the driver should wait before timing out a failed connection.
   */
  public static final String CONNECT_TIMEOUT = "loginTimeout";

  /**
   * Description of the {@link SqlServerConstants#CONNECT_TIMEOUT} property.
   */
  public static final String CONNECT_TIMEOUT_DESCRIPTION = "Time in seconds to wait for a connection to the server " +
    "before terminating the attempt and generating an error. A zero value indicates that the timeout is the default " +
    "system timeout, which is specified as 15 seconds by default.";

  /**
   * JDBC Driver property name used to specify if Always Encrypted (AE) feature must be enabled. When AE is enabled,
   * the JDBC driver transparently encrypts and decrypts sensitive data stored in encrypted database columns in the
   * SQL Server.
   */
  public static final String COLUMN_ENCRYPTION = "columnEncryptionSetting";

  /**
   * Description of the {@link SqlServerConstants#COLUMN_ENCRYPTION} property.
   */
  public static final String COLUMN_ENCRYPTION_DESCRIPTION = "Whether to encrypt data sent between the client and " +
    "server for encrypted database columns in the SQL server.";

  /**
   * JDBC Driver property name used to specify if encryption must be enabled. When enabled, SQL Server uses
   * Secure Sockets Layer (SSL) encryption for all the data sent between the client and the server if the server has a
   * certificate installed. The default value is "false".
   */
  public static final String ENCRYPT = "encrypt";

  /**
   * Description of the {@link SqlServerConstants#ENCRYPT} property.
   */
  public static final String ENCRYPT_DESCRIPTION = "Whether to encrypt all data sent between the client and server. " +
    "This requires that the SQL server has a certificate installed.";

  /**
   * JDBC Driver property name used to specify if server certificate must be trusted without validation.
   */
  public static final String TRUST_SERVER_CERTIFICATE = "trustServerCertificate";

  /**
   * Description of the {@link SqlServerConstants#TRUST_SERVER_CERTIFICATE} property.
   */
  public static final String TRUST_SERVER_CERTIFICATE_DESCRIPTION = "Whether to trust the SQL server certificate " +
    "without validating it when using SSL encryption for data sent between the client and server.";

  /**
   * JDBC Driver property name used to specify workstation ID which identifies the specific workstation in various
   * SQL Server profiling and logging tools.
   */
  public static final String WORKSTATION_ID = "workstationID";

  /**
   * Description of the {@link SqlServerConstants#WORKSTATION_ID} property.
   */
  public static final String WORKSTATION_ID_DESCRIPTION = "Used to identify the specific workstation in various " +
    "SQL Server profiling and logging tools.";

  /**
   * JDBC Driver property name used to specify the name of the failover server used in a database mirroring
   * configuration. This property is used for an initial connection failure to the principal server; after you make the
   * initial connection, this property is ignored.
   */
  public static final String FAILOVER_PARTNER = "failoverPartner";

  /**
   * Description of the {@link SqlServerConstants#FAILOVER_PARTNER} property.
   */
  public static final String FAILOVER_PARTNER_DESCRIPTION = "Name or network address of the SQL Server instance that " +
    "acts as a failover partner.";

  /**
   * JDBC Driver property name used to specify the network packet size used to communicate with SQL Server, specified
   * in bytes. A value of -1 indicates using the server default packet size. A value of 0 indicates using the maximum
   * value, which is 32767. If this property is set to a value outside the acceptable range, an exception occurs.
   */
  public static final String PACKET_SIZE = "packetSize";

  /**
   * Description of the {@link SqlServerConstants#PACKET_SIZE} property.
   */
  public static final String PACKET_SIZE_DESCRIPTION = "Network packet size in bytes to use when communicating with " +
    "the SQL Server.";

  /**
   * The name of widget which is used to specify the language environment for the session. The session language
   * determines the datetime formats and system messages.
   */
  public static final String CURRENT_LANGUAGE = "currentLanguage";

  /**
   * Description of the {@link SqlServerConstants#CURRENT_LANGUAGE} property.
   */
  public static final String CURRENT_LANGUAGE_DESCRIPTION = "Language to use for SQL sessions. The language " +
    "determines datetime formats and system messages.";

  /**
   * Format of SQL Server specific JDBC connection string.
   */
  public static final String SQL_SERVER_CONNECTION_STRING_FORMAT = "jdbc:sqlserver://%s:%s;databaseName=%s";

  /**
   * The name of authentication type option which is used to indicate that connection must be established using
   * Azure AD principal name and password.
   */
  public static final String AD_PASSWORD_OPTION = "ActiveDirectoryPassword";

  /**
   * Valid value of the {@link SqlServerConstants#COLUMN_ENCRYPTION} property, which indicates that
   * Always Encrypted (AE) feature will be enabled.
   */
  public static final String COLUMN_ENCRYPTION_ENABLED = "Enabled";

  /**
   * Valid value of the {@link SqlServerConstants#COLUMN_ENCRYPTION} property, which indicates that
   * Always Encrypted (AE) feature will be disabled.
   */
  public static final String COLUMN_ENCRYPTION_DISABLED = "Disabled";

  /**
   * Query to set the language environment for the session. The session language determines the datetime formats and
   * system messages.
   */
  public static final String SET_LANGUAGE_QUERY_FORMAT = "SET LANGUAGE '%s';";

}
