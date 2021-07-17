# SQL Server Connector


Description
-----------
This plugin can be used to browse and sample data from SQL Server database using JDBC.

Properties
----------
**Plugin Name:** Name of the SQL Server JDBC plugin to use. This is the value of the 'name' key
defined in the JSON file for the JDBC plugin.

**Host:** Host name or IP address of the database server to connect to.

**Port:** Port number of the database server to connect to. If not specified will default to 1433.

**Username:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**Password:** Password to use to connect to the specified database.

**Authentication Type** Indicates which authentication method will be used for the connection. Use 'SQL Login'. to
connect to a SQL Server using username and password properties. Use 'Active Directory Password' to connect to an Azure
SQL Database/Data Warehouse using an Azure AD principal name and password.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.
This is a semicolon-separated list of key-value pairs, where each pair is separated by a equals '=' and specifies
the key and value for the argument. For example, 'key1=value1;key2=value' specifies that the connection will be
given arguments 'key1' mapped to 'value1' and the argument 'key2' mapped to 'value2'.