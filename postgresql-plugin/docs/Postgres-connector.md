# PostgreSQL Connector


Description
-----------
This plugin can be used to browse and sample data from PostgreSQL database using JDBC.
Since database must be specified when connecting to the database. Users can only browse from database schema.

Properties
----------
**Plugin Name:** Name of the PostgreSQL JDBC plugin to use. This is the value of the 'name' key
defined in the JSON file for the JDBC plugin.

**Host:** Host name or IP address of the database server to connect to.

**Port:** Port number of the database server to connect to. If not specified will default to 5432.

**Username:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**Password:** Password to use to connect to the specified database.

**Database** The name of the database to connect to.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.
This is a semicolon-separated list of key-value pairs, where each pair is separated by a equals '=' and specifies
the key and value for the argument. For example, 'key1=value1;key2=value' specifies that the connection will be
given arguments 'key1' mapped to 'value1' and the argument 'key2' mapped to 'value2'.