# Amazon Redshift Connection

Description
-----------
Use this connection to access data in an Amazon Redshift database using JDBC.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**JDBC Driver name:** Name of the JDBC driver to use.

**Host:** Host of the current master instance of Redshift cluster.

**Port:** Port that Redshift master instance is listening to.

**Database:** Redshift database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.
