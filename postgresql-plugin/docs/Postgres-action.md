# PostgreSQL Action


Description
-----------
Action that runs a PostgreSQL command.


Use Case
--------
The action can be used whenever you want to run a PostgreSQL command before or after a data pipeline.
For example, you may want to run a sql update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Host:** Host that PostgreSQL is running on.

**Port:** Port that PostgreSQL is running on.

**Database:** MySQL database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Connection Timeout** The timeout value used for socket connect operations. If connecting to the server takes longer
than this value, the connection is broken.The timeout is specified in seconds and a value of zero means that it is 
disabled.

Example
-------
Suppose you want to execute a query against a PostgreSQL database named "prod" that is running on "localhost" 
port 5432 (Ensure that the driver for PostgreSQL is installed. You can also provide driver name for some specific driver, 
otherwise "postgresql" will be used), then configure the plugin with:

```
Driver Name: "postgresql"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Host: "localhost"
Port: 5432
Database: "prod"
```
