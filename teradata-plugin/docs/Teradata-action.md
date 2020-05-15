# Teradata Action


Description
-----------
Action that runs a Teradata command.


Use Case
--------
The action can be used whenever you want to run a Teradata command before or after a data pipeline.
For example, you may want to run a sql update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Host:** Host that Teradata is running on.

**Port:** Port that Teradata is running on.

**Database:** Teradata database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

Example
-------
Suppose you want to execute a query against a Teradata database named "prod" that is running on "localhost" 
port 1025 (Ensure that the driver for Teradata is installed. You can also provide driver name for some specific driver, 
otherwise "teradata" will be used), then configure the plugin with:

```
Driver Name: "teradata"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Host: "localhost"
Port: 1025
Database: "prod"
Username: "dbc"
Password: "dbc"
```
