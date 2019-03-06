# Netezza Action


Description
-----------
Action that runs a Netezza command.


Use Case
--------
The action can be used whenever you want to run a Netezza command before or after a data pipeline.
For example, you may want to run a sql update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Host:** Host that Netezza is running on.

**Port:** Port that Netezza is running on.

**Database:** Netezza database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

Example
-------
Suppose you want to execute a query against a Netezza database named "prod" that is running on "localhost" 
port 5480 (Ensure that the driver for Netezza is installed. You can also provide driver name for some specific driver, 
otherwise "netezza" will be used), then configure the plugin with:

```
Driver Name: "netezza"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Host: "localhost"
Port: 5480
Database: "prod"
```
