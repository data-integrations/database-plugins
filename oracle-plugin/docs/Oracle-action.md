# Oracle Action


Description
-----------
Action that runs an Oracle command.


Use Case
--------
The action can be used whenever you want to run Oracle command before or after a data pipeline.
For example, you may want to run a sql update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Host:** Host that Oracle is running on.

**Port:** Port that Oracle is running on.

**Database:** Oracle database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Default Batch Value:** The default batch value that triggers an execution request.

Example
-------
Suppose you want to execute a query against Oracle database named "XE" that is running on "localhost" 
port 1251 (Ensure that the driver for Oracle is installed. You can also provide driver name for some specific driver, 
otherwise "oracle" will be used), then configure the plugin with:

```
Driver Name: "oracle"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Host: "localhost"
Port: 1251
Database: "XE"
Username: "system"
Password: "oracle"
Default Batch Value: 10
```
