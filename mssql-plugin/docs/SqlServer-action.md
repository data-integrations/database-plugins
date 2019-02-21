# SQL Server Action


Description
-----------
Action that runs a SQL Server command.


Use Case
--------
The action can be used whenever you want to run a SQL Server command before or after a data pipeline.
For example, you may want to run a sql update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Host:** Host that SQL Server is running on.

**Port:** The port where SQL Server is listening. If the port number is specified in the 
connection string, no request to SQLbrowser is made. When the port and instanceName 
are both specified, the connection is made to the specified port. However, the instanceName is validated and an 
error is thrown if it does not match the port.

**Database:** SQL Server database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Instance Name** The SQL Server instance name to connect to. When it is not specified, a 
connection is made to the default instance. For the case where both the instanceName and port are specified, 
see the notes for port. If you specify a Virtual Network Name in the Server connection property, you cannot 
use instanceName connection property

**Query Timeout** The number of seconds to wait before a timeout has occurred on a query. The default value is -1, 
which means infinite timeout. Setting this to 0 also implies to wait indefinitely.

Example
-------
Suppose you want to execute a query against a SQL Server database named "prod" that is running on "localhost" 
port 1433 (Ensure that the driver for SQL Server is installed. You can also provide driver name for some specific driver, 
otherwise "sqlserver42" will be used), then configure the plugin with:

```
Driver Name: "sqlserver42"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Host: "localhost"
Port: 1433
Database: "prod"
```
