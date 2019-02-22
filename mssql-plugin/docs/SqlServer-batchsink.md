# SQL Server Batch Sink


Description
-----------
Writes records to a SQL Server table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a SQL Server table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a SQL Server table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that SQL Server is running on.

**Port:** The port where SQL Server is listening. If the port number is specified in the 
connection string, no request to SQLbrowser is made. When the port and instanceName 
are both specified, the connection is made to the specified port. However, the instanceName is validated and an 
error is thrown if it does not match the port.

**Database:** SQL Server database name.

**Table Name:** Name of the table to export to.

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
Suppose you want to write output records to "users" table of SQL Server database named "prod" that is running on "localhost", 
port 1433, as "sa" user with "Test11" password (Ensure that the driver for SQL Server is installed. You can also provide 
driver name for some specific driver, otherwise "sqlserver42" will be used), then configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "sqlserver42"
Host: "localhost"
Port: 1433
Database: "prod"
Table Name: "users"
Username: "sa"
Password: "Test11"
```
