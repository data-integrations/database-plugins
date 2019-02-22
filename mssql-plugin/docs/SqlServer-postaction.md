# SQL Server Query Post-run Action


Description
-----------
Runs a SQL Server query at the end of the pipeline run.
Can be configured to run only on success, only on failure, or always at the end of the run.


Use Case
--------
The action is used whenever you need to run a query at the end of a pipeline run.
For example, you may have a pipeline that imports data from a database table to
hdfs files. At the end of the run, you may want to run a query that deletes the data
that was read from the table.


Properties
----------
**Run Condition:** When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'success'.
If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or failed.
If set to 'success', the action will only be executed if the pipeline run succeeded.
If set to 'failure', the action will only be executed if the pipeline run failed.

**Driver Name:** Name of the JDBC driver to use.

**Query:** Query to run.

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

**Enable Auto-Commit:** Whether to enable auto-commit for queries run by this source. Defaults to 'false'.
Normally this setting does not matter. It only matters if you are using a jdbc driver -- like the Hive
driver -- that will error when the commit operation is run, or a driver that will error when auto-commit is
set to false. For drivers like those, you will need to set this to 'true'.

**Instance Name** The SQL Server instance name to connect to. When it is not specified, a 
connection is made to the default instance. For the case where both the instanceName and port are specified, 
see the notes for port. If you specify a Virtual Network Name in the Server connection property, you cannot 
use instanceName connection property

**Query Timeout** The number of seconds to wait before a timeout has occurred on a query. The default value is -1, 
which means infinite timeout. Setting this to 0 also implies to wait indefinitely.

Example
-------
Suppose you want to delete all records from SQL Server table "userEvents" of database "prod" running on localhost, port 1433,
without authentication using driver "sqlserver42" if pipeline completes successfully (Ensure that the driver for SQL Server is 
installed. You can also driver name for some specific driver, otherwise "sqlserver42" will be used ), 
then configure the plugin with:

```
Run Condition: "success" 
Driver Name: "sqlserver42"
Query: "delete * from userEvents"
Host: "localhost"
Port: 1433
Database: "prod"
```
