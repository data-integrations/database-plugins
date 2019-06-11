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

**Port:** Port that SQL Server is listening to. If the port number is specified in the
connection string, no request to SQLbrowser is made. When the port and instanceName 
are both specified, the connection is made to the specified port. However, the instanceName is validated and an 
error is thrown if it does not match the port.

**Database:** SQL Server database name.

**Authentication Type:** Indicates which SQL authentication method will be used for the connection. Use 'SQL Login' to
connect to a SQL Server using username and password properties. Use 'Active Directory Password' to connect to
an Azure SQL Database/Data Warehouse using an Azure AD principal name and password.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Instance Name:** SQL Server instance name to connect to. When it is not specified, a
connection is made to the default instance. For the case where both the instanceName and port are specified,
see the notes for port. If you specify a Virtual Network Name in the Server connection property, you cannot
use instanceName connection property.

**Query Timeout:** Number of seconds to wait before a timeout has occurred on a query. The default value is -1,
which means infinite timeout. Setting this to 0 also implies to wait indefinitely.

**Application Intent:** Declares the application workload type when connecting to a server.

**Connect Timeout:** Time in seconds to wait for a connection to the server before terminating the attempt and
generating an error.

**Workstation ID:** Used to identify the specific workstation in various SQL Server profiling and logging tools.

**Trust Server Certificate:** Whether to trust the SQL server certificate without validating it when using SSL
encryption for data sent between the client and server.

**Column Encryption:** Whether to encrypt data sent between the client and server for encrypted database columns in the
SQL server.

**Encrypt:** Whether to encrypt all data sent between the client and server. This requires that the SQL server has a
certificate installed.

**Failover Partner:** Name or network address of the SQL Server instance that acts as a failover partner.

**Packet Size:** Network packet size in bytes to use when communicating with the SQL Server.

**Current Language:** Language to use for SQL sessions. The language determines datetime formats and system messages.
See [sys.syslanguages] for the list of installed languages.

[sys.syslanguages]:
https://docs.microsoft.com/en-us/sql/relational-databases/system-compatibility-views/sys-syslanguages-transact-sql

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Enable Auto-Commit:** Whether to enable auto-commit for queries run by this source. Defaults to 'false'.
Normally this setting does not matter. It only matters if you are using a jdbc driver -- like the Hive
driver -- that will error when the commit operation is run, or a driver that will error when auto-commit is
set to false. For drivers like those, you will need to set this to 'true'.


Example
-------
Suppose you want to delete all records from SQL Server table "userEvents" of database "prod" running on localhost,
port 1433, without authentication using driver "sqlserver42" if the pipeline completes successfully (Ensure that the
driver for SQL Server is installed. You can also driver name for some specific driver, otherwise "sqlserver42" will be
used ), then configure the plugin with:

```
Run Condition: "success" 
Driver Name: "sqlserver42"
Query: "delete * from userEvents"
Host: "localhost"
Port: 1433
Database: "prod"
```
