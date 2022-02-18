# Oracle Query Post-run Action


Description
-----------
Runs an Oracle query at the end of the pipeline run.
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

**Host:** Host that Oracle is running on.

**Port:** Port that Oracle is running on.

**Connection Type** Whether to use an SID, Service Name, or TNS Connect Descriptor when connecting to the database.

**SID/Service Name/TNS Connect Descriptor:** Oracle connection point (Database name, Service name, or a TNS Connect Descriptor). When using TNS, place
the full TNS Connect Descriptor in the text field. For example:
(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 123.123.123.123)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)
(SERVICE_NAME = XE)))

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Default Batch Value:** The default batch value that triggers an execution request.

**Enable Auto-Commit:** Whether to enable auto-commit for queries run by this source. Defaults to 'false'.
Normally this setting does not matter. It only matters if you are using a jdbc driver -- like the Hive
driver -- that will error when the commit operation is run, or a driver that will error when auto-commit is
set to false. For drivers like those, you will need to set this to 'true'.


Example
-------
Suppose you want to delete all records from Oracle table "userEvents" of database "XE" running on localhost, port 3306,
without authentication using driver "oracle" if the pipeline completes successfully (Ensure that the driver for Oracle is 
installed. You can also driver name for some specific driver, otherwise "oracle" will be used ), 
then configure the plugin with:

```
Run Condition: "success" 
Driver Name: "oracle"
Query: "delete * from userEvents"
Host: "localhost"
Port: 1251
Database: "XE"
Username: "system"
Password: "oracle"
Default Batch Value: 10
```
