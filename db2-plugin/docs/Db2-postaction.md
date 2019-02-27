# DB2 Query Post-run Action


Description
-----------
Runs a DB2 query at the end of the pipeline run.
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

**Host:** Host that DB2 is running on.

**Port:** Port that DB2 is listening to.

**Database:** DB2 database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

Example
-------
Suppose you want to delete all records from DB2 table "userEvents" of database "prod" running on "localhost", port 50000,
without authentication using driver "db211" if pipeline completes successfully (Ensure that the driver for DB2 is 
installed. You can also driver name for some specific driver, otherwise "db211" will be used ), 
then configure the plugin with:

```
Run Condition: "success" 
Driver Name: "db211"
Query: "delete * from userEvents"
Host: "localhost"
Port: 50000
Database: "prod"
```
