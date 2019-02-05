# Database Query Post-run Action


Description
-----------
Runs a database query at the end of the pipeline run.
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

**Connection String:** JDBC connection string including database name.

**Query:** The query to run.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Enable Auto-Commit:** Whether to enable auto-commit for queries run by this source. Defaults to 'false'.
Normally this setting does not matter. It only matters if you are using a jdbc driver -- like the Hive
driver -- that will error when the commit operation is run, or a driver that will error when auto-commit is
set to false. For drivers like those, you will need to set this to 'true'.

**Transaction Isolation Level:** The transaction isolation level for queries run by this sink.


Example
-------
Suppose you want to delete all records from PostrgeSQL table "userEvents" of database "prod" running on "localhost", 
port 5432, without authentication using driver "postgres" if pipeline completes successfully. 
Ensure that the driver for PostgreSQL is installed, then configure the plugin with:

```
Run Condition: "success" 
Driver Name: "postgres"
Connection String: "jdbc:postgresql://localhost:5432/prod"
Query: "delete * from userEvents"
```
