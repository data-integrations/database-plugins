# Jethro Action


Description
-----------
Runs a Jethro query at the end of the pipeline run.
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

**Host:** Host that Jethro is running on.

**Port:** Port that Jethro is running on.

**Database:** MemSQL database name.

**Query:** Query to run.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

Example
-------
Suppose you want to execute a query against a Jethro database instance named "prod" that is running on "localhost" 
port 9112, then configure the plugin with:

```
Run Condition: "success" 
Driver Name: "jethro"
Host: "localhost"
Port: 9112
Database: "prod"
Query: "TRUNCATE TABLE testTable"
```