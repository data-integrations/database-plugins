# Aurora DB MySQL Query Post-run Action


Description
-----------
Runs an Aurora DB MySQL query at the end of the pipeline run.
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

**Cluster Endpoint:** Host of the current master instance of MySQL cluster.

**Port:** Port that MySQL master instance is listening to.

**Database:** Aurora DB database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Auto Reconnect:** Should the driver try to re-establish stale and/or dead connections.

Example
-------
Suppose you want to delete all records from an Aurora DB MySQL table "userEvents" of database "prod" running on 
"mycluster.xyz.eu-central-1.rds.amazonaws.com", port 3306, without authentication using driver "mysql" if the
pipeline completes successfully. Ensure that the driver for MySQL is installed (you can also provide driver name for 
some specific driver, otherwise "mysql" will be used ), then configure the plugin with:

```
Run Condition: "success" 
Driver Name: "mysql"
Query: "delete * from userEvents"
Host: "mycluster.xyz.eu-central-1.rds.amazonaws.com"
Port: 3306
Database: "prod"
Auto Reconnect: "false"
```
