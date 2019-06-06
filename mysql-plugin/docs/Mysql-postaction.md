# MySQL Query Post-run Action


Description
-----------
Runs a MySQL query at the end of the pipeline run.
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

**Host:** Host that MySQL is running on.

**Port:** Port that MySQL is running on.

**Database:** MySQL database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Auto Reconnect:** Should the driver try to re-establish stale and/or dead connections.

**Enable Auto-Commit:** Whether to enable auto-commit for queries run by this source. Defaults to 'false'.
Normally this setting does not matter. It only matters if you are using a jdbc driver -- like the Hive
driver -- that will error when the commit operation is run, or a driver that will error when auto-commit is
set to false. For drivers like those, you will need to set this to 'true'.

**Use SSL:** Turns on SSL encryption. The connection will fail if SSL is not available.

**Keystore URL:** URL to the client certificate KeyStore (if not specified, use defaults). Must be accessible at the
same location on host where CDAP Master is running and all hosts on which at least one HDFS, MapReduce, or YARN daemon
role is running.

**Keystore Password:** Password for the client certificates KeyStore.

**Truststore URL:** URL to the trusted root certificate KeyStore (if not specified, use defaults). Must be accessible at
the same location on host where CDAP Master is running and all hosts on which at least one HDFS, MapReduce, or YARN
daemon role is running.

**Truststore Password:** Password for the trusted root certificates KeyStore

**Use Compression:** Use zlib compression when communicating with the server. Select this option for WAN
connections.

**Use ANSI Quotes:** Treats " as an identifier quote character and not as a string quote character.

**SQL_MODE:** Override the default SQL_MODE session variable used by the server.


Example
-------
Suppose you want to delete all records from MySQL table "userEvents" of database "prod" running on localhost, port 3306,
without authentication using driver "mariadb" if the pipeline completes successfully (Ensure that the driver for MySQL is 
installed. You can also driver name for some specific driver, otherwise "mysql" will be used ), 
then configure the plugin with:

```
Run Condition: "success" 
Driver Name: "maridb"
Query: "delete * from userEvents"
Host: "localhost"
Port: 3306
Database: "prod"
```
