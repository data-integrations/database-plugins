# Aurora DB MySQL Action


Description
-----------
Action that runs an Aurora DB MySQL command.


Use Case
--------
The action can be used whenever you want to run an Aurora DB MySQL command before or after a data pipeline.
For example, you may want to run a SQL update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

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
Suppose you want to execute a query against a database named "prod" that is running on 
"mycluster.xyz.eu-central-1.rds.amazonaws.com", port 3306, without authentication. Ensure that the driver for MySQL 
is installed (you can also provide driver name for some specific driver, otherwise "mysql" will be used), 
then configure the plugin with:

```
Driver Name: "mysql"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Cluster endpoint: "mycluster.xyz.eu-central-1.rds.amazonaws.com"
Port: 3306
Database: "prod"
Auto Reconnect: "true"
```
