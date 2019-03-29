# Aurora DB PostgreSQL Action


Description
-----------
Action that runs an Aurora DB PostgreSQL command.


Use Case
--------
The action can be used whenever you want to run an Aurora DB PostgreSQL command before or after a data pipeline.
For example, you may want to run a SQL update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Cluster Endpoint:** Host of the current master instance of PostgreSQL cluster.

**Port:** Port that PostgreSQL master instance is listening to.

**Database:** Aurora DB database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Connection Timeout** The timeout value used for socket connect operations. If connecting to the server takes longer
than this value, the connection is broken. The timeout is specified in seconds and a value of zero means that it is 
disabled.

Example
-------
Suppose you want to execute a query against a database named "prod" that is running on 
"mycluster.xyz.eu-central-1.rds.amazonaws.com", port 5432, without authentication. Ensure that the driver for PostgreSQL 
is installed (you can also provide driver name for some specific driver, otherwise "postgresql" will be used), 
then configure the plugin with:

```
Driver Name: "postgresql"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Cluster endpoint: "mycluster.xyz.eu-central-1.rds.amazonaws.com"
Port: 5432
Database: "prod"
Connection timeout: 100
```
