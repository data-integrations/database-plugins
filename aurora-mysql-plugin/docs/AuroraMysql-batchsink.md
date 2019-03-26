# Aurora DB MySQL Batch Sink


Description
-----------
Writes records to an Aurora DB MySQL table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to an Aurora DB MySQL table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to an Aurora DB MySQL table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Cluster Endpoint:** Host of the current master instance of MySQL cluster.

**Port:** Port that MySQL master instance is listening to.

**Database:** Aurora DB database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Auto Reconnect:** Should the driver try to re-establish stale and/or dead connections.

Example
-------
Suppose you want to write output records to "users" table of DB2 database named "prod" that is running on 
"mycluster.xyz.eu-central-1.rds.amazonaws.com", port 3306, as "sa" user with "Test11" password. Ensure that the driver 
for MySQL is installed. You can also provide driver name for some specific driver, otherwise "mysql" will be used. 
Configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "mysql"
Host: "mycluster.xyz.eu-central-1.rds.amazonaws.com"
Port: 3306
Database: "prod"
Table Name: "users"
Username: "sa"
Password: "Test11"
Auto Recconnect: "false"
```
