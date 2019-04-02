# Aurora DB PostgreSQL Batch Sink


Description
-----------
Writes records to an Aurora DB PostgreSQL table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to an Aurora DB PostgreSQL table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to an Aurora DB PostgreSQL table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Cluster Endpoint:** Host of the current master instance of PostgreSQL cluster.

**Port:** Port that PostgreSQL master instance is listening to.

**Database:** Aurora DB database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Connection Timeout** The timeout value used for socket connect operations. If connecting to the server takes longer
than this value, the connection is broken.The timeout is specified in seconds and a value of zero means that it is 
disabled.

Example
-------
Suppose you want to write output records to "users" table of DB2 database named "prod" that is running on 
"mycluster.xyz.eu-central-1.rds.amazonaws.com", port 5432, as "sa" user with "Test11" password. Ensure that the driver 
for PostgreSQL is installed. You can also provide driver name for some specific driver, otherwise "postgresql" will be 
used. Configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "postgresql"
Host: "mycluster.xyz.eu-central-1.rds.amazonaws.com"
Port: 5432
Database: "prod"
Table Name: "users"
Username: "sa"
Password: "Test11"
Connection timeout: 100
```
