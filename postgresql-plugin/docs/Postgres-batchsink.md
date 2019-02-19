# PostgreSQL Batch Sink


Description
-----------
Writes records to a PostgreSQL table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a PostgreSQL table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a Mysql table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that PostgreSQL is running on.

**Port:** Port that PostgreSQL is running on.

**Database:** PostgreSQL database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Connection Timeout** The timeout value used for socket connect operations. If connecting to the server takes longer
than this value, the connection is broken.The timeout is specified in seconds and a value of zero means that it is 
disabled.

Example
-------
Suppose you want to write output records to "users" table of PostgreSQL database named "prod" that is running on "localhost", 
port 5432, as "root" user with "root" password (Ensure that the driver for PostgreSQL is installed. You can also provide 
driver name for some specific driver, otherwise "postgresql" will be used), then configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "postgresql"
Host: "localhost"
Port: 5432
Database: "prod"
Table Name: "users"
Username: "root"
Password: "root"
```
