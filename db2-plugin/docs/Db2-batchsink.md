# DB2 Batch Sink


Description
-----------
Writes records to a DB2 table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a DB2 table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a DB2 table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that DB2 is running on.

**Port:** Port that DB2 is listening to.

**Database:** DB2 database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

Example
-------
Suppose you want to write output records to "users" table of DB2 database named "prod" that is running on "localhost", 
port 50000, as "sa" user with "Test11" password (Ensure that the driver for DB2 is installed. You can also provide 
driver name for some specific driver, otherwise "db211" will be used.). Configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "db211"
Host: "localhost"
Port: 50000
Database: "prod"
Table Name: "users"
Username: "sa"
Password: "Test11"
```
