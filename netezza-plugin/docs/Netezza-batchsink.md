# Netezza Batch Sink


Description
-----------
Writes records to a Netezza table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a Netezza table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a Netezza table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Netezza is running on.

**Port:** Port that Netezza is running on.

**Database:** Netezza database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

Example
-------
Suppose you want to write output records to "users" table of Netezza database named "prod" that is running on "localhost", 
port 5480, as "test" user with "testpwsd" password (Ensure that the driver for Netezza is installed. You can also provide 
driver name for some specific driver, otherwise "netezza" will be used), then configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "netezza"
Host: "localhost"
Port: 5480
Database: "prod"
Table Name: "users"
Username: "test"
Password: "testpwsd"
```
