# Oracle Batch Sink


Description
-----------
Writes records to Oracle table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a Oracle table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a Oracle table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Oracle is running on.

**Port:** Port that Oracle is running on.

**Database:** Oracle database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Default Batch Value:** The default batch value that triggers an execution request.

Example
-------
Suppose you want to write output records to "users" table of Oracle database named "XE" that is running on "localhost", 
port 1251, as "system" user with "oracle" password (Ensure that the driver for Oracle is installed. You can also provide 
driver name for some specific driver, otherwise "oracle" will be used), then configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "oracle"
Host: "localhost"
Port: 1251
Database: "XE"
Table Name: "users"
Username: "system"
Password: "oracle"
```
