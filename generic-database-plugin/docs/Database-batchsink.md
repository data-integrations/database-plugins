# Database Batch Sink


Description
-----------
Writes records to a database table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a database table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a database table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Connection String:** JDBC connection string including database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Transaction Isolation Level:** The transaction isolation level for queries run by this sink.

Example
-------
Suppose you want to write output records to "users" table of Mysql database named "prod" that is running on "localhost", 
port 3306, as "root" user with "root" password. Ensure that the driver for MySQL is installed, 
then configure the plugin with:

```
Reference Name: "snk1"
Driver Name: "mysql8"
Connection String: "jdbc:mysql://localhost:3306/prod"
Table Name: "users"
Username: "root"
Password: "root"
```
