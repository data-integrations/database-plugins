# Teradata Batch Sink


Description
-----------
Writes records to a Teradata table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a Teradata table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a Teradata table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Teradata is running on.

**Port:** Port that Teradata is running on.

**Database:** Teradata database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

Example
-------
Suppose you want to write output records to "users" table of Teradata database named "prod" that is running on "localhost", 
port 1025, as "root" user with "root" password (Ensure that the driver for Teradata is installed. You can also provide 
driver name for some specific driver, otherwise "teradata" will be used), then configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "teradata"
Host: "localhost"
Port: 1025
Database: "prod"
Table Name: "users"
Username: "dbc"
Password: "dbc"
```
Data Types Mapping
------
Teradata specific data types mapped to string and can have multiple input formats and one 'canonical' output form.
Please, refer to Teradata data types documentation to figure out proper formats.

| Teradata Data Type                                  | CDAP Schema Data Type | Comment                                      |
|-----------------------------------------------------|-----------------------|----------------------------------------------|
| BYTEINT                                             | INT                   |                                              |
| SMALLINT                                            | INT                   |                                              |
| INTEGER                                             | INT                   |                                              |
| BIGINT                                              | LONG                  |                                              |
| DECIMAL/NUMERIC                                     | DECIMAL               |                                              |
| FLOAT/REAL/DOUBLE PRECISION                         | DOUBLE                |                                              |
| NUMBER                                              | DECIMAL               |                                              |
| BYTE                                                | BYTES                 |                                              |
| VARBYTE                                             | BYTES                 |                                              |
| BLOB                                                | BYTES                 |                                              |
| CHAR                                                | STRING                |                                              |
| VARCHAR                                             | STRING                |                                              |
| CLOB                                                | STRING                |                                              |
| DATE                                                | DATE                  |                                              |
| TIME                                                | TIME_MICROS           |                                              |
| TIMESTAMP                                           | TIMESTAMP_MICROS      |                                              |
| TIME WITH TIME ZONE                                 | TIME_MICROS           |                                              |
| TIMESTAMP WITH TIME ZONE                            | TIMESTAMP_MICROS      |                                              |
| INTERVAL YEAR                                       | STRING                |                                              |
| INTERVAL YEAR TO MONTH                              | STRING                |                                              |
| INTERVAL MONTH                                      | STRING                |                                              |
| INTERVAL DAY                                        | STRING                |                                              |
| INTERVAL DAY TO HOUR                                | STRING                |                                              |
| INTERVAL DAY TO MINUTE                              | STRING                |                                              |
| INTERVAL DAY TO SECOND                              | STRING                |                                              |
| INTERVAL HOUR                                       | STRING                |                                              |
| INTERVAL HOUR TO MINUTE                             | STRING                |                                              |
| INTERVAL HOUR TO SECOND                             | STRING                |                                              |
| INTERVAL MINUTE                                     | STRING                |                                              |
| INTERVAL MINUTE TO SECOND                           | STRING                |                                              |
| INTERVAL SECOND                                     | STRING                |                                              |
| ST_Geometry                                         | STRING                |                                              |