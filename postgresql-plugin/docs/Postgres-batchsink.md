# PostgreSQL Batch Sink


Description
-----------
Writes records to a PostgreSQL table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a PostgreSQL table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a PostgreSQL table where it can be served to your users.

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

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

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
Data Types Mapping
------
All PostgreSQL specific data types mapped to string and can have multiple input formats and one 'canonical' output form.
Please, refer to PostgreSQL data types documentation to figure out proper formats.

| PostgreSQL Data Type                                | CDAP Schema Data Type | Comment                                      |
|-----------------------------------------------------|-----------------------|----------------------------------------------|
| bigint                                              | long                  |                                              |
| bit(n)                                              | string                | string with '0' and '1' chars exact n length |
| bit varying(n)                                      | string                | string with '0' and '1' chars max n length   |
| boolean                                             | boolean               |                                              |
| bytea                                               | bytes                 |                                              |
| character                                           | string                |                                              |
| character varying                                   | string                |                                              |
| double precision                                    | double                |                                              |
| integer                                             | int                   |                                              |
| numeric(precision, scale)/decimal(precision, scale) | decimal               |                                              |
| real                                                | float                 |                                              |
| smallint                                            | int                   |                                              |
| text                                                | string                |                                              |
| date                                                | date                  |                                              |
| time [ (p) ] [ without time zone ]                  | time                  |                                              |
| time [ (p) ] with time zone                         | string                |                                              |
| timestamp [ (p) ] [ without time zone ]             | timestamp             |                                              |
| timestamp [ (p) ] with time zone                    | timestamp             | stored in UTC format in database             |
| xml                                                 | string                |                                              |
| tsquery                                             | string                |                                              |
| tsvector                                            | string                |                                              |
| uuid                                                | string                |                                              |
| box                                                 | string                |                                              |
| cidr                                                | string                |                                              |
| circle                                              | string                |                                              |
| inet                                                | string                |                                              |
| interval                                            | string                |                                              |
| json                                                | string                |                                              |
| jsonb                                               | string                |                                              |
| line                                                | string                |                                              |
| lseg                                                | string                |                                              |
| macaddr                                             | string                |                                              |
| macaddr8                                            | string                |                                              |
| money                                               | string                |                                              |
| path                                                | string                |                                              |
| point                                               | string                |                                              |
| polygon                                             | string                |                                              |