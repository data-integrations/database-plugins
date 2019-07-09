# PostgreSQL Batch Source


Description
-----------
Reads from a PostgreSQL using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from a PostgreSQL. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that PostgreSQL is running on.

**Port:** Port that PostgreSQL is running on.

**Database:** PostgreSQL database name.

**Import Query:** The SELECT query to use to import data from the specified table.
You can specify an arbitrary number of columns to import, or import all columns using \*. The Query should
contain the '$CONDITIONS' string. For example, 'SELECT * FROM table WHERE $CONDITIONS'.
The '$CONDITIONS' string will be replaced by 'splitBy' field limits specified by the bounding query.
The '$CONDITIONS' string is not required if numSplits is set to one.

**Bounding Query:** Bounding Query should return the min and max of the values of the 'splitBy' field.
For example, 'SELECT MIN(id),MAX(id) FROM table'. Not required if numSplits is set to one.

**Split-By Field Name:** Field Name which will be used to generate splits. Not required if numSplits is set to one.

**Number of Splits to Generate:** Number of splits to generate.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Connection Timeout** The timeout value used for socket connect operations. If connecting to the server takes longer
than this value, the connection is broken.The timeout is specified in seconds and a value of zero means that it is 
disabled.

**Schema:** The schema of records output by the source. This will be used in place of whatever schema comes
back from the query. However, it must match the schema that comes back from the query,
except it can mark fields as nullable and can contain a subset of the fields.


Example
------
Suppose you want to read data from PostgreSQL database named "prod" that is running on "localhost" port 5432,
as "postgres" user with "postgres" password (Ensure that the driver for PostgreSQL is installed. You can also provide 
driver name for some specific driver, otherwise "postgresql" will be used),  then configure plugin with: 


```
Reference Name: "src1"
Driver Name: "postgresql"
Host: "localhost"
Port: 5432
Database: "prod"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
Username: "postgres"
Password: "postgres"
```  

For example, if the 'id' column is a primary key of type int and the other columns are
non-nullable varchars, output records will have this schema:

    | field name     | type                |
    | -------------- | ------------------- |
    | id             | int                 |
    | name           | string              |
    | email          | string              |
    | phone          | string              |

Data Types Mapping
------
All PostgreSQL specific data types mapped to string and can have multiple input formats and one 'canonical' output form.
Please, refer to PostgreSQL data types documentation to figure out proper formats.

| PostgreSQL Data Type                                | CDAP Schema Data Type | Comment                                      |
|-----------------------------------------------------|-----------------------|----------------------------------------------|
| bigint                                              | long                  |                                              |
| bigserial                                           | long                  |                                              |
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
| smallserial                                         | int                   |                                              |
| serial                                              | int                   |                                              |
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