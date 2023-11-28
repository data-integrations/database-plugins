# Amazon Redshift Batch Source

Description
-----------
Reads from an Amazon Redshift database using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from an Amazon Redshift database. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**JDBC Driver name:** Name of the JDBC driver to use.

**Host:** Host URL of the current master instance of Redshift cluster.

**Port:** Port that Redshift master instance is listening to.

**Database:** Redshift database name.

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

**Schema:** The schema of records output by the source. This will be used in place of whatever schema comes
back from the query. However, it must match the schema that comes back from the query,
except it can mark fields as nullable and can contain a subset of the fields.

**Fetch Size:** The number of rows to fetch at a time per split. Larger fetch size can result in faster import,
with the tradeoff of higher memory usage.

Example
------
Suppose you want to read data from an Amazon Redshift database named "prod" that is running on
"redshift.xyz.eu-central-1.redshift.amazonaws.com", port 5439, as "sa" user with "Test11" password.
Ensure that the driver for Redshift is installed (you can also provide driver name for some specific driver,
otherwise "redshift" will be used), then configure the plugin with:then configure plugin with:

```
Reference Name: "src1"
Driver Name: "redshift"
Host: "redshift.xyz.eu-central-1.redshift.amazonaws.com"
Port: 5439
Database: "prod"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
Username: "sa"
Password: "Test11"
```  

Data Types Mapping
------------------

Mapping of Redshift types to CDAP schema:

| Redshift Data Type                                  | CDAP Schema Data Type | Comment                          |
|-----------------------------------------------------|-----------------------|----------------------------------|
| bigint                                              | long                  |                                  |
| boolean                                             | boolean               |                                  |
| character                                           | string                |                                  |
| character varying                                   | string                |                                  |
| double precision                                    | double                |                                  |
| integer                                             | int                   |                                  |
| numeric(precision, scale)/decimal(precision, scale) | decimal               |                                  |
| numeric(with 0 precision)                           | string                |                                  |
| real                                                | float                 |                                  |
| smallint                                            | int                   |                                  |
| smallserial                                         | int                   |                                  |
| text                                                | string                |                                  |
| date                                                | date                  |                                  |
| time [ (p) ] [ without time zone ]                  | time                  |                                  |
| time [ (p) ] with time zone                         | string                |                                  |
| timestamp [ (p) ] [ without time zone ]             | timestamp             |                                  |
| timestamp [ (p) ] with time zone                    | timestamp             | stored in UTC format in database |
| xml                                                 | string                |                                  |
| json                                                | string                |                                  |
| super                                               | string                |                                  |
| geometry                                            | bytes                 |                                  |
| hllsketch                                           | string                |                                  |
