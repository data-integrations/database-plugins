# Aurora DB MySQL Batch Source


Description
-----------
Reads from an Aurora DB MySQL database using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from an Aurora DB MySQL database. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Cluster Endpoint:** Host of the current master instance of MySQL cluster.

**Port:** Port that MySQL master instance is listening to.

**Database:** Aurora DB database name.

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

**Auto Reconnect:** Should the driver try to re-establish stale and/or dead connections.

**Schema:** The schema of records output by the source. This will be used in place of whatever schema comes
back from the query. However, it must match the schema that comes back from the query,
except it can mark fields as nullable and can contain a subset of the fields.

**Fetch Size:** The number of rows to fetch at a time per split. Larger fetch size can result in faster import, 
with the tradeoff of higher memory usage.

Example
------
Suppose you want to read data from an Aurora DB MySQL database named "prod" that is running on 
"mycluster.xyz.eu-central-1.rds.amazonaws.com", port 3306, as "sa" user with "Test11" password. 
Ensure that the driver for MySQL is installed (you can also provide driver name for some specific driver, 
otherwise "mysql" will be used), then configure the plugin with:then configure plugin with: 


```
Reference Name: "src1"
Driver Name: "mysql"
Host: "mycluster.xyz.eu-central-1.rds.amazonaws.com"
Port: 3306
Database: "prod"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
Username: "sa"
Password: "Test11"
Auto Reconnect: "true"
```  

Mapping of MySQL types to CDAP schema:

| sql type           | schema type  |
| ------------------ | ------------ |
| INT                | int          |
| TINYINT            | boolean      |
| BOOL,BOOLEAN       | boolean      |
| INTEGER            | int          |
| VARCHAR(40)        | string       |
| DOUBLE             | double       |
| BOOLEAN            | boolean      |
| VARCHAR(30)        | string       |
| TINYINT            | int          |
| SMALLINT           | int          |
| MEDIUMINT          | int          |
| BIGINT             | long         |
| FLOAT              | float        |
| REAL               | double       |
| NUMERIC(10,2)      | double       |
| DECIMAL(10,2)      | double       |
| BIT                | boolean      | 
| DATE               | date         |
| TIME               | time         |
| YEAR               | date         |
| TIMESTAMP(3)       | timestamp    |
| TEXT               | string       |
| TINYTEXT           | string       |
| MEDIUMTEXT         | string       |
| LONGTEXT           | string       |
| CHAR(100)          | string       |
| BINARY(100)        | bytes        |
| VARBINARY(20)      | bytes        |
| TINYBLOB           | bytes        |
| BLOB(100)          | bytes        |
| MEDIUMBLOB         | bytes        |
| LONGBLOB           | bytes        |
| GEOMETRY           | bytes        |
| POINT              | bytes        |
| LINESTRING         | bytes        |
| POLYGON            | bytes        |
| MULTIPOINT         | bytes        |
| MULTILINESTRING    | bytes        |
| MULTIPOLYGON       | bytes        |
| GEOMETRYCOLLECTION | bytes        |
| JSON               | string       |
