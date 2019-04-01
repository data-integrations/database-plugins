# Aurora DB PostgreSQL Batch Source


Description
-----------
Reads from an Aurora DB PostgreSQL database using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from an Aurora DB PostgreSQL database. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Cluster Endpoint:** Host of the current master instance of PostgreSQL cluster.

**Port:** Port that PostgreSQL master instance is listening to.

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

**Connection Timeout** The timeout value used for socket connect operations. If connecting to the server takes longer
than this value, the connection is broken.The timeout is specified in seconds and a value of zero means that it is 
disabled.

**Schema:** The schema of records output by the source. This will be used in place of whatever schema comes
back from the query. However, it must match the schema that comes back from the query,
except it can mark fields as nullable and can contain a subset of the fields.

Example
------
Suppose you want to read data from an Aurora DB PostgreSQL database named "prod" that is running on 
"mycluster.xyz.eu-central-1.rds.amazonaws.com", port 5432, as "sa" user with "Test11" password. 
Ensure that the driver for PostgreSQL is installed (you can also provide driver name for some specific driver, 
otherwise "postgresql" will be used), then configure the plugin with:then configure plugin with: 


```
Reference Name: "src1"
Driver Name: "postgresql"
Host: "mycluster.xyz.eu-central-1.rds.amazonaws.com"
Port: 5432
Database: "prod"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
Username: "sa"
Password: "Test11"
Connection timeout: 100
```  

Mapping of PostgreSQL types to CDAP schema:

    +==========================================+
    |  sql type              | type            |
    +==========================================+
    | integer                | int             |
    | real                   | float           |
    | boolean                | boolean         |
    | character              | varying(30)     |
    | smallint               | int             |
    | bigint                 | long            |
    | numeric(10,2)          | double          |
    | numeric(10,2)          | double          |
    | double precision       | double          |
    | date                   | date            |
    | time without time zone | time            |
    | timestamp(3) without   |                 |
    | time zone              | timestamp       |
    | text                   | string          |
    | character(100)         | string          |
    | bytea                  | bytes           |    
    | varying(255)           | string          |
    | interval               | string          |
    | int4range              | string          |
    | int8range              | string          |
    | numrange               | string          |
    | tsrange                | string          |
    | tstzrange              | string          |
    | daterange              | string          |
    | bit varying(5)         | string          |
    | uuid                   | string          |
    | tsvector               | string          |
    | cidr                   | string          |
    | inet                   | string          |
    | macaddr                | string          |
    | point                  | string          |
    | line                   | string          |
    | lseg                   | string          |
    | box                    | string          |
    | path                   | string          |
    | polygon                | string          |
    | circle                 | string          |
    | public.type_name       | string          |
    | xml                    | string          |
    | json                   | string          |
    | integer[]              | string          |
    | text[]                 | string          |
    | text[]                 | string          |
    | double precision[]     | string          |
    | double precision[]     | string          |
    | double precision[]     | string          |
    | bigint[]               | string          |
    +==========================================+