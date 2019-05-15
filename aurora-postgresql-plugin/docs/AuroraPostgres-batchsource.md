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

|  sql type              | schema type     | representation                                                        |
| ---------------------- | --------------- | --------------------------------------------------------------------- |
| integer                | int             |                                                                       |
| real                   | float           |                                                                       |
| boolean                | boolean         |                                                                       |
| character              | varying(30)     |                                                                       |
| smallint               | int             |                                                                       |
| bigint                 | long            |                                                                       |
| numeric(10,2)          | double          |                                                                       |
| numeric(10,2)          | double          |                                                                       |
| double precision       | double          |                                                                       |
| date                   | date            |                                                                       |
| time without time zone | time            |                                                                       |
| timestamp(3) without   |                 |                                                                       |
| time zone              | timestamp       |                                                                       |
| text                   | string          |                                                                       |
| character(100)         | string          |                                                                       |
| bytea                  | bytes           |                                                                       |
| varying(255)           | string          |                                                                       |
| interval               | string          | '3 days 04:05:06'                                                     |
| int4range              | string          | '[2,12)'                                                              |
| int8range              | string          | '[21,30)'                                                             |
| numrange               | string          | '(20,30)'                                                             |
| tsrange                | string          | '["2014-07-16 00:00:00",)'                                            |
| tstzrange              | string          | '["2014-07-16 00:00:00+00","2014-07-18 00:00:00+00")'                 |
| daterange              | string          | '[2014-07-16,2014-07-18)'                                             |
| bit varying(5)         | string          | '101'                                                                 |
| uuid                   | string          | 'ab2589a4-5394-9866-0aa3-27b6fd8483c5'                                |
| tsvector               | string          | ''a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat''                  |
| cidr                   | string          | '192.168.100.128/25'                                                  |
| inet                   | string          | '192.168.100.128/25'                                                  |
| macaddr                | string          | '08:00:2b:01:02:03'                                                   | 
| point                  | string          | '(10,10)'                                                             |
| line                   | string          | '{1,-1,0}'                                                            |
| lseg                   | string          | '[(10,10),(15,15)]'                                                   |
| box                    | string          | '(15,15),(10,10)'                                                     |
| path                   | string          | '[(10,10),(15,15)]'                                                   |
| polygon                | string          | '((10,10),(15,15))'                                                   |
| circle                 | string          | '<(1,123),10>'                                                        |
| public.enum_type       | string          |                                                                       |
| xml                    | string          | '<book><title>Manual</title><chapter>...</chapter></book>'            |
| json                   | string          | '{"firstName": "John"}'                                               |
| integer[]              | string          | '{1,2,4,5}'                                                           |
| text[]                 | string          | '{'testset','esrs'}'                                                  |
| double precision[]     | string          | '{12.23000000000000000,1245.12300000000000000,21.42100000000000000}'  |
| bigint[]               | string          | '{12,12412214214214,21512}'                                           |

