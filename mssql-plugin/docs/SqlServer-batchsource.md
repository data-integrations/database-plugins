# SQL Server Batch Source


Description
-----------
Reads from a SQL Server using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from a SQL Server. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that SQL Server is running on.

**Port:** The port where SQL Server is listening. If the port number is specified in the 
connection string, no request to SQLbrowser is made. When the port and instanceName 
are both specified, the connection is made to the specified port. However, the instanceName is validated and an 
error is thrown if it does not match the port.

**Database:** SQL Server database name.

**Import Query:** The SELECT query to use to import data from the specified table.
You can specify an arbitrary number of columns to import, or import all columns using \*. The Query should
contain the '$CONDITIONS' string. For example, 'SELECT * FROM table WHERE $CONDITIONS'.
The '$CONDITIONS' string will be replaced by 'splitBy' field limits specified by the bounding query.
The '$CONDITIONS' string is not required if numSplits is set to one. (Macro-enabled)

**Bounding Query:** Bounding Query should return the min and max of the values of the 'splitBy' field.
For example, 'SELECT MIN(id),MAX(id) FROM table'. Not required if numSplits is set to one.

**Split-By Field Name:** Field Name which will be used to generate splits. Not required if numSplits is set to one.

**Number of Splits to Generate:** Number of splits to generate.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Schema:** The schema of records output by the source. This will be used in place of whatever schema comes
back from the query. However, it must match the schema that comes back from the query,
except it can mark fields as nullable and can contain a subset of the fields.

**Instance Name** The SQL Server instance name to connect to. When it is not specified, a 
connection is made to the default instance. For the case where both the instanceName and port are specified, 
see the notes for port. If you specify a Virtual Network Name in the Server connection property, you cannot 
use instanceName connection property

**Query Timeout** The number of seconds to wait before a timeout has occurred on a query. The default value is -1, 
which means infinite timeout. Setting this to 0 also implies to wait indefinitely.

Example
------
Suppose you want to read data from SQL Server database named "prod" that is running on "localhost" port 1433,
as "sa" user with "Test11" password (Ensure that the driver for SQL Server is installed. You can also provide 
driver name for some specific driver, otherwise "sqlserver42" will be used),  then configure plugin with: 


```
Reference Name: "src1"
Driver Name: "sqlserver42"
Host: "localhost"
Port: 1433
Database: "prod"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
Username: "sa"
Password: "Test11"
```  

For example, if the 'id' column is a primary key of type int and the other columns are
non-nullable varchars, output records will have this schema:

    +======================================+
    | field name     | type                |
    +======================================+
    | id             | int                 |
    | name           | string              |
    | email          | string              |
    | phone          | string              |
    +======================================+
