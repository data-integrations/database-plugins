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

**Port:** Port that SQL Server is listening to. If the port number is specified in the
connection string, no request to SQLbrowser is made. When the port and instanceName 
are both specified, the connection is made to the specified port. However, the instanceName is validated and an 
error is thrown if it does not match the port.

**Database:** SQL Server database name.

**Import Query:** The SELECT query to use to import data from the specified table.
You can specify an arbitrary number of columns to import, or import all columns using \*. The Query should
contain the '$CONDITIONS' string. For example, 'SELECT * FROM table WHERE $CONDITIONS'.
The '$CONDITIONS' string will be replaced by 'splitBy' field limits specified by the bounding query.
The '$CONDITIONS' string is not required if numSplits is set to one.

**Bounding Query:** Bounding Query should return the min and max of the values of the 'splitBy' field.
For example, 'SELECT MIN(id),MAX(id) FROM table'. Not required if numSplits is set to one.

**Split-By Field Name:** Field Name which will be used to generate splits. Not required if numSplits is set to one.

**Number of Splits to Generate:** Number of splits to generate.

**Authentication Type:** Indicates which SQL authentication method will be used for the connection. Use 'SQL Login' to
connect to a SQL Server using username and password properties. Use 'Active Directory Password' to connect to
an Azure SQL Database/Data Warehouse using an Azure AD principal name and password.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Instance Name:** SQL Server instance name to connect to. When it is not specified, a
connection is made to the default instance. For the case where both the instanceName and port are specified,
see the notes for port. If you specify a Virtual Network Name in the Server connection property, you cannot
use instanceName connection property.

**Query Timeout:** Number of seconds to wait before a timeout has occurred on a query. The default value is -1,
which means infinite timeout. Setting this to 0 also implies to wait indefinitely.

**Connect Timeout:** Time in seconds to wait for a connection to the server before terminating the attempt and
generating an error.

**Workstation ID:** Used to identify the specific workstation in various SQL Server profiling and logging tools.

**Trust Server Certificate:** Whether to trust the SQL server certificate without validating it when using SSL
encryption for data sent between the client and server.

**Column Encryption:** Whether to encrypt data sent between the client and server for encrypted database columns in the
SQL server.

**Encrypt:** Whether to encrypt all data sent between the client and server. This requires that the SQL server has a
certificate installed.

**Failover Partner:** Name or network address of the SQL Server instance that acts as a failover partner.

**Packet Size:** Network packet size in bytes to use when communicating with the SQL Server.

**Current Language:** Language to use for SQL sessions. The language determines datetime formats and system messages.
See [sys.syslanguages] for the list of installed languages.

[sys.syslanguages]:
https://docs.microsoft.com/en-us/sql/relational-databases/system-compatibility-views/sys-syslanguages-transact-sql

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Schema:** The schema of records output by the source. This will be used in place of whatever schema comes
back from the query. However, it must match the schema that comes back from the query,
except it can mark fields as nullable and can contain a subset of the fields.


Data Types Mapping
----------

    | MS SQL Data Type | CDAP Schema Data Type  | Comment                                                        |
    | ---------------- | ---------------------- | -------------------------------------------------------------- |
    | BIGINT           | long                   |                                                                |
    | BINARY           | bytes                  |                                                                |
    | BIT              | boolean                |                                                                |
    | CHAR             | string                 |                                                                |
    | DATE             | date                   |                                                                |
    | DATETIME         | datetime               | Users can manually set output schema to map it to timestamp.   |
    | DATETIME2        | datetime               | Users can manually set output schema to map it to timestamp.   |
    | DATETIMEOFFSET   | datetime               | Users can manually set output schema to map it to string.      |
    | DECIMAL          | decimal                |                                                                |
    | FLOAT            | double                 |                                                                |
    | IMAGE            | bytes                  |                                                                |
    | INT              | int                    |                                                                |
    | MONEY            | decimal                |                                                                |
    | NCHAR            | string                 |                                                                |
    | NTEXT            | string                 |                                                                |
    | NUMERIC          | decimal                |                                                                |
    | NVARCHAR         | string                 |                                                                |
    | NVARCHAR(MAX)    | string                 |                                                                |
    | REAL             | float                  |                                                                |
    | SMALLDATETIME    | timestamp              |                                                                |
    | SMALLINT         | int                    |                                                                |
    | SMALLMONEY       | decimal                |                                                                |
    | TEXT             | string                 |                                                                |
    | TIME             | time                   | TIME data type has the accuracy of 100 nanoseconds which is    |
    |                  |                        | not currently supported. Values of this type will be rounded   |
    |                  |                        | to microsecond.                                                |
    | TINYINT          | int                    |                                                                |
    | UDT              | bytes                  | UDT types are mapped according to the type they are an alias   |
    |                  |                        | of. For example, is there is an 'SSN' type that was created as |
    |                  |                        | 'CREATE TYPE SSN FROM varchar(11);', that type would get       |
    |                  |                        | mapped to a CDAP string. Common Language Runtime UDTs are      |
    |                  |                        | mapped to CDAP bytes.                                          |
    | UNIQUEIDENTIFIER | string                 |                                                                |
    | VARBINARY        | bytes                  |                                                                |
    | VARBINARY(MAX)   | bytes                  |                                                                |
    | VARCHAR          | string                 |                                                                |
    | VARCHAR(MAX)     | string                 |                                                                |
    | XML              | string                 |                                                                |
    | SQLVARIANT       | string                 |                                                                |
    | GEOMETRY         | bytes                  |                                                                |
    | GEOGRAPHY        | bytes                  |                                                                |


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

    | field name     | type                |
    | -------------- | ------------------- |
    | id             | int                 |
    | name           | string              |
    | email          | string              |
    | phone          | string              |
