# Oracle Batch Source


Description
-----------
Reads from an Oracle table using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from an Oracle. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.
Typically, the name of the table/view.

**Use Connection** Whether to use a connection. If a connection is used, you do not need to provide the credentials.

**Connection** Name of the connection to use. Project and service account information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Oracle is running on.

**Port:** Port that Oracle is running on.

**Connection Type** Whether to use an SID, Service Name, or TNS Connect Descriptor when connecting to the database.

**SID/Service Name/TNS Connect Descriptor:** Oracle connection point (Database name, Service name, or a TNS Connect Descriptor). When using TNS, place
the full TNS Connect Descriptor in the text field. For example:
(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 123.123.123.123)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)
(SERVICE_NAME = XE)))

**Role** Login role of the user when connecting to the database.

**Transaction Isolation Level** The transaction isolation level of the databse connection
- TRANSACTION_READ_COMMITTED: No dirty reads. Non-repeatable reads and phantom reads are possible.
- TRANSACTION_SERIALIZABLE (default): No dirty reads. Non-repeatable and phantom reads are prevented.
- Note: If the user role selected is SYSDBA or SYSOPER, the plugin will default to TRANSACTION_READ_COMMITTED to prevent ORA-08178 errors

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

**Default Batch Value:** The default batch value that triggers an execution request.

**Default Row Prefetch:** The default number of rows to prefetch from the server.

**Fetch Size:** The number of rows to fetch at a time per split. Larger fetch size can result in faster import,
with the tradeoff of higher memory usage.

Data Types Mapping
----------

    | Oracle Data Type               | CDAP Schema Data Type | Comment                                                    |
    | ------------------------------ | --------------------- | -----------------------------------------------------------|
    | VARCHAR2                       | string                |                                                            |
    | NVARCHAR2                      | string                |                                                            |
    | VARCHAR                        | string                |                                                            |
    | NUMBER                         | string                | For NUMBER types defined without a precision and scale.    |
    |                                |                       | Users can manually set output schema to map it to Decimal. |
    | NUMBER                         | decimal               | For NUMBER types defined with a precision and scale.       |
    | FLOAT                          | double                |                                                            |
    | LONG                           | string                |                                                            |
    | DATE                           | timestamp             |                                                            |
    | BINARY_FLOAT                   | float                 |                                                            |
    | BINARY_DOUBLE                  | double                |                                                            |
    | TIMESTAMP                      | timestamp             |                                                            |
    | TIMESTAMP WITH TIME ZONE       | string                |                                                            |
    | TIMESTAMP WITH LOCAL TIME ZONE | timestamp             |                                                            |
    | INTERVAL YEAR TO MONTH         | string                |                                                            |
    | INTERVAL DAY TO SECOND         | string                |                                                            |
    | RAW                            | bytes                 |                                                            |
    | LONG RAW                       | bytes                 |                                                            |
    | ROWID                          | string                |                                                            |
    | UROWID                         | string                |                                                            |
    | CHAR                           | string                |                                                            |
    | NCHAR                          | string                |                                                            |
    | CLOB                           | string                |                                                            |
    | NCLOB                          | string                |                                                            |
    | BLOB                           | bytes                 |                                                            |
    | BFILE                          | bytes                 | BFILE is a data type used to store a locator (link)        |
    |                                |                       | to an external file, which is stored outside of the        |
    |                                |                       | database. Only the locator will be read from an            |
    |                                |                       | Oracle table and not the content of the external file.     |


Example
------
Suppose you want to read data from Oracle database named "XE" that is running on "localhost" port 1251,
as "system" user with "oracle" password (Ensure that the driver for Oracle is installed. You can also provide 
driver name for some specific driver, otherwise "oracle" will be used), then configure plugin with: 


```
Reference Name: "src1"
Driver Name: "oracle"
Host: "localhost"
Port: 1251
Connection Type: "Service Name"
SID/Service Name/TNS Connect Descriptor: "XE"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
Username: "system"
Password: "oracle"
Default Batch Value: 10
Default Row Prefetch: 40
```  

For example, if the 'id' column is a primary key of type int and the other columns are
non-nullable varchars, output records will have this schema:

    | field name     | type                |
    | -------------- | ------------------- |
    | id             | int                 |
    | name           | string              |
    | email          | string              |
    | phone          | string              |
