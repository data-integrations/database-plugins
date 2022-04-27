# Oracle Batch Sink


Description
-----------
Writes records to an Oracle table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to an Oracle table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to Oracle table where it can be served to your users.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Oracle is running on.

**Port:** Port that Oracle is running on.

**Role** Login role of the user when connecting to the database. For eg, NORMAL, SYSDBA, SYSOPER, etc.

**Transaction Isolation Level** The transaction isolation level of the databse connection
- TRANSACTION_READ_COMMITTED: No dirty reads. Non-repeatable reads and phantom reads are possible.
- TRANSACTION_SERIALIZABLE (default): No dirty reads. Non-repeatable and phantom reads are prevented.
- Note: If the user role selected is SYSDBA or SYSOPER, the plugin will default to TRANSACTION_READ_COMMITTED to prevent ORA-08178 errors

**Connection Type** Whether to use an SID or Service Name when connecting to the database.

**SID/Service Name/TNS Connect Descriptor:** Oracle connection point (Database name, Service name, or a TNS Connect Descriptor). When using TNS, place
the full TNS Connect Descriptor in the text field. For example:
(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 123.123.123.123)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)
(SERVICE_NAME = XE)))

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Default Batch Value:** The default batch value that triggers an execution request.


Data Types Mapping
----------

    | Oracle Data Type               | CDAP Schema Data Type | Comment                                                |
    | ------------------------------ | --------------------- | ------------------------------------------------------ |
    | VARCHAR2                       | string                |                                                        |
    | NVARCHAR2                      | string                |                                                        |
    | VARCHAR                        | string                |                                                        |
    | NUMBER                         | decimal               |                                                        |
    | FLOAT                          | double                |                                                        |
    | LONG                           | string                |                                                        |
    | DATE                           | timestamp             |                                                        |
    | BINARY_FLOAT                   | float                 |                                                        |
    | BINARY_DOUBLE                  | double                |                                                        |
    | TIMESTAMP                      | timestamp             |                                                        |
    | TIMESTAMP WITH TIME ZONE       | string                | Timestamp string in the following format:              |
    |                                |                       | "2019-07-15 15:57:46.65 GMT"                           |
    | TIMESTAMP WITH LOCAL TIME ZONE | timestamp             |                                                        |
    | INTERVAL YEAR TO MONTH         | string                | Oracle's 'INTERVAL YEAR TO MONTH' literal in the       |
    |                                |                       | standard format: "year[-month]"                        |
    | INTERVAL DAY TO SECOND         | string                | Oracle's 'INTERVAL DAY TO SECOND' literal in the       |
    |                                |                       | standard format:                                       |
    |                                |                       | "[day] [hour][:minutes][:seconds[.milliseconds]"       |
    | RAW                            | bytes                 |                                                        |
    | LONG RAW                       | bytes                 |                                                        |
    | ROWID                          | string                |                                                        |
    | UROWID                         | string                |                                                        |
    | CHAR                           | string                |                                                        |
    | NCHAR                          | string                |                                                        |
    | CLOB                           | string                |                                                        |
    | NCLOB                          | string                |                                                        |
    | BLOB                           | bytes                 |                                                        |
    | BFILE                          |                       | BFILE data type is not supported for the sink          |


Example
-------
Suppose you want to write output records to "users" table of Oracle database named "XE" that is running on "localhost", 
port 1251, as "system" user with "oracle" password (Ensure that the driver for Oracle is installed. You can also provide 
driver name for some specific driver, otherwise "oracle" will be used), then configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "oracle"
Host: "localhost"
Port: 1251
Database: "XE"
Table Name: "users"
Username: "system"
Password: "oracle"
```
