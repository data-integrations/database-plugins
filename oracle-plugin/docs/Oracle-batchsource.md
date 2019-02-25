# Oracle Batch Source


Description
-----------
Reads from a Oracle using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from a Oracle. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Oracle is running on.

**Port:** Port that Oracle is running on.

**Database:** Oracle database name.

**Import Query:** The SELECT query to use to import data from the specified table.
You can specify an arbitrary number of columns to import, or import all columns using \*. The Query should
contain the '$CONDITIONS' string. For example, 'SELECT * FROM table WHERE $CONDITIONS'.
The '$CONDITIONS' string will be replaced by 'splitBy' field limits specified by the bounding query.
The '$CONDITIONS' string is not required if numSplits is set to one. (Macro-enabled)

**Bounding Query:** Bounding Query should return the min and max of the values of the 'splitBy' field.
For example, 'SELECT MIN(id),MAX(id) FROM table'. Not required if numSplits is set to one.

**Split-By Field Name:** Field Name which will be used to generate splits. Not required if numSplits is set to one. (Macro-enabled)

**Number of Splits to Generate:** Number of splits to generate.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Default Batch Value:** The default batch value that triggers an execution request.

**Default Row Prefetch:** The default number of rows to prefetch from the server.


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
Database: "XE"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
Username: "system"
Password: "oracle"
Default Batch Value: 10
Default Row Prefetch: 40
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
