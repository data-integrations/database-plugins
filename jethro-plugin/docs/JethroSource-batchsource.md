# Jethro Batch Source


Description
-----------
Reads from a Jethro Data instance using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from a Jethro Data instance. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Jethro Data is running on.

**Port:** Port that Jethro Data is running on.

**Instance:** Jethro Data instance name.

**Import Query:** The SELECT query to use to import data from the specified table.
You can specify an arbitrary number of columns to import, or import all columns using \*. The Query should
contain the '$CONDITIONS' string. For example, 'SELECT * FROM table WHERE $CONDITIONS'.
The '$CONDITIONS' string will be replaced by 'splitBy' field limits specified by the bounding query.
The '$CONDITIONS' string is not required if numSplits is set to one.

**Bounding Query:** Bounding Query should return the min and max of the values of the 'splitBy' field.
For example, 'SELECT MIN(id),MAX(id) FROM table'. Not required if numSplits is set to one.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Split-By Field Name:** Field Name which will be used to generate splits. Not required if numSplits is set to one.

**Number of Splits to Generate:** Number of splits to generate.

**Schema:** The schema of records output by the source. This will be used in place of whatever schema comes
back from the query. However, it must match the schema that comes back from the query,
except it can mark fields as nullable and can contain a subset of the fields.

Data Types Mapping
----------

    | Jethro Data Type               | CDAP Schema Data Type | Comment                                            |
    | ------------------------------ | --------------------- | -------------------------------------------------- |
    | INTEGER                        | int                   |                                                    |
    | BIGINT                         | long                  |                                                    |
    | FLOAT                          | float                 |                                                    |
    | DOUBLE                         | double                |                                                    |
    | STRING                         | string                |                                                    |
    | TIMESTAMP                      | timestamp             |                                                    |
