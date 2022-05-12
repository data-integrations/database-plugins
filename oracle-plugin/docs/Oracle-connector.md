# Oracle Connection


Description
-----------
Use this connection to access data in an Oracle database using JDBC.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**JDBC Driver name:** Select the JDBC driver to use.

**Host:** Host name or IP address of the database server to connect to.

**Port:** Port number of the database server to connect to. If not specified will default to 1521.

**Username:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**Password:** Password to use to connect to the specified database.

**SID/Service Name/TNS Connect Descriptor:** Oracle connection point (Database name, Service name, or a TNS Connect Descriptor). When using TNS, place
the full TNS Connect Descriptor in the text field. For example:
(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 123.123.123.123)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)
(SERVICE_NAME = XE)))

**Connection Type** Whether to use an SID, Service Name, or TNS Connect Descriptor when connecting to the database.

**Role:** Login role of the user when connecting to the database.

**Transaction Isolation Level** The transaction isolation level of the databse connection
- TRANSACTION_READ_COMMITTED: No dirty reads. Non-repeatable reads and phantom reads are possible.
- TRANSACTION_SERIALIZABLE (default): No dirty reads. Non-repeatable and phantom reads are prevented.
- Note: If the user role selected is SYSDBA or SYSOPER, the plugin will default to TRANSACTION_READ_COMMITTED to prevent ORA-08178 errors

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.
This is a semicolon-separated list of key-value pairs, where each pair is separated by a equals '=' and specifies
the key and value for the argument. For example, 'key1=value1;key2=value' specifies that the connection will be
given arguments 'key1' mapped to 'value1' and the argument 'key2' mapped to 'value2'.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body. It can be in the following form :

1. `/{schema}/{table}`
   This path indicates a table. A table is the only one that can be sampled. Browse on this path to return the specified table.

2. `/{schema}`
   This path indicates a schema. A schema cannot be sampled. Browse on this path to get all the tables under this schema.

3. `/`
   This path indicates the root. A root cannot be sampled. Browse on this path to get all the schemas visible through this connection.
