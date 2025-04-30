# SQL Server Connection


Description
-----------
Use this connection to access data in a SQL Server database using JDBC.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**JDBC Driver name:** Select the JDBC driver to use.

**Host:** Host name or IP address of the database server to connect to.

**Port:** Port number of the database server to connect to. Default is 1433.

**Username:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**Password:** Password to use to connect to the specified database.

**Transaction Isolation Level** The transaction isolation level of the database connection
- TRANSACTION_READ_COMMITTED: No dirty reads. Non-repeatable reads and phantom reads are possible.
- TRANSACTION_SERIALIZABLE: No dirty reads. Non-repeatable and phantom reads are prevented.
- TRANSACTION_REPEATABLE_READ: No dirty reads. Prevents non-repeatable reads, but phantom reads are still possible.
- TRANSACTION_READ_UNCOMMITTED: Allows dirty reads (reading uncommitted changes from other transactions). Non-repeatable reads and phantom reads are possible.

For more details on the Transaction Isolation Levels supported in SQL Server, refer to the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql?view=sql-server-ver16)

**Authentication Type:** Indicates which authentication method will be used for the connection. Use 'SQL Login'. to
connect to a SQL Server using username and password properties. Use 'Active Directory Password' to connect to an Azure
SQL Database/Data Warehouse using an Azure AD principal name and password.

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

1. `/{database}/{schema}/{table}`
   This path indicates a table. A table is the only one that can be sampled. Browse on this path to return the specified table.

2. `/{database}/{schema}`
   This path indicates a schema. A schema cannot be sampled. Browse on this path to get all the tables under this schema.

3. `/{database}`
   This path indicates a database. A database cannot be sampled. Browse on this path to get all the schemas under this database.

4. `/`
   This path indicates the root. A root cannot be sampled. Browse on this path to get all the databases visible through this connection.