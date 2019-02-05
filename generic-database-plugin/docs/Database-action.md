# Database Action


Description
-----------
Action that runs a database command.


Use Case
--------
The action can be used whenever you want to run a database command before or after a data pipeline.
For example, you may want to run a sql update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Connection String:** JDBC connection string including database name.

**Database Command:** Database command to execute.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.

**Transaction Isolation Level:** The transaction isolation level for queries run by this sink.

Example
-------
Suppose you want to execute a query against a PostgreSQL database named "prod" that is running on "localhost" 
port 5432. Ensure that the driver for PostgreSQL is installed, then configure the plugin with:

```
Driver Name: "postgres"
Connection String: "jdbc:postgresql://localhost:5432/prod"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
```
