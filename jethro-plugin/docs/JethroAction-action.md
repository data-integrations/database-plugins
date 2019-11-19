# Jethro Action


Description
-----------
Action that runs a Jethro command.


Use Case
--------
The action can be used whenever you want to run a Jethro command before a data pipeline.
For example, you may want to run a sql command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Jethro is running on.

**Port:** Port that Jethro is running on.

**Instance:** Jethro database name.

**Database Command:** Database command to execute.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

Example
-------
Suppose you want to execute a query against a Jethro database instance named "prod" that is running on "localhost" 
port 9112, then configure the plugin with:

```
Driver Name: "jethro"
Host: "localhost"
Port: 9112
Database: "prod"
Database Command: "TRUNCATE TABLE testTable"
```