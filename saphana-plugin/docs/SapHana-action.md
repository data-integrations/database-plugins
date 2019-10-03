# SAP HANA Action


Description
-----------
Action that runs a SAP HANA command.


Use Case
--------
The action can be used whenever you want to run a SAP HANA command before or after a data pipeline.
For example, you may want to run a sql update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Host:** Host that SAP HANA is running on.

**Port:** Port that SAP HANA is running on.

**Database:** SAP HANA database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.


Example
-------

//TODO