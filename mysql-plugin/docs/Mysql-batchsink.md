# MySQL Batch Sink


Description
-----------
Writes records to a MySQL table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a MySQL table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a MySQL table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that MySQL is running on.

**Port:** Port that MySQL is running on.

**Database:** MySQL database name.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Auto Reconnect:** Should the driver try to re-establish stale and/or dead connections.

**Use SSL:** Turns on SSL encryption. The connection will fail if SSL is not available.

**Keystore URL:** URL to the client certificate KeyStore (if not specified, use defaults). Must be accessible at the
same location on host where CDAP Master is running and all hosts on which at least one HDFS, MapReduce, or YARN daemon
role is running.

**Keystore Password:** Password for the client certificates KeyStore.

**Truststore URL:** URL to the trusted root certificate KeyStore (if not specified, use defaults). Must be accessible at
the same location on host where CDAP Master is running and all hosts on which at least one HDFS, MapReduce, or YARN
daemon role is running.

**Truststore Password:** Password for the trusted root certificates KeyStore

**Use Compression:** Use zlib compression when communicating with the server. Select this option for WAN
connections.

**SQL_MODE:** Override the default SQL_MODE session variable used by the server.


Example
-------
Suppose you want to write output records to "users" table of MySQL database named "prod" that is running on "localhost", 
port 3306, as "root" user with "root" password (Ensure that the driver for MySQL is installed. You can also provide 
driver name for some specific driver, otherwise "mysql" will be used), then configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "mariadb"
Host: "localhost"
Port: 3306
Database: "prod"
Table Name: "users"
Username: "root"
Password: "root"
```
