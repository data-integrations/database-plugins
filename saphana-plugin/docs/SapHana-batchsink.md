# SAP HANA Batch Sink


Description
-----------

This sink is used whenever you need to write to a SAP HANA table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a SAP HANA table where it can be served to your users.

Column names would be autodetected from input schema.


Use Case
--------
This sink is used whenever you need to write to a SAP HANA table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a SAP HANA table where it can be served to your users.

Column names would be autodetected from input schema.



Properties
----------

**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Table Name:** Name of the table to export to.

**Host:** Host that SAP HANA is running on.

**Port:** Port that SAP HANA is running on.

**Database:** SAP HANA database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.