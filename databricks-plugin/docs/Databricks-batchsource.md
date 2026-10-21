# Databricks Batch Source

Description
-----------
Reads data from a Databricks table using a configurable SQL query.

Properties
----------
* **Use Connection**: Whether to use an existing Databricks connection.
* **Host**: Server Hostname of the Databricks cluster or SQL warehouse.
* **Port**: Database port (default is 443).
* **HTTP Path**: The HTTP Path for the Databricks cluster or SQL warehouse.
* **Reference Name**: Name used to identify this source for lineage.
* **Database / Catalog**: Optional catalog or database name.
* **Import Query**: SQL query to execute against Databricks.
* **Transaction Isolation Level**: Select `TRANSACTION_READ_UNCOMMITTED` when using Databricks JDBC 2.6.40, or `TRANSACTION_REPEATABLE_READ` when using Databricks JDBC 3.4.3. The 3.4.3 driver requires Java 11 or later.
