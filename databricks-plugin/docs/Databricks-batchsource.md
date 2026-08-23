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
