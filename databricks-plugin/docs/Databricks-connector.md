# Databricks Database Connector

Description
-----------
Connects to Databricks database / Lakehouse via JDBC.

Properties
----------
* **Host**: Server Hostname of the Databricks cluster or SQL warehouse.
* **Port**: Database port (default is 443).
* **HTTP Path**: The HTTP Path for the Databricks cluster or SQL warehouse.
* **Database / Catalog**: Optional catalog or database name to connect to.
* **Username**: Username / token user.
* **Password / Token**: Personal Access Token (PAT) or password.
* **Transaction Isolation Level**: Select `TRANSACTION_READ_UNCOMMITTED` when using Databricks JDBC 2.6.40, or `TRANSACTION_REPEATABLE_READ` when using Databricks JDBC 3.4.3. The 3.4.3 driver requires Java 11 or later.
* **Connection Arguments**: Arbitrary key-value pairs to pass as connection arguments to the JDBC driver (e.g. `AuthMech=11;Auth_Flow=2`).
