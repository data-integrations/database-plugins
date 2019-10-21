# database-plugins

In order to run integration tests for these plugins you should have running database instance. It may be running on host
machine or in docker container. Tests create tables and sample data, so user configured via system property should have 
corresponding permissions. To run integration tests execute following command in shell:
```
mvn clean test \ 
-DauroraMysql.clusterEndpoint=cdap-cluster.xyz.eu-central-1.rds.amazonaws.com -DauroraMysql.port=3306 \
-DauroraMysql.database=cdapdb -DauroraMysql.username=cdap -DauroraMysql.password=cdap \
-DauroraPostgresql.clusterEndpoint=pginstance.cxywmbgwp60k.eu-central-1.rds.amazonaws.com -DauroraPostgresql.port=5432 \
-DauroraPostgresql.database=cdappg -DauroraPostgresql.username=cdap -DauroraPostgresql.password=cdap \
-DjdbcDriversJars="/jdbc/drivers/jars/some.jar, "
```
Notice that you must change properties for Aurora MySQL and Aurora Postgresql to real before running tests.
## Setup Local Environment
MySQL, Postgresql, MSSQL, DB2, SAP HANA are using prebuild images.

Oracle DB image should be build separately.

Note that you should login into docker account to pull SAP HANA image. 
Account can be created [here](https://hub.docker.com/signup)
Also, please note SAP HANA is sensitive to some CPU instructions.
CPU model "host-passthrough" or similar can be required if running inside VM.
SAP HANA requires that password for DB is provided through url.
Convenience script ```docker-compose/db-plugins-env/saphana-password-server.sh``` 
provided for this purpose.

Netezza requires VMware Player for running Netezza emulator.

* [Install Docker Compose](https://docs.docker.com/compose/install/)
* Build local docker images
  * [Build Oracle DB docker image version 12.1.0.2-ee](https://github.com/oracle/docker-images/tree/master/OracleDatabase/SingleInstance)
* Start docker environment by running commands:
```bash
bash saphana-password-server.sh &
cd docker-compose/db-plugins-env/
docker-compose up -d
```
* [Install and start Netezza emulator](http://dwgeek.com/install-vmware-player-netezza-emulator.html/)
* Create database `mydb` in Netezza emulator

### Properties
#### MySQL
* **mysql.host** - Server host. Default: localhost.
* **mysql.port** - Server port. Default: 3306.
* **mysql.database** - Server namespace for test databases. Default: mydb.
* **mysql.username** - Server username. Default: root.
* **mysql.password** - Server password. Default: 123Qwe123.
#### Postgresql
* **postgresql.host** - Server host. Default: localhost.
* **postgresql.port** - Server port. Default: 5432.
* **postgresql.database** - Server namespace for test databases. Default: mydb.
* **postgresql.username** - Server username. Default: postgres.
* **postgresql.password** - Server password. Default: 123Qwe123.
#### MSSQL
* **mssql.host** - Server host. Default: localhost.
* **mssql.port** - Server port. Default: 1433.
* **mssql.database** - Server namespace for test databases. Default: tempdb.
* **mssql.username** - Server username. Default: sa.
* **mssql.password** - Server password. Default: 123Qwe123.
#### DB2
* **db2.host** - Server host. Default: localhost.
* **db2.port** - Server port. Default: 50000. 
* **db2.database** - Server namespace for test databases. Default: mydb.
* **db2.username** - Server username. Default: db2inst1.
* **db2.password** - Server password. Default: 123Qwe123.
#### Oracle
* **oracle.host** - Server host. Default: localhost.
* **oracle.port** - Server port. Default: 1521.
* **oracle.username** - Server username. Default: SYSTEM.
* **oracle.password** - Server password. Default: 123Qwe123.
* **oracle.database** - Server sid/database. Default: cdap.
* **oracle.connectionType** - Server connection type (service/sid) Default: sid.
#### Netezza
* **netezza.host** - Server host. Default: localhost.
* **netezza.port** - Server port. Default: 5480.
* **netezza.database** - Server namespace for test databases. Default: mydb.
* **netezza.username** - Server username. Default: admin.
* **netezza.password** - Server password. Default: password.
#### Aurora MySQL
* **auroraMysql.clusterEndpoint** - Cluster endpoint.
* **auroraMysql.port** - Server port.
* **auroraMysql.database** - Server namespace for test databases.
* **auroraMysql.username** - Server username.
* **auroraMysql.password** - Server password.
#### Aurora Postgresql
* **auroraPostgresql.clusterEndpoint** - Cluster endpoint.
* **auroraPostgresql.port** - Server port.
* **auroraPostgresql.database** - Server namespace for test databases. Default: mydb.
* **auroraPostgresql.username** - Server username.
* **auroraPostgresql.password** - Server password.  