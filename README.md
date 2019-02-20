# database-plugins

In order to run integration tests for these plugins you should have running database instance. It may be running on host
machine or in docker container. Tests create tables and sample data, so user configured via system property should have 
corresponding permissions. To run integration tests execute following command in shell:
```
mvn clean test \ 
-Dmysql.host=localhost -Dmysql.port=3306 -Dmysql.database=mydb -Dmysql.username=root -Dmysql.password=root \
-Dpostgresql.host=localhost -Dpostgresql.port=54032 -Dpostgresql.database=test -Dpostgresql.username=postgres \ 
-Dpostgresql.password=cdap
```
