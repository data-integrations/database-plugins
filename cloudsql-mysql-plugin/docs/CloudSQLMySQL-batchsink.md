# CloudSQL MySQL Batch Sink


Description
-----------
Writes records to a CloudSQL MySQL table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a CloudSQL MySQL table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a GCS bucket and you want to export the contents
of the bucket to a CloudSQL MySQL table where it can be served to your users.

Column names would be autodetected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Database:** MySQL database name.

**Connection Name:** The CloudSQL instance to connect to in the format <PROJECT_ID>:\<REGION>:<INSTANCE_NAME>. 
Can be found in the instance overview page.

**CloudSQL Instance Type:** Whether the CloudSQL instance to connect to is private or public. Defaults to 'Public'.

**Table Name:** Name of the table to export to. Table must exist prior to running the pipeline.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Transaction Isolation Level:** Transaction isolation level for queries run by this sink. 

**Connection Timeout:** The timeout value (in seconds) used for socket connect operations. If connecting to the server 
takes longer than this value, the connection is broken. A value of 0 means that it is disabled.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.


Data Types Mapping
------------------

    | MySQL Data Type                | CDAP Schema Data Type | 
    | ------------------------------ | --------------------- | 
    | BIT                            | boolean               | 
    | TINYINT                        | int                   | 
    | BOOL, BOOLEAN                  | boolean               | 
    | SMALLINT                       | int                   | 
    | MEDIUMINT                      | double                | 
    | INT,INTEGER                    | int                   | 
    | BIGINT                         | long                  | 
    | FLOAT                          | float                 | 
    | DOUBLE                         | double                | 
    | DECIMAL                        | decimal               | 
    | DATE                           | date                  | 
    | DATETIME                       | timestamp             | 
    | TIMESTAMP                      | timestamp             | 
    | TIME                           | time                  | 
    | YEAR                           | date                  | 
    | CHAR                           | string                | 
    | VARCHAR                        | string                | 
    | BINARY                         | bytes                 | 
    | VARBINARY                      | bytes                 | 
    | TINYBLOB                       | bytes                 | 
    | TINYTEXT                       | string                | 
    | BLOB                           | bytes                 | 
    | TEXT                           | string                | 
    | MEDIUMBLOB                     | bytes                 | 
    | MEDIUMTEXT                     | string                | 
    | LONGBLOB                       | bytes                 | 
    | LONGTEXT                       | string                | 
    | ENUM                           | string                | 
    | SET                            | string                | 



Examples
--------
**Connecting to a public CloudSQL MySQL instance**

Suppose you want to write output records to "users" table of CloudSQL MySQL database named "prod", as "root" user with 
"root" password (Get the latest version of the CloudSQL socket factory jar with driver and dependencies 
[here](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/releases), then configure plugin with: 


```
Reference Name: "sink1"
Driver Name: "cloudsql-mysql"
Connection Name: [PROJECT_ID]:[REGION_ID]:[INSTANCE_NAME]
CloudSQL Instance Type: "Public"
Database: "prod"
Table Name: "users"
Username: "root"
Password: "root"
```
  
  
**Connecting to a private CloudSQL MySQL instance**

If you want to connect to a private CloudSQL MySQL instance, create a Compute Engine VM that runs the CloudSQL Proxy 
docker image using the following command

```
# Set the environment variables
export PROJECT=[project_id]
export REGION=[vm-region]
export ZONE=`gcloud compute zones list --filter="name=${REGION}" --limit
1 --uri --project=${PROJECT}| sed 's/.*\///'`
export SUBNET=[vpc-subnet-name]
export NAME=[gce-vm-name]
export MYSQL_CONN=[mysql-instance-connection-name]

# Create a Compute Engine VM
gcloud beta compute --project=${PROJECT_ID} instances create ${INSTANCE_NAME}
--zone=${ZONE} --machine-type=g1-small --subnet=${SUBNE} --no-address
--metadata=startup-script="docker run -d -p 0.0.0.0:3306:3306
gcr.io/cloudsql-docker/gce-proxy:1.16 /cloud_sql_proxy
-instances=${MYSQL_CONNECTION_NAME}=tcp:0.0.0.0:3306" --maintenance-policy=MIGRATE
--scopes=https://www.googleapis.com/auth/cloud-platform
--image=cos-69-10895-385-0 --image-project=cos-cloud
```

Optionally, you can promote the internal IP address of the VM running the Proxy image to a static IP using

```
# Get the VM internal IP
export IP=`gcloud compute instances describe ${NAME} --zone ${ZONE} |
grep "networkIP" | awk '{print $2}'`

# Promote the VM internal IP to static IP
gcloud compute addresses create mysql-proxy --addresses ${IP} --region
${REGION} --subnet ${SUBNET}

# Note down the IP to be used in MySQL or PostgreSQL JDBC 
# connection string
echo Proxy IP: ${IP}

echo "JDBC Connection strings:"
echo "jdbc:postgresql://${IP}:5432/{PostgreSQL_DB_NAME}"
echo "jdbc:mysql://${IP}:3306/{MySQL_DB_NAME}"
```

Get the latest version of the CloudSQL socket factory jar with driver and dependencies from
[here](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/releases), then configure plugin with:

```
Reference Name: "sink1"
Driver Name: "cloudsql-mysql"
Connection Name: <proxy-ip> (obtained from commands above)
CloudSQL Instance Type: "Private"
Database: "prod"
Table Name: "users"
Username: "root"
Password: "root"
```
