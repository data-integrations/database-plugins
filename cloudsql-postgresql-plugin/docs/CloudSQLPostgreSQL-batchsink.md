# CloudSQL PostgreSQL Batch Sink


Description
-----------
Writes records to a CloudSQL PostgreSQL table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a CloudSQL PostgreSQL table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a GCS bucket and you want to export the contents
of the bucket to a CloudSQL PostgreSQL table where it can be served to your users.

Column names would be auto detected from input schema.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.
Typically, the name of the table/view.

**Use Connection** Whether to use a connection. If a connection is used, you do not need to provide the credentials.

**Connection** Name of the connection to use. Project and service account information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.

**Driver Name:** Name of the JDBC driver to use.

**Database:** CloudSQL PostgreSQL database name.

**Connection Name:** The CloudSQL instance to connect to in the format <PROJECT_ID>:\<REGION>:<INSTANCE_NAME>.
Can be found in the instance overview page.

**CloudSQL Instance Type:** Whether the CloudSQL instance to connect to is private or public. Defaults to 'Public'.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Transaction Isolation Level:** Transaction isolation level for queries run by this sink. 

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Connection Timeout** The timeout value used for socket connect operations. If connecting to the server takes longer
than this value, the connection is broken.The timeout is specified in seconds and a value of zero means that it is 
disabled.


Examples
--------
**Connecting to a public CloudSQL PostgreSQL instance**

Suppose you want to write output records to "users" table of CloudSQL PostgreSQL database named "prod", as "postgres" 
user with "postgres" password (Get the latest version of the CloudSQL socket factory jar with driver and dependencies 
[here](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/releases)), then configure plugin with:


```
Reference Name: "sink1"
Driver Name: "cloudsql-postgresql"
Database: "prod"
Connection Name: [PROJECT_ID]:[REGION]:[INSTANCE_NAME]
CloudSQL Instance Type: "Public"
Table Name: "users"
Username: "postgres"
Password: "postgres"
```
  
  
**Connecting to a private CloudSQL PostgreSQL instance**

If you want to connect to a private CloudSQL PostgreSQL instance, create a Compute Engine VM that runs the CloudSQL Proxy 
docker image using the following command

```
# Set the environment variables
export PROJECT=[project_id]
export REGION=[vm-region]
export ZONE=`gcloud compute zones list --filter="name=${REGION}" --limit
1 --uri --project=${PROJECT}| sed 's/.*\///'`
export SUBNET=[vpc-subnet-name]
export NAME=[gce-vm-name]
export POSTGRESQL_CONN=[postgresql-instance-connection-name]

# Create a Compute Engine VM
gcloud beta compute --project=${PROJECT_ID} instances create ${INSTANCE_NAME}
--zone=${ZONE} --machine-type=g1-small --subnet=${SUBNE} --no-address
--metadata=startup-script="docker run -d -p 0.0.0.0:3306:3306
gcr.io/cloudsql-docker/gce-proxy:1.16 /cloud_sql_proxy
-instances=${POSTGRESQL_CONNECTION_NAME}=tcp:0.0.0.0:3306" --maintenance-policy=MIGRATE
--scopes=https://www.googleapis.com/auth/cloud-platform
--image=cos-69-10895-385-0 --image-project=cos-cloud  
```

Optionally, you can promote the internal IP address of the VM running the Proxy image to a static IP using

```
# Get the VM internal IP
export IP=`gcloud compute instances describe ${NAME} --zone ${ZONE} |
grep "networkIP" | awk '{print $2}'`

# Promote the VM internal IP to static IP
gcloud compute addresses create postgresql-proxy --addresses ${IP} --region
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
Driver Name: "cloudsql-postgresql"
Database: "prod"
Connection Name: <proxy-ip> (obtained from commands above)
CloudSQL Instance Type: "Private"
Table Name: "users"
Username: "postgres"
Password: "postgres"
``` 


Data Types Mapping
------------------
All PostgreSQL specific data types mapped to string and can have multiple input formats and one 'canonical' output form.
Please, refer to PostgreSQL data types documentation to figure out proper formats.

| PostgreSQL Data Type                                | CDAP Schema Data Type | Comment                                      |
|-----------------------------------------------------|-----------------------|----------------------------------------------|
| bigint                                              | long                  |                                              |
| bit(n)                                              | string                | string with '0' and '1' chars exact n length |
| bit varying(n)                                      | string                | string with '0' and '1' chars max n length   |
| boolean                                             | boolean               |                                              |
| bytea                                               | bytes                 |                                              |
| character                                           | string                |                                              |
| character varying                                   | string                |                                              |
| double precision                                    | double                |                                              |
| integer                                             | int                   |                                              |
| numeric(precision, scale)/decimal(precision, scale) | decimal               |                                              |
| real                                                | float                 |                                              |
| smallint                                            | int                   |                                              |
| text                                                | string                |                                              |
| date                                                | date                  |                                              |
| time [ (p) ] [ without time zone ]                  | time                  |                                              |
| time [ (p) ] with time zone                         | string                |                                              |
| timestamp [ (p) ] [ without time zone ]             | timestamp             |                                              |
| timestamp [ (p) ] with time zone                    | timestamp             | stored in UTC format in database             |
| xml                                                 | string                |                                              |
| tsquery                                             | string                |                                              |
| tsvector                                            | string                |                                              |
| uuid                                                | string                |                                              |
| box                                                 | string                |                                              |
| cidr                                                | string                |                                              |
| circle                                              | string                |                                              |
| inet                                                | string                |                                              |
| interval                                            | string                |                                              |
| json                                                | string                |                                              |
| jsonb                                               | string                |                                              |
| line                                                | string                |                                              |
| lseg                                                | string                |                                              |
| macaddr                                             | string                |                                              |
| macaddr8                                            | string                |                                              |
| money                                               | string                |                                              |
| path                                                | string                |                                              |
| point                                               | string                |                                              |
| polygon                                             | string                |                                              |
