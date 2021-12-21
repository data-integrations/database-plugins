# CloudSQL PostgreSQL Batch Source


Description
-----------
Reads from a CloudSQL PostgreSQL database table(s) using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from a CloudSQL PostgreSQL instance database. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Database:** CloudSQL PostgreSQL database name.

**Connection Name:** The CloudSQL instance to connect to in the format <PROJECT_ID>:\<REGION>:<INSTANCE_NAME>.
Can be found in the instance overview page.

**CloudSQL Instance Type:** Whether the CloudSQL instance to connect to is private or public. Defaults to 'Public'.

**Import Query:** The SELECT query to use to import data from the specified table.
You can specify an arbitrary number of columns to import, or import all columns using \*. The Query should
contain the '$CONDITIONS' string. For example, 'SELECT * FROM table WHERE $CONDITIONS'.
The '$CONDITIONS' string will be replaced by 'splitBy' field limits specified by the bounding query.
The '$CONDITIONS' string is not required if numSplits is set to one.

**Bounding Query:** Bounding Query should return the min and max of the values of the 'splitBy' field.
For example, 'SELECT MIN(id),MAX(id) FROM table'. Not required if numSplits is set to one.

**Split-By Field Name:** Field Name which will be used to generate splits. Not required if numSplits is set to one.

**Number of Splits to Generate:** Number of splits to generate.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Schema:** The schema of records output by the source. This will be used in place of whatever schema comes
back from the query. However, it must match the schema that comes back from the query,
except it can mark fields as nullable and can contain a subset of the fields.

**Fetch Size:** The number of rows to fetch at a time per split. Larger fetch size can result in faster import,
with the tradeoff of higher memory usage.

Examples
--------
**Connecting to a public CloudSQL PostgreSQL instance**

Suppose you want to read data from CloudSQL PostgreSQL database named "prod", as "postgres" user with "postgres" 
password (Get the latest version of the CloudSQL socket factory jar with driver and dependencies 
[here](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/releases)), then configure plugin with:


```
Reference Name: "src1"
Driver Name: "cloudsql-postgresql"
Database: "prod"
Connection Name: [PROJECT_ID]:[REGION]:[INSTANCE_NAME]
CloudSQL Instance Type: "Public"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
Username: "postgres"
Password: "postgres"
```  

For example, if the 'id' column is a primary key of type int and the other columns are
non-nullable varchars, output records will have this schema:

    | field name     | type                |
    | -------------- | ------------------- |
    | id             | int                 |
    | name           | string              |
    | email          | string              |
    | phone          | string              |

  
  
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
Reference Name: "src1"
Driver Name: "cloudsql-postgresql"
Database: "prod"
Connection Name: <proxy-ip> (obtained from commands above)
CloudSQL Instance Type: "Private"
Import Query: "select id, name, email, phone from users;"
Number of Splits to Generate: 1
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
| bigserial                                           | long                  |                                              |
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
| smallserial                                         | int                   |                                              |
| serial                                              | int                   |                                              |
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
