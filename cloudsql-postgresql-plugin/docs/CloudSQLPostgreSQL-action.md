# PostgreSQL Action


Description
-----------
Action that runs a PostgreSQL command on a CloudSQL PostgreSQL instance.


Use Case
--------
The action can be used whenever you want to run a PostgreSQL command before or after a data pipeline.
For example, you may want to run a SQL update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Database:** PostgreSQL database name.

**Connection Name:** The CloudSQL instance to connect to in the format <PROJECT_ID>:\<REGION>:<INSTANCE_NAME>. 
Can be found in the instance overview page.

**Port:** Port that PostgreSQL is running on.

**CloudSQL Instance Type:** Whether the CloudSQL instance to connect to is private or public. Defaults to 'Public'.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Authentication Type:** How to authenticate to the database. `Basic` uses the Username and Password.
`IAM` uses Cloud SQL IAM database authentication as the principal in Service Account Email, and the Username
and Password are not used. For setup steps, see the IAM Database Authentication section of the
CloudSQL PostgreSQL connection documentation.

**Service Account Email:** The IAM principal to log in as when Authentication Type is `IAM`. The database
username is derived from this email. For a public instance, this is the service account that Data Fusion
impersonates. For a private instance, this is the identity the Cloud SQL Auth Proxy runs as.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Connection Timeout** The timeout value used for socket connect operations. If connecting to the server takes longer
than this value, the connection is broken.The timeout is specified in seconds and a value of zero means that it is 
disabled.


Examples
--------
**Connecting to a public CloudSQL PostgreSQL instance**

Suppose you want to execute a query against a CloudSQL PostgreSQL database named "prod", as "postgres" user with "postgres" 
password (Get the latest version of the CloudSQL socket factory jar with driver and dependencies 
[here](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/releases)), then configure plugin with:


```
Driver Name: "cloudsql-postgresql"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Connection Name: [PROJECT_ID]:[REGION]:[INSTANCE_NAME]
CloudSQL Instance Type: "Public"
Database: "prod"
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
--metadata=startup-script="docker run -d -p 0.0.0.0:5432:5432
gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.14.1 --address 0.0.0.0 --port 5432
--private-ip ${POSTGRESQL_CONN}" --maintenance-policy=MIGRATE
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
Driver Name: "cloudsql-postgresql"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Connection Name: <proxy-ip> (obtained from commands above)
CloudSQL Instance Type: "Private"
Database: "prod"
Username: "postgres"
Password: "postgres"
```
