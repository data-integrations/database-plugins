# CloudSQLPostgreSQL Connection


Description
-----------
Use this connection to access data in a CloudSQLPostgreSQL database using JDBC.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**JDBC Driver name:** Select the JDBC driver to use.

**CloudSQL Instance Type:** Whether the CloudSQL instance to connect to is private or public. Defaults to 'Public'.

**Connection Name:** The CloudSQL instance to connect to in the format <PROJECT_ID>:\<REGION>:<INSTANCE_NAME>.
Can be found in the instance overview page.

**Port:** Port that PostgreSQL is running on.

**Database:** CloudSQL PostgreSQL database name.

**Username:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**Password:** Password to use to connect to the specified database.

**Authentication Type:** How to authenticate to the database. `Basic` uses the Username and Password.
`IAM` uses Cloud SQL IAM database authentication as the principal in Service Account Email, and the Username
and Password are not used. See **IAM Database Authentication** below for the setup steps.

**Service Account Email:** The IAM principal to log in as when Authentication Type is `IAM`. The database
username is derived from this email. For a public instance, this is the service account that Data Fusion
impersonates. For a private instance, this is the identity the Cloud SQL Auth Proxy runs as.

**Transaction Isolation Level:** The transaction isolation level of the database connection.
- TRANSACTION_READ_COMMITTED: No dirty reads. Non-repeatable reads and phantom reads are possible.
- TRANSACTION_SERIALIZABLE: No dirty reads. Non-repeatable and phantom reads are prevented.
- TRANSACTION_REPEATABLE_READ: No dirty reads. Prevents non-repeatable reads, but phantom reads are still possible.
- Note: PostgreSQL does not implement `TRANSACTION_READ_UNCOMMITTED` as a distinct isolation level. Instead, this mode behaves identically to`TRANSACTION_READ_COMMITTED`, which is why it is not exposed as a separate option.

For more details on the Transaction Isolation Levels supported in CloudSQL PostgreSQL, refer to the [CloudSQL PostgreSQL documentation](https://www.postgresql.org/docs/current/transaction-iso.html)

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.
This is a semicolon-separated list of key-value pairs, where each pair is separated by a equals '=' and specifies
the key and value for the argument. For example, 'key1=value1;key2=value' specifies that the connection will be
given arguments 'key1' mapped to 'value1' and the argument 'key2' mapped to 'value2'.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body. It can be in the following form :

1. `/{schema}/{table}`
   This path indicates a table. A table is the only one that can be sampled. Browse on this path to return the specified table.

2. `/{schema}`
   This path indicates a schema. A schema cannot be sampled. Browse on this path to get all the tables under this schema.

3. `/`
   This path indicates the root. A root cannot be sampled. Browse on this path to get all the schemas visible through this connection.

IAM Database Authentication
---------------------------
Set **Authentication Type** to `IAM` to log in with Cloud SQL IAM database authentication instead of a database
password. The database username is derived from **Service Account Email**: for CloudSQL PostgreSQL it is
the email without the `.gserviceaccount.com` suffix. For example, `my-sa@my-project.iam.gserviceaccount.com`
logs in as `my-sa@my-project.iam`.

For both instance types:
1. Enable IAM database authentication on the Cloud SQL instance and add the service account as an IAM database user.
2. Grant the service account the `Cloud SQL Client` and `Cloud SQL Instance User` roles, and grant it privileges in
   the database. A new IAM database user has no privileges.

**Public instance:** Data Fusion impersonates the service account, so the same database user is used at design time
and at runtime. In addition to the steps above:
- Grant the Data Fusion service agent (`service-<project-number>@gcp-sa-datafusion.iam.gserviceaccount.com`) and
  the Dataproc service account used to run pipelines the `Service Account Token Creator` role on the service account.
- Use a JDBC driver built from Cloud SQL socket factory version 1.14.0 or later.

**Private instance:** the Cloud SQL Auth Proxy performs the IAM login. Start the proxy (version 2) with the
`--auto-iam-authn` flag, running as the service account in Service Account Email (for example, the VM's service
account, or `--impersonate-service-account`).

Examples
--------
**Connecting to a public CloudSQL PostgreSQL instance**

Suppose you want to read data from CloudSQL PostgreSQL database named "prod", as "postgres" user with "postgres"
password (Get the latest version of the CloudSQL socket factory jar with driver and dependencies
[here](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/releases)), then configure plugin with:


```
Name: "connection1"
Driver Name: "cloudsql-postgresql"
Database: "prod"
Connection Name: [PROJECT_ID]:[REGION]:[INSTANCE_NAME]
CloudSQL Instance Type: "Public"
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
Name: "connection1"
Driver Name: "cloudsql-postgresql"
Database: "prod"
Connection Name: <proxy-ip> (obtained from commands above)
CloudSQL Instance Type: "Private"
Username: "postgres"
Password: "postgres"
```