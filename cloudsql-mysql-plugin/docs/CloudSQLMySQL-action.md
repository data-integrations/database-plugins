# CloudSQL MySQL Action


Description
-----------
Action that runs a MySQL command on a CloudSQL MySQL instance.


Use Case
--------
The action can be used whenever you want to run a MySQL command before or after a data pipeline.
For example, you may want to run a SQL update command on a database before the pipeline source pulls data from tables.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Database Command:** Database command to execute.

**Database:** MySQL database name.

**Connection Name:** The CloudSQL instance to connect to in the format <PROJECT_ID>:\<REGION>:<INSTANCE_NAME>. 
Can be found in the instance overview page.

**CloudSQL Instance Type:** Whether the CloudSQL instance to connect to is private or public. Defaults to 'Public'.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Timeout:** The timeout value (in seconds) used for socket connect operations. If connecting to the server 
takes longer than this value, the connection is broken. A value of 0 means that it is disabled.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.



Examples
--------
**Connecting to a public CloudSQL MySQL instance**

Suppose you want to execute a query against a CloudSQL MySQL database named "prod", as "root" user with "root" password 
(Get the latest version of the CloudSQL socket factory jar with driver and dependencies 
[here](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/releases), then configure plugin with: 

```
Driver Name: "cloudsql-mysql"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Instance Name: [PROJECT_ID]:[REGION]:[INSTANCE_NAME]
CloudSQL Instance Type: "Public"
Database: "prod"
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
```

Get the latest version of the CloudSQL socket factory jar with driver and dependencies from
[here](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/releases), then configure plugin with:

```
Driver Name: "cloudsql-mysql"
Database Command: "UPDATE table_name SET price = 20 WHERE ID = 6"
Instance Name: [PROJECT_ID]:[REGION]:[INSTANCE_NAME]
CloudSQL Instance Type: "Private"
Database: "prod"
Username: "root"
Password: "root"
``` 
