# MongoDB Batch Sink


Description
-----------
Converts a StructuredRecord into a BSONWritable and then writes it to a MongoDB collection.


Configuration
-------------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Host:** Host that MongoDB is running on.

**Port:** Port that MongoDB is listening to.

**Database:** MongoDB database name.

**Collection:** Name of the database collection to write to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. See
[Connection String Options] for a full description of these arguments.

[Connection String Options]:
https://docs.mongodb.com/manual/reference/connection-string/#connections-connection-options
