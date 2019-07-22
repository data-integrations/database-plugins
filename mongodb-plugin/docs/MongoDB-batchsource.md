# MongoDB Batch Source


Description
-----------
Reads documents from a MongoDB collection and converts each document into a StructuredRecord with the help
of a specified schema. The user can optionally provide input query, input fields, and splitter classes.


Configuration
-------------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Host:** Host that MongoDB is running on.

**Port:** Port that MongoDB is listening to.

**Database:** MongoDB database name.

**Collection:** Name of the database collection to read from.

**Output Schema:** Specifies the schema of the documents.

**Input Query:** Optionally filter the input collection with a query. This query must be represented in JSON format
and use the [MongoDB extended JSON format] to represent non-native JSON data types. (Macro-enabled)

**Input Fields:** [Projection document] that can limit the fields that appear in each document. This must be
represented in JSON format, and use the [MongoDB extended JSON format] to represent non-native JSON data types. If no
projection document is provided, all fields will be read. (Macro-enabled)

**Splitter Class:** The name of the Splitter class to use. If left empty, the MongoDB Hadoop Connector will attempt
to make a best-guess as to which Splitter to use. (Macro-enabled)

The Hadoop connector provides these Splitters:

  - `com.mongodb.hadoop.splitter.StandaloneMongoSplitter`
  - `com.mongodb.hadoop.splitter.ShardMongoSplitter`
  - `com.mongodb.hadoop.splitter.ShardChunkMongoSplitter`
  - `com.mongodb.hadoop.splitter.MultiMongoCollectionSplitter`

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Authentication Connection String:** Auxiliary MongoDB connection string to authenticate against when constructing
splits. (Macro-enabled)

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. See
[Connection String Options] for a full description of these arguments.

[MongoDB extended JSON format]:
http://docs.mongodb.org/manual/reference/mongodb-extended-json/

[Projection document]:
http://docs.mongodb.org/manual/reference/method/db.collection.find/#projections

[Connection String Options]:
https://docs.mongodb.com/manual/reference/connection-string/#connections-connection-options
