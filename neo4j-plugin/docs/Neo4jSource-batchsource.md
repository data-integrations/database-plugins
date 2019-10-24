# Neo4j Batch Source


Description
-----------
Reads from a Neo4j instance using a configurable CQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from a Neo4j instance.


Properties
----------
**Reference Name:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**Neo4j Host:** Neo4j database host.

**Neo4j Port:** Neo4j database port.

**Input Query:** The query to use to import data from the Neo4j database. 
Query example: 'MATCH (n:Label) RETURN n.property_1, n.property_2'.

**Username:** User to use to connect to the Neo4j database.

**Password:** Password to use to connect to the Neo4j database.

**Splits Number:** The number of splits to generate. If set to one, the orderBy is not needed.

**Order By:** Field Name which will be used for ordering during splits generation. This is required unless numSplits 
is set to one and 'ORDER BY' keyword not exist in Input Query.


Data Types Mapping
----------

    | Neo4j Data Types                | CDAP Schema Data Types | Comment                                            |
    | ------------------------------- | ---------------------- | -------------------------------------------------- |
    | null                            | null                   |                                                    |
    | List                            | array                  |                                                    |
    | Map                             | record                 |                                                    |
    | Boolean                         | boolean                |                                                    |
    | Integer                         | long                   |                                                    |
    | Float                           | double                 |                                                    |
    | String                          | string                 |                                                    |
    | ByteArray                       | bytes                  |                                                    |
    | Date                            | date                   |                                                    |
    | Time                            | time-micros            |                                                    |
    | LocalTime                       | time-micros            |                                                    |
    | DateTime                        | timestamp-micros       |                                                    |
    | LocalDateTime                   | timestamp-micros       |                                                    |
    | Node                            | record                 |                                                    |
    | Relationship                    | record                 |                                                    |
    | Duration                        | record                 |                                                    |
    | Point                           | record                 |                                                    |
    | Path                            |                        |                                                    |
   