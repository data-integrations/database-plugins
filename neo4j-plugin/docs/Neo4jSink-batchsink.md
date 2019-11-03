# Neo4j Batch Sink


Description
-----------
Write data to Neo4j instance using a configurable CQL query.
Count of created Node and Relation depend on Output Query.


Use Case
--------
The sink is used whenever you need to write to a Neo4j instance.


Properties
----------
**Reference Name:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**Neo4j Host:** Neo4j database host.

**Neo4j Port:** Neo4j database port.

**Output Query:** The query to use to export data to the Neo4j database. Query example: 'CREATE (n:<label_field> $(*))' 
or 'CREATE (n:<label_field> $(property_1, property_2))'. Addition information can be found on 
https://wiki.cask.co/display/CE/Neo4j+database+plugin

**Username:** User to use to connect to the Neo4j database.

**Password:** Password to use to connect to the Neo4j database.


Data Types Mapping
----------

    | CDAP Schema Data Types | Neo4j Data Types                      | Comment                                      |
    | ---------------------- | ------------------------------------- | -------------------------------------------- |
    | null                   | null                                  |                                              |
    | array                  | List                                  |                                              |
    | boolean                | Boolean                               |                                              |
    | long, int              | Integer                               |                                              |
    | double                 | Float                                 |                                              |
    | string                 | String                                |                                              |
    | bytes                  | ByteArray                             |                                              |
    | date                   | Date                                  |                                              |
    | time-micros            | Time, LocalTime                       |                                              |
    | timestamp-micros       | DateTime, LocalDateTime               |                                              |
    | record                 | Duration, Point                       | Depending on record fields                   |


   