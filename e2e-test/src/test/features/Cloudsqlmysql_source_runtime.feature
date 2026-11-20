Feature: CloudSqlMySql to BigQuery and CloudSqlMySql to GCS Run-Time

  @CLDMYSQL @TC-Runtime-CLOUDSQLMYSQL-to-BigQuery:
  Scenario Outline:Verify data transferred from CloudSql-Mysql to BigQuery with mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    When Target is BigQuery
    Then Link CloudSQLMySQL to BigQuery to establish connection
    Then Enter the source CloudSQLMySQL Properties with import query "<importQuery>"
    Then Click on Validate button
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the CloudSQLMySQL Properties
    Then Enter the BigQuery Properties for table "clsMySQLBQTableName"
    Then Verify the Connector status
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for BigQuery
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture Logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Validate successMessage is displayed
    Then Validate count of records transferred from importQuery "<importQuery>" to BigQuery in "clsMySQLBQTableName"
    Then Delete the table "clsMySQLBQTableName"
    Examples:
      | importQuery      |
      | clsImportQuery   |
      | clsImportQuery2  |

  @CLDMYSQL @TC-Runtime-CLOUDSQLMYSQL-to-BigQuery:
  Scenario:Verify user is able to transferred data from CloudSql-Mysql to BigQuery with Advanced
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    When Target is BigQuery
    Then Link CloudSQLMySQL to BigQuery to establish connection
    Then Enter the source CloudSQLMySQL properties with import query "clsImportQuery1" bounding query "clsBoundingQuery"
    Then Enter the source CloudSQL-MySQL with Split and Number of splits CloudSQLMySQL properties
    Then Click on Validate button
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the CloudSQLMySQL Properties
    Then Enter the BigQuery Properties for table "clsMySQLBQTableName"
    Then Verify the Connector status
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for BigQuery
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture Logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Validate successMessage is displayed
    Then Get Count of no of records transferred to BigQuery in "clsMySQLBQTableName"
    Then Delete the table "clsMySQLBQTableName"

  @CLDMYSQL @TC-Runtime-CLOUDSQLMYSQL-to-GCS:
  Scenario Outline:Verify user is able to transferred data from CloudSql-Mysql to GCS with mandatory fields
    Given Open DataFusion Project to configure pipeline
    Given Cloud Storage bucket should not exist in "projectId" with the name "clsFileBucketCreate"
    When Source is CloudSQLMySQL
    When Target is GCS
    Then Link CloudSQLMySQL to GCS to establish connection
    Then Enter the source CloudSQLMySQL Properties with import query "clsImportQuery"
    Then Click on Validate button
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the CloudSQLMySQL Properties
    Then Enter the GCS Properties and "<format>" file format
    Then Verify the Connector status
    Then Close the GCS Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Verify the folder created in "projectId" with bucket name "clsFileBucketCreate"
    Then Open and capture Logs
    Then Validate successMessage is displayed
    Examples:
      | format    |
      | avro      |
      | csv       |
      | parquet   |
      | tsv       |
      | delimited |
      | json      |
      | orc       |

  @CLDMYSQL @TC-Runtime-CLOUDSQLMYSQL-to-GCS:
  Scenario:Verify user is able to Duplicate the pipeline
    Given Open DataFusion Project to configure pipeline
    Given Cloud Storage bucket should not exist in "projectId" with the name "clsFileBucketCreate"
    When Source is CloudSQLMySQL
    When Target is GCS
    Then Link CloudSQLMySQL to GCS to establish connection
    Then Enter the source CloudSQLMySQL Properties with import query "clsImportQuery"
    Then Click on Validate button
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the CloudSQLMySQL Properties
    Then Enter the GCS Properties and "csv" file format
    Then Verify the Connector status
    Then Close the GCS Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Verify the folder created in "projectId" with bucket name "clsFileBucketCreate"
    Then Create Duplicate pipeline
    Then Validate studio is opened with duplicate pipeline

  @CLDMYSQL @TC-Runtime-CLOUDSQLMYSQL-to-BigQuery:
  Scenario:Verify preview gets failed when incorrect values in Split column CloudSql-Mysql
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    When Target is BigQuery
    Then Link CloudSQLMySQL to BigQuery to establish connection
    Then Enter the source CloudSQLMySQL properties with import query "clsImportQuery1" bounding query "clsBoundingQuery"
    Then Enter the incorrect values in split column with number of splits
    Then Click on Validate button
    Then Verify the Connector status
    Then Capture output schema
    Then Close the CloudSQLMySQL Properties
    Then Enter the BigQuery Properties for table "clsMySQLBQTableName"
    Then Verify the Connector status
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"
