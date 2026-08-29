Feature: BigQuery to CloudSqlMySql and GCS to CloudSqlMySql Runtime

  @CLDMYSQL @TC-Runtime-BigQuery-to-CLOUDSqlMYSQL:
  Scenario: TC-CLDMYSQL-RNTM-01:Verify user is able to transferred data from BigQuery to CloudSqlMysql with mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Source is BigQuery
    When Target is CloudSQLMySQL
    Then Link BigQuery to CloudSQLMySQL to establish connection
    Then Enter the Source BigQuery Properties for table "clsTableNameBQ"
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the BigQuery Properties
    Then Enter the sink CloudSQLMySQL Properties for table "clsTableNameBQCS"
    Then Click on Validate button
    Then Verify the Connector status
    Then Close the CloudSQLMySQL Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for CloudSQL MySQL
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Pre records count from CloudSQLMySQL table "clsTableNameBQCS"
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture Logs
    Then Verify the pipeline status is "Succeeded"
    Then Post records count from CloudSQLMySQL table "clsTableNameBQCS"
    Then Validate the output record count
    Then Validate successMessage is displayed
    Then Validate the count of records transferred from BigQuery "clsTableNameBQ" to CloudSqlMySql "clsTableNameBQCS"

  @CLDMYSQL @TC-Runtime-BigQuery-to-CLOUDSqlMYSQL:
  Scenario: TC-CLDMYSQL-RNTM-02:Verify user is able to transferred data from BigQuery to CloudSqlMysql with filter
    Given Open DataFusion Project to configure pipeline
    When Source is BigQuery
    When Target is CloudSQLMySQL
    Then Link BigQuery to CloudSQLMySQL to establish connection
    Then Enter the Source BigQuery Properties for table "clsTableNameBQ1"
    Then Enter the Source BigQuery with filter "clsFilterBigQuery" option
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the BigQuery Properties
    Then Enter the sink CloudSQLMySQL Properties for table "clsTableNameBQCS1"
    Then Click on Validate button
    Then Verify the Connector status
    Then Close the CloudSQLMySQL Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for CloudSQL MySQL
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Pre records count from CloudSQLMySQL table "clsTableNameBQCS1"
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture Logs
    Then Verify the pipeline status is "Succeeded"
    Then Post records count from CloudSQLMySQL table "clsTableNameBQCS1"
    Then Validate the output record count
    Then Validate successMessage is displayed
    Then Validate the count of records transferred from BigQuery "clsTableNameBQ1" to CloudSqlMySql with filter "clsFilterBigQuery"

  @CLDMYSQL @TC-Runtime-GCS-to-CLOUDSQLMYSQL:
  Scenario: TC-CLDMYSQL-RNTM-03:Verify user is able to transferred data from GCS to CloudSQLMySQL with mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Source is GCS bucket
    When Target is CloudSQLMySQL
    Then Link GCS to CloudSQLMySQL to establish connection
    Then Enter the GCS Properties with "clsBucket" GCS bucket and format "clsFormatType"
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the GCS Properties
    Then Enter the sink CloudSQLMySQL Properties for table "clsTableNameGCSCS"
    Then Click on Validate button
    Then Verify the Connector status
    Then Close the CloudSQLMySQL Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for CloudSQL MySQL
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture Logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Validate successMessage is displayed

  @CLDMYSQL @TC-Runtime-GCS-to-CLOUDSQLMYSQL:
  Scenario: TC-CLDMYSQL-RNTM-04:Verify user is able to Duplicate the pipeline
    Given Open DataFusion Project to configure pipeline
    When Source is GCS bucket
    When Target is CloudSQLMySQL
    Then Link GCS to CloudSQLMySQL to establish connection
    Then Enter the GCS Properties with "clsBucket" GCS bucket and format "clsFormatType"
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the GCS Properties
    Then Enter the sink CloudSQLMySQL Properties for table "clsTableNameGCSCS"
    Then Click on Validate button
    Then Verify the Connector status
    Then Close the CloudSQLMySQL Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for CloudSQL MySQL
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Create Duplicate pipeline
    Then Validate studio is opened with duplicate pipeline
