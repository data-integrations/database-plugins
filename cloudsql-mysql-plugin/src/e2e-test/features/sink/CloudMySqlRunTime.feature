# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@CloudMySql
Feature: CloudMySql Sink - Run time scenarios

  @BQ_SOURCE_TEST @CLOUDMYSQL_TEST_TABLE
  Scenario: To verify data is getting transferred from BigQuery source to CloudMySql sink successfully
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqOutputMultipleDatatypesSchema"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL MySQL" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target CloudSQLMySql table is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @CLOUDMYSQL_TEST_TABLE
  Scenario: To verify data is getting transferred from BigQuery source to CloudMySql sink successfully when connection arguments are set
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqOutputMultipleDatatypesSchema"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL MySQL" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter key value pairs for plugin property: "connectionArguments" with values from json: "connectionArgumentsList"
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target CloudSQLMySql table is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @CLOUDMYSQL_TEST_TABLE
  Scenario: To verify data is getting transferred from BigQuery source to CloudSMySQL sink with Advanced property Connection timeout
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqOutputMultipleDatatypesSchema"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL MySQL" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "connectionTimeout" with value: "connectionTimeout"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target CloudSQLMySql table is equal to the values from source BigQuery table



