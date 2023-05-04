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
Feature: CloudMySql Source - Run time scenarios

Feature: CloudMySql - Verify data transfer from CloudMySql source to BigQuery sink

  @CLOUDMYSQL_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify data is getting transferred from CloudMySql source to BigQuery sink successfully
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL MySQL" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Enter input plugin property: "user" with value: "username"
    Then Enter input plugin property: "password" with value: "password"
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "datatypesSchema"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target Big Query table is equal to the values from source table

  @CLOUDMYSQL_SOURCE_DATATYPES_TEST @BQ_SINK_TEST
  Scenario: To verify data is getting transferred from CloudMySql source to BigQuery sink successfully with all datatypes
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL MySQL" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Validate "BigQuery" plugin properties
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
    Then Validate the values of records transferred to target Big Query table is equal to the values from source table


  @CLOUDMYSQL_SOURCE_DATATYPES_TEST @CLOUDMYSQL_SINK_TEST @PLUGIN-20670
  Scenario: To verify data is getting transferred from CloudMySql source to CloudMySql sink successfully
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL MySQL" and "CloudSQL MySQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "datatypesSchema"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL MySQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Validate "CloudSQL MySQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to target table is equal to the values from source table

  @CLOUDMYSQL_SOURCE_DATATYPES_TEST @CLOUDMYSQL_SINK_TEST @PLUGIN-20670
  Scenario: To verify data is getting transferred from CloudMySql source to CloudMySql successfully when connection arguments are set
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL MySQL" and "CloudSQL MySQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter key value pairs for plugin property: "connectionArguments" with values from json: "connectionArgumentsList"
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL MySQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Validate "CloudSQL MySQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to target table is equal to the values from source table

  @CLOUDMYSQL_SOURCE_DATATYPES_TEST
    Scenario: Verify user should not be able to deploy and run the pipeline when plugin is configured with invalid bounding query
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL MySQL" and "CloudSQL MySQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Enter textarea plugin property: "boundingQuery" with value: "invalidboundQuery"
    Then Enter textarea plugin property: "importQuery" with value: "cloudsqlimportQuery"
    Then Replace input plugin property: "splitBy" with value: "splitby"
    Then Replace input plugin property: "numSplits" with value: "numbersplitsgenerate"
    Then Click on the Get Schema button
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL MySQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Validate "CloudSQL MySQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                  |
      | ERROR | errorLogsMessageInvalidBoundingQuery     |
