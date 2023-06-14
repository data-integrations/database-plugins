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

@CloudMySql @CloudMySql_Required
Feature: CloudMySql Sink - Run time scenarios (macro)

  @BQ_SOURCE_TEST @CLOUDMYSQL_TEST_TABLE
  Scenario: To verify data is getting transferred from BigQuery source to CloudMySql sink using macro arguments in connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL MySQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "cloudSQLMySQLDriverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "user" and set the value to: "cloudSQLMySQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "cloudSQLMySQLPassword"
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "driverName" for key "cloudSQLMySQLDriverName"
    Then Enter runtime argument value from environment variable "username" for key "cloudSQLMySQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "cloudSQLMySQLPassword"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "driverName" for key "cloudSQLMySQLDriverName"
    Then Enter runtime argument value from environment variable "username" for key "cloudSQLMySQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "cloudSQLMySQLPassword"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target CloudSQLMySql table is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @CLOUDMYSQL_TEST_TABLE @PLUGIN-1633
  Scenario: To verify data is getting transferred from BigQuery source to CloudMySql sink using macro arguments in basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL MySQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "Tablename"
    Then Validate "CloudSQL MySQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "targetTable" for key "Tablename"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "targetTable" for key "Tablename"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target CloudSQLMySql table is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @CLOUDMYSQL_TEST_TABLE
  Scenario: Verify pipeline failure message in logs when user provides invalid Table Name of plugin with Macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL MySQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "invalidTablename"
    Then Validate "CloudSQL MySQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "invalidTablename" for key "invalidTablename"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                  |
      | ERROR | errorLogsMessageInvalidTableName         |

  @BQ_SOURCE_TEST @CLOUDMYSQL_TEST_TABLE
  Scenario: Verify pipeline failure message in logs when user provides invalid credentials of plugin with Macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL MySQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "user" and set the value to: "Username"
    Then Click on the Macro button of Property: "password" and set the value to: "Password"
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "invalidUserName" for key "Username"
    Then Enter runtime argument value "invalidPassword" for key "Password"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                  |
      | ERROR | errorLogsMessageInvalidCredentials       |

