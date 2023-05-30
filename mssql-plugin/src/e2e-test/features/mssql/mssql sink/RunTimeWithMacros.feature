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

@Mysql
Feature: Mssql Sink - Run time scenarios (macro)

  @BQ_SOURCE_TEST @MSSQL_TEST_TABLE @Mssql_Required
  Scenario: Verify User is able to preview and deploy the pipeline when the data is transferred from BigQuery to Mssql using macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "SQL Server" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "SQL Server" to establish connection
    Then Navigate to the properties page of plugin: "SQL Server"
    And Click on the Macro button of Property: "jdbcPluginName" and set the value to: "DriverName"
    And Click on the Macro button of Property: "host" and set the value to: "Host"
    And Click on the Macro button of Property: "port" and set the value to: "Port"
    And Click on the Macro button of Property: "user" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "SQL Server" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "driverName" for key "DriverName"
    Then Enter runtime argument value from environment variable "host" for key "Host"
    Then Enter runtime argument value from environment variable "port" for key "Port"
    Then Enter runtime argument value from environment variable "username" for key "Username"
    Then Enter runtime argument value from environment variable "password" for key "Password"
    And Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    And Enter runtime argument value "driverName" for key "DriverName"
    And Enter runtime argument value from environment variable "host" for key "Host"
    And Enter runtime argument value from environment variable "port" for key "Port"
    And Enter runtime argument value from environment variable "username" for key "Username"
    And Enter runtime argument value from environment variable "password" for key "Password"
    And Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to target MsSql table is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @MSSQL_TEST_TABLE @Mssql_Required
  Scenario: Verify User is able to preview and deploy the pipeline when the data is transferred from BigQuery to Mssql using basic section as macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "SQL Server" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "SQL Server" to establish connection
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    And Click on the Macro button of Property: "database" and set the value to: "DatabaseName"
    And Click on the Macro button of Property: "tableName" and set the value to: "TableName"
    And Click on the Macro button of Property: "dbSchemaName" and set the value to: "SchemaName"
    Then Validate "SQL Server" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "databaseName" for key "DatabaseName"
    Then Enter runtime argument value "targetTable" for key "TableName"
    Then Enter runtime argument value "schema" for key "SchemaName"
    And Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "databaseName" for key "DatabaseName"
    Then Enter runtime argument value "targetTable" for key "TableName"
    Then Enter runtime argument value "schema" for key "SchemaName"
    And Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to target MsSql table is equal to the values from source BigQuery table

  @MSSQL_SOURCE_TEST @MSSQL_SINK_TEST @Mssql_Required
  Scenario: Verify pipeline failure message in logs when user provides invalid Table Name of plugin with Macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "SQL Server" from the plugins list as: "Sink"
    Then Connect plugins: "SQL Server" and "SQL Server2" to establish connection
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "outputSchema"
    Then Validate "SQL Server" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "SQL Server2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    And Click on the Macro button of Property: "tableName" and set the value to: "TableName"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Validate "SQL Server2" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    And Enter runtime argument value "invalidTableName" for key "TableName"
    And Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                         |
      | ERROR | errorMessageInvalidTableName    |

  @MSSQL_SOURCE_TEST @MSSQL_SINK_TEST @Mssql_Required
  Scenario: Verify pipeline failure message in logs when user provides invalid Credentials for connection with Macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "SQL Server" from the plugins list as: "Sink"
    Then Connect plugins: "SQL Server" and "SQL Server2" to establish connection
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "outputSchema"
    Then Validate "SQL Server" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "SQL Server2"
    And Click on the Macro button of Property: "jdbcPluginName" and set the value to: "DriverName"
    And Click on the Macro button of Property: "host" and set the value to: "Host"
    And Click on the Macro button of Property: "port" and set the value to: "Port"
    And Click on the Macro button of Property: "user" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "SQL Server" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    And Enter runtime argument value "driverName" for key "DriverName"
    And Enter runtime argument value "invalid.Host" for key "Host"
    And Enter runtime argument value "invalid.Port" for key "Port"
    And Enter runtime argument value "invalid.Username" for key "Username"
    And Enter runtime argument value "invalid.Password" for key "Password"
    And Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                         |
      | ERROR | errorMessageInvalidCredentials    |
