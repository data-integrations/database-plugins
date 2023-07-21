#
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
#

@PostgreSQL_Source
Feature: PostgreSQL - Verify PostgreSQL plugin data transfer with macro arguments

  @POSTGRESQL_SOURCE_TEST @Postgresql_Required @POSTGRESQL_SINK_TEST
  Scenario: To verify data is getting transferred from PostgreSQL to PostgreSQL successfully using macro arguments in connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "PostgreSQL" and "PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "PostgreSQLDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "PostgreSQLHost"
    Then Click on the Macro button of Property: "port" and set the value to: "PostgreSQLPort"
    Then Click on the Macro button of Property: "user" and set the value to: "PostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "PostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "PostgreSQL2"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "PostgreSQLDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "PostgreSQLHost"
    Then Click on the Macro button of Property: "port" and set the value to: "PostgreSQLPort"
    Then Click on the Macro button of Property: "user" and set the value to: "PostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "PostgreSQLPassword"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "driverName" for key "PostgreSQLDriverName"
    Then Enter runtime argument value from environment variable "host" for key "PostgreSQLHost"
    Then Enter runtime argument value from environment variable "port" for key "PostgreSQLPort"
    Then Enter runtime argument value from environment variable "username" for key "PostgreSQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "PostgreSQLPassword"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "driverName" for key "PostgreSQLDriverName"
    Then Enter runtime argument value from environment variable "host" for key "PostgreSQLHost"
    Then Enter runtime argument value from environment variable "port" for key "PostgreSQLPort"
    Then Enter runtime argument value from environment variable "username" for key "PostgreSQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "PostgreSQLPassword"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target table is equal to the values from source table

  @POSTGRESQL_SOURCE_TEST @Postgresql_Required @POSTGRESQL_SINK_TEST
  Scenario: To verify data is getting transferred from PostgreSQL to PostgreSQL successfully using macro arguments in basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "PostgreSQL" and "PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "splitBy" and set the value to: "PostgreSQLSplitByColumn"
    Then Click on the Macro button of Property: "importQuery" and set the value in textarea: "PostgreSQLImportQuery"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "PostgreSQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "PostgreSQLTableName"
    Then Click on the Macro button of Property: "dbSchemaName" and set the value to: "PostgreSQLSchemaName"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Validate "PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "splitByColumn" for key "PostgreSQLSplitByColumn"
    Then Enter runtime argument value "selectQuery" for key "PostgreSQLImportQuery"
    Then Enter runtime argument value "targetTable" for key "PostgreSQLTableName"
    Then Enter runtime argument value "schema" for key "PostgreSQLSchemaName"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "splitByColumn" for key "PostgreSQLSplitByColumn"
    Then Enter runtime argument value "selectQuery" for key "PostgreSQLImportQuery"
    Then Enter runtime argument value "targetTable" for key "PostgreSQLTableName"
    Then Enter runtime argument value "schema" for key "PostgreSQLSchemaName"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target table is equal to the values from source table

  @POSTGRESQL_SOURCE_TEST @Postgresql_Required @POSTGRESQL_SINK_TEST
  Scenario: To verify pipeline preview fails when invalid connection details provided using macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "PostgreSQL" and "PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "PostgreSQLDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "PostgreSQLHost"
    Then Click on the Macro button of Property: "port" and set the value to: "PostgreSQLPort"
    Then Click on the Macro button of Property: "user" and set the value to: "PostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "PostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "PostgreSQL2"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "PostgreSQLDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "PostgreSQLHost"
    Then Click on the Macro button of Property: "port" and set the value to: "PostgreSQLPort"
    Then Click on the Macro button of Property: "user" and set the value to: "PostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "PostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "invalidDriverName" for key "PostgreSQLDriverName"
    Then Enter runtime argument value "invalidHost" for key "PostgreSQLHost"
    Then Enter runtime argument value "invalidPort" for key "PostgreSQLPort"
    Then Enter runtime argument value "invalidUserName" for key "PostgreSQLUsername"
    Then Enter runtime argument value "invalidPassword" for key "PostgreSQLPassword"
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "Failed"

  @POSTGRESQL_SOURCE_TEST @Postgresql_Required @POSTGRESQL_SINK_TEST
  Scenario: To verify pipeline preview fails when invalid basic details provided using macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "PostgreSQL" and "PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "importQuery" and set the value in textarea: "PostgreSQLInvalidImportQuery"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "PostgreSQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "PostgreSQLTableName"
    Then Click on the Macro button of Property: "dbSchemaName" and set the value to: "PostgreSQLSchemaName"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Validate "PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "invalidTableNameImportQuery" for key "PostgreSQLInvalidImportQuery"
    Then Enter runtime argument value "invalidTable" for key "PostgreSQLTableName"
    Then Enter runtime argument value "schema" for key "PostgreSQLSchemaName"
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "Failed"

  @POSTGRESQL_SOURCE_TEST @POSTGRESQL_SINK_TEST @BQ_SINK_TEST @Plugin-1526
  Scenario: To verify data is getting transferred from PostgreSQL source to BigQuery sink using macro arguments in connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "PostgreSQL" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "PostgreSQLDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "PostgreSQLHost"
    Then Click on the Macro button of Property: "port" and set the value to: "PostgreSQLPort"
    Then Click on the Macro button of Property: "user" and set the value to: "PostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "PostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqDataset"
    Then Click on the Macro button of Property: "table" and set the value to: "bqTable"
    Then Click on the Macro button of Property: "truncateTableMacroInput" and set the value to: "bqTruncateTable"
    Then Click on the Macro button of Property: "updateTableSchemaMacroInput" and set the value to: "bqUpdateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "driverName" for key "PostgreSQLDriverName"
    Then Enter runtime argument value from environment variable "host" for key "PostgreSQLHost"
    Then Enter runtime argument value from environment variable "port" for key "PostgreSQLPort"
    Then Enter runtime argument value from environment variable "username" for key "PostgreSQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "PostgreSQLPassword"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqTargetTable" for key "bqTable"
    Then Enter runtime argument value "bqTruncateTable" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchema" for key "bqUpdateTableSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "driverName" for key "PostgreSQLDriverName"
    Then Enter runtime argument value from environment variable "host" for key "PostgreSQLHost"
    Then Enter runtime argument value from environment variable "port" for key "PostgreSQLPort"
    Then Enter runtime argument value from environment variable "username" for key "PostgreSQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "PostgreSQLPassword"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqTargetTable" for key "bqTable"
    Then Enter runtime argument value "bqTruncateTable" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchema" for key "bqUpdateTableSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target BigQuery table is equal to the values from source table

  @POSTGRESQL_SOURCE_TEST @Postgresql_Required @POSTGRESQL_SINK_TEST @BQ_SINK_TEST @Plugin-1526
  Scenario: To verify data is getting transferred from PostgreSQL source to BigQuery sink using macro arguments in basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "PostgreSQL" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "splitBy" and set the value to: "PostgreSQLSplitByColumn"
    Then Click on the Macro button of Property: "importQuery" and set the value in textarea: "PostgreSQLImportQuery"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqDataset"
    Then Click on the Macro button of Property: "table" and set the value to: "bqTable"
    Then Click on the Macro button of Property: "truncateTableMacroInput" and set the value to: "bqTruncateTable"
    Then Click on the Macro button of Property: "updateTableSchemaMacroInput" and set the value to: "bqUpdateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "splitByColumn" for key "PostgreSQLSplitByColumn"
    Then Enter runtime argument value "selectQuery" for key "PostgreSQLImportQuery"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqTargetTable" for key "bqTable"
    Then Enter runtime argument value "bqTruncateTable" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchema" for key "bqUpdateTableSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "splitByColumn" for key "PostgreSQLSplitByColumn"
    Then Enter runtime argument value "selectQuery" for key "PostgreSQLImportQuery"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqTargetTable" for key "bqTable"
    Then Enter runtime argument value "bqTruncateTable" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchema" for key "bqUpdateTableSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target BigQuery table is equal to the values from source table
