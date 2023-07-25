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

@Regression @Source_Required
Feature: CloudSQL-PostGreSQL source - Verify CloudSQL-PostGreSQL plugin data transfer with macro arguments

  @CLOUDSQLPOSTGRESQL_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TARGET_TEST
  Scenario: To verify data is getting transferred from CloudSQLPostgreSQL to CloudSQLPostgreSQL successfully using macro arguments in connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL PostgreSQL" and "CloudSQL PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "cloudSQLPostgreSQLDriverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "user" and set the value to: "cloudSQLPostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "cloudSQLPostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL2"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "cloudSQLPostgreSQLDriverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "user" and set the value to: "cloudSQLPostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "cloudSQLPostgreSQLPassword"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "CloudSQL PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "driverName" for key "cloudSQLPostgreSQLDriverName"
    Then Enter runtime argument value from environment variable "username" for key "cloudSQLPostgreSQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "cloudSQLPostgreSQLPassword"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "driverName" for key "cloudSQLPostgreSQLDriverName"
    Then Enter runtime argument value from environment variable "username" for key "cloudSQLPostgreSQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "cloudSQLPostgreSQLPassword"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target table is equal to the values from source table

  @CLOUDSQLPOSTGRESQL_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TARGET_TEST
  Scenario: To verify data is getting transferred from CloudSQLPostgreSQL to CloudSQLPostgreSQL successfully using macro arguments in basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL PostgreSQL" and "CloudSQL PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "splitBy" and set the value to: "cloudSQLPostgreSQLSplitByColumn"
    Then Click on the Macro button of Property: "importQuery" and set the value in textarea: "cloudSQLPostgreSQLImportQuery"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "cloudSQLPostgreSQLTableName"
    Then Click on the Macro button of Property: "dbSchemaName" and set the value to: "cloudSQLPostgreSQLSchemaName"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Validate "CloudSQL PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "splitByColumn" for key "cloudSQLPostgreSQLSplitByColumn"
    Then Enter runtime argument value "selectQuery" for key "cloudSQLPostgreSQLImportQuery"
    Then Enter runtime argument value "targetTable" for key "cloudSQLPostgreSQLTableName"
    Then Enter runtime argument value "schema" for key "cloudSQLPostgreSQLSchemaName"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "splitByColumn" for key "cloudSQLPostgreSQLSplitByColumn"
    Then Enter runtime argument value "selectQuery" for key "cloudSQLPostgreSQLImportQuery"
    Then Enter runtime argument value "targetTable" for key "cloudSQLPostgreSQLTableName"
    Then Enter runtime argument value "schema" for key "cloudSQLPostgreSQLSchemaName"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target table is equal to the values from source table

  @CLOUDSQLPOSTGRESQL_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TARGET_TEST
  Scenario: To verify pipeline preview fails when invalid connection details provided using macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL PostgreSQL" and "CloudSQL PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "cloudSQLPostgreSQLDriverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "user" and set the value to: "cloudSQLPostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "cloudSQLPostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL2"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "cloudSQLPostgreSQLDriverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "user" and set the value to: "cloudSQLPostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "cloudSQLPostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "CloudSQL PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "invalidDriverName" for key "cloudSQLPostgreSQLDriverName"
    Then Enter runtime argument value "invalidUserName" for key "cloudSQLPostgreSQLUsername"
    Then Enter runtime argument value "invalidPassword" for key "cloudSQLPostgreSQLPassword"
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "Failed"

  @CLOUDSQLPOSTGRESQL_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TARGET_TEST
  Scenario: To verify pipeline preview fails when invalid basic details provided using macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL PostgreSQL" and "CloudSQL PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "importQuery" and set the value in textarea: "cloudSQLPostgreSQLInvalidImportQuery"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "cloudSQLPostgreSQLTableName"
    Then Click on the Macro button of Property: "dbSchemaName" and set the value to: "cloudSQLPostgreSQLSchemaName"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Validate "CloudSQL PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "invalidTableNameImportQuery" for key "cloudSQLPostgreSQLInvalidImportQuery"
    Then Enter runtime argument value "invalidTable" for key "cloudSQLPostgreSQLTableName"
    Then Enter runtime argument value "schema" for key "cloudSQLPostgreSQLSchemaName"
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "Failed"

  @CLOUDSQLPOSTGRESQL_SOURCE_TEST @BQ_SINK_TEST @PLUGIN-1526
  Scenario: To verify data is getting transferred from CloudSQLPostgreSQL source to BigQuery sink using macro arguments in connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL PostgreSQL" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "cloudSQLPostgreSQLDriverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "user" and set the value to: "cloudSQLPostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "cloudSQLPostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Validate "CloudSQL PostgreSQL" plugin properties
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
    Then Enter runtime argument value "driverName" for key "cloudSQLPostgreSQLDriverName"
    Then Enter runtime argument value from environment variable "username" for key "cloudSQLPostgreSQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "cloudSQLPostgreSQLPassword"
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
    Then Enter runtime argument value "driverName" for key "cloudSQLPostgreSQLDriverName"
    Then Enter runtime argument value from environment variable "username" for key "cloudSQLPostgreSQLUsername"
    Then Enter runtime argument value from environment variable "password" for key "cloudSQLPostgreSQLPassword"
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
    Then Validate the values of records transferred to target Big Query table is equal to the values from source table

  @CLOUDSQLPOSTGRESQL_SOURCE_TEST @BQ_SINK_TEST @PLUGIN-1526
  Scenario: To verify data is getting transferred from CloudSQLPostgreSQL source to BigQuery sink using macro arguments in basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL PostgreSQL" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "splitBy" and set the value to: "cloudSQLPostgreSQLSplitByColumn"
    Then Click on the Macro button of Property: "importQuery" and set the value in textarea: "cloudSQLPostgreSQLImportQuery"
    Then Validate "CloudSQL PostgreSQL" plugin properties
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
    Then Enter runtime argument value "splitByColumn" for key "cloudSQLPostgreSQLSplitByColumn"
    Then Enter runtime argument value "selectQuery" for key "cloudSQLPostgreSQLImportQuery"
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
    Then Enter runtime argument value "splitByColumn" for key "cloudSQLPostgreSQLSplitByColumn"
    Then Enter runtime argument value "selectQuery" for key "cloudSQLPostgreSQLImportQuery"
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
    Then Validate the values of records transferred to target Big Query table is equal to the values from source table
