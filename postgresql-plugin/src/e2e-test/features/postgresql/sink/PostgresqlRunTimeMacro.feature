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

@PostgreSQL_Sink
Feature: PostgreSQL - Verify data transfer to PostgreSQL sink with macro arguments

  @BQ_SOURCE_TEST @Postgresql_Required @POSTGRESQL_TEST_TABLE @PLUGIN-1628 @Plugin-1526
  Scenario: To verify data is getting transferred from BigQuery source to PostgreSQL sink using macro arguments in connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "PostgreSQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqDataset"
    Then Click on the Macro button of Property: "table" and set the value to: "bqTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "PostgreSQLDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "PostgreSQLHost"
    Then Click on the Macro button of Property: "port" and set the value to: "PostgreSQLPort"
    Then Click on the Macro button of Property: "user" and set the value to: "PostgreSQLUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "PostgreSQLPassword"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqSourceTable" for key "bqTable"
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
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqSourceTable" for key "bqTable"
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
    Then Validate the values of records transferred to target PostgreSQL table is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @Postgresql_Required @POSTGRESQL_TEST_TABLE @PLUGIN-1628 @Plugin-1526
  Scenario: To verify data is getting transferred from BigQuery source to PostgreSQL sink using macro arguments in basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "PostgreSQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqDataset"
    Then Click on the Macro button of Property: "table" and set the value to: "bqTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "PostgreSQLTableName"
    Then Click on the Macro button of Property: "dbSchemaName" and set the value to: "PostgreSQLSchemaName"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqSourceTable" for key "bqTable"
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
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqSourceTable" for key "bqTable"
    Then Enter runtime argument value "targetTable" for key "PostgreSQLTableName"
    Then Enter runtime argument value "schema" for key "PostgreSQLSchemaName"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target PostgreSQL table is equal to the values from source BigQuery table
