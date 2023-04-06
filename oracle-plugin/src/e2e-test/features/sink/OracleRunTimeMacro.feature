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

@Oracle @Oracle_Required
Feature: Oracle - Verify data transfer to Oracle sink with macro arguments

  @BQ_SOURCE_TEST @ORACLE_TEST_TABLE
  Scenario: To verify data is getting transferred from BigQuery source to Oracle sink using macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Oracle" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "Oracle" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqDataset"
    Then Click on the Macro button of Property: "table" and set the value to: "bqTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Oracle"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "oracleDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "oracleHost"
    Then Click on the Macro button of Property: "port" and set the value to: "oraclePort"
    Then Click on the Macro button of Property: "user" and set the value to: "oracleUsername"
    Then Click on the Macro button of Property: "password" and set the value to: "oraclePassword"
    Then Select radio button plugin property: "connectionType" with value: "service"
    Then Select radio button plugin property: "role" with value: "sysdba"
    Then Click on the Macro button of Property: "database" and set the value to: "oracleDatabaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "oracleTableName"
    Then Click on the Macro button of Property: "dbSchemaName" and set the value to: "oracleSchemaName"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Validate "Oracle" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqSourceTable" for key "bqTable"
    Then Enter runtime argument value "driverName" for key "oracleDriverName"
    Then Enter runtime argument value from environment variable "host" for key "oracleHost"
    Then Enter runtime argument value from environment variable "port" for key "oraclePort"
    Then Enter runtime argument value from environment variable "username" for key "oracleUsername"
    Then Enter runtime argument value from environment variable "password" for key "oraclePassword"
    Then Enter runtime argument value "databaseName" for key "oracleDatabaseName"
    Then Enter runtime argument value "targetTable" for key "oracleTableName"
    Then Enter runtime argument value "schema" for key "oracleSchemaName"
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
    Then Enter runtime argument value "driverName" for key "oracleDriverName"
    Then Enter runtime argument value from environment variable "host" for key "oracleHost"
    Then Enter runtime argument value from environment variable "port" for key "oraclePort"
    Then Enter runtime argument value from environment variable "username" for key "oracleUsername"
    Then Enter runtime argument value from environment variable "password" for key "oraclePassword"
    Then Enter runtime argument value "databaseName" for key "oracleDatabaseName"
    Then Enter runtime argument value "targetTable" for key "oracleTableName"
    Then Enter runtime argument value "schema" for key "oracleSchemaName"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
