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

@Regression @Sink_Required
Feature: CloudSQL-PostgreSQL sink - Verify data transfer from BigQuery source to CloudSQL-PostgreSQL sink

  @BQ_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TEST_TABLE @PLUGIN-1526
  Scenario: To verify data is getting transferred from BigQuery source to CloudSQLPostgreSQL sink successfully with supported datatypes
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL PostgreSQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqOutputMultipleDatatypesSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on the Preview Data link on the Sink plugin node: "CloudSQLPostgreSQL"
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to target CloudPostgreSQL table is equal to the values from BigQuery table

  @BQ_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TEST_TABLE @PLUGIN-1526
  Scenario: To verify data is getting transferred from BigQuery source to CloudSQLPostgreSQL sink successfully when connection arguments are set
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL PostgreSQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqOutputMultipleDatatypesSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter key value pairs for plugin property: "connectionArguments" with values from json: "connectionArgumentsList"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on the Preview Data link on the Sink plugin node: "CloudSQLPostgreSQL"
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to target CloudPostgreSQL table is equal to the values from BigQuery table

  @BQ_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TEST_TABLE @PLUGIN-1526
  Scenario: To verify data is getting transferred from BigQuery source to CloudSQLPostgreSQL sink with Advanced property Connection timeout
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "CloudSQL PostgreSQL" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqOutputMultipleDatatypesSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Replace input plugin property: "connectionTimeout" with value: "connectionTimeout"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on the Preview Data link on the Sink plugin node: "CloudSQLPostgreSQL"
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to target CloudPostgreSQL table is equal to the values from BigQuery table
