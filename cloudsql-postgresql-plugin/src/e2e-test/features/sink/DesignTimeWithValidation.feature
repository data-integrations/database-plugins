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
Feature: CloudSQL-PostgreSQL Sink - Verify CloudSQL-postgreSQL Sink Plugin Error scenarios

  Scenario:Verify CloudSQLPostgreSQL sink plugin validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | jdbcPluginName |
      | referenceName  |
      | database       |
      | tableName      |

  Scenario: To verify CloudSQLPostgreSQL sink plugin validation error message with invalid reference test data
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "invalidRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "referenceName" is displaying an in-line error message: "errorMessageCloudPostgreSQLInvalidReferenceName"

  Scenario: To verify CloudSQLPostgreSQL sink plugin validation error message with invalid connection name test data
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "invalidConnectionName"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "connectionName" is displaying an in-line error message: "errorMessageConnectionName"

  @CLOUDSQLPOSTGRESQL_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TARGET_TEST
  Scenario: To verify CloudSQLPostgreSQL sink plugin validation error message with invalid database
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
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "datatypesSchema"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "invalidDatabaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidSinkDatabase" on the header

  @CLOUDSQLPOSTGRESQL_SOURCE_TEST @CLOUDSQLPOSTGRESQL_TARGET_TEST
  Scenario: To verify CloudSQLPostgreSQL sink plugin validation error message with invalid table name
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
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "datatypesSchema"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "invalidTable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "tableName" is displaying an in-line error message: "errorMessageInvalidTableName"

  Scenario: To verify CloudSQLPostgreSQL sink plugin validation error message with blank username
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "user" is displaying an in-line error message: "errorMessageBlankUsername"
