# Copyright © 2023 Cask Data, Inc.
##
## Licensed under the Apache License, Version 2.0 (the "License"); you may not
## use this file except in compliance with the License. You may obtain a copy of
## the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
## License for the specific language governing permissions and limitations under
# the License..

@CloudMySql @CloudMySql_Required
Feature: CloudMySql sink- Verify CloudsqlMysql sink plugin design time validation scenarios

  @CLOUDMYSQL_SOURCE_TEST @CLOUDMYSQL_TARGET_TEST
  Scenario: To verify CloudMySql sink plugin validation error message with invalid database
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL MySQL" and "CloudSQL MySQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
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
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "invalidDatabaseName"
    Then Enter input plugin property: "tableName" with value: "mytable"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidSinkDatabase" on the header

  @CLOUDMYSQL_SOURCE_TEST @CLOUDMYSQL_TARGET_TEST
  Scenario: To verify CloudMySql sink plugin validation error message with invalid tablename
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Connect plugins: "CloudSQL MySQL" and "CloudSQL MySQL2" to establish connection
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
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
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "invalidRefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter input plugin property: "tableName" with value: "Invalidtable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "tableName" is displaying an in-line error message: "errorMessageInvalidTableName"

  Scenario: To verify CloudMySql sink plugin validation error message with invalid reference Name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "invalidRef"
    Then Enter key value pairs for plugin property: "connectionArguments" with values from json: "connectionArgumentsList"
    Then Enter input plugin property: "database" with value: "TestDatabase"
    Then Enter input plugin property: "tableName" with value: "mytable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "referenceName" is displaying an in-line error message: "errorMessageCloudMySqlInvalidReferenceName"

  Scenario: To verify CloudMySql sink plugin validation error message with blank username
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter key value pairs for plugin property: "connectionArguments" with values from json: "connectionArgumentsList"
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "TestDatabase"
    Then Enter input plugin property: "tableName" with value: "mytable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "user" is displaying an in-line error message: "errorMessageBlankUsername"

  Scenario: To verify CloudMySql sink plugin validation error message with ConnectionName
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Enter input plugin property: "connectionName" with value: "cdf-athena:us-central1"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter key value pairs for plugin property: "connectionArguments" with values from json: "connectionArgumentsList"
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "TestDatabase"
    Then Enter input plugin property: "tableName" with value: "mytable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "connectionName" is displaying an in-line error message: "errorMessageConnectionName"
