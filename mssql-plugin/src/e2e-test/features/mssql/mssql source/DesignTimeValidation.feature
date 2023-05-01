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

@Mssql
Feature: Mssql source- Verify Mssql source plugin design time validation scenarios

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message with invalid database
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "invalidDatabase"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidSourceDatabase" on the header

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message with invalid import query
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "invalidImportQuery"
    Then Replace input plugin property: "numSplits" with value: "numberOfSplits"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "importQuery" is displaying an in-line error message: "errorMessageInvalidImportQuery"

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message with invalid reference name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "invalidRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "referenceName" is displaying an in-line error message: "errorMessageMssqlInvalidReferenceName"

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message with blank username
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "user" is displaying an in-line error message: "errorMessageBlankUsername"

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message with blank password
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageBlankPassword" on the header

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message when fetch size is changed to zero
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Replace input plugin property: "fetchSize" with value: "zeroValue"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "fetchSize" is displaying an in-line error message: "errorMessageInvalidFetchSize"

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message with number of splits without split by field name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Replace input plugin property: "numSplits" with value: "numberOfSplits"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "numSplits" is displaying an in-line error message: "errorMessageBlankSplitBy"
    Then Verify that the Plugin Property: "splitBy" is displaying an in-line error message: "errorMessageBlankSplitBy"

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message when number of Split value is changed to zero
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Replace input plugin property: "numSplits" with value: "zeroValue"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "numSplits" is displaying an in-line error message: "errorMessageInvalidNumberOfSplits"

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message when number of Split value is not a number
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Replace input plugin property: "numSplits" with value: "zeroSplits"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "numSplits" is displaying an in-line error message: "errorMessageNumberOfSplitNotNumber"

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: To verify Mssql source plugin validation error message with blank bounding query
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Replace input plugin property: "numSplits" with value: "blankvalue"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "boundingQuery" is displaying an in-line error message: "errorMessageBoundingQuery"
    Then Verify that the Plugin Property: "numSplits" is displaying an in-line error message: "errorMessagenumofSplit"
