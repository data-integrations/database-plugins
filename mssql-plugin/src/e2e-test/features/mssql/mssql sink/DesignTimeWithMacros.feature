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

@Mysql
Feature: MSSQL Server Sink - Design time scenarios (macro)

  @MSSQL_SOURCE_TEST @MSSQL_SINK_TEST @Mssql_Required
  Scenario: Verify user should be able to validate sink plugin with macros for Connection section
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
    Then Validate "SQL Server" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "SQL Server2"
    And Click on the Macro button of Property: "jdbcPluginName" and set the value to: "DriverName"
    And Click on the Macro button of Property: "host" and set the value to: "Host"
    And Click on the Macro button of Property: "port" and set the value to: "Port"
    And Click on the Macro button of Property: "user" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    And Click on the Validate button

  @MSSQL_SOURCE_TEST @MSSQL_SINK_TEST @Mssql_Required
  Scenario: Verify user should be able to validate sink plugin with macros for Basic section
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
    Then Validate "SQL Server" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "SQL Server2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    And Click on the Macro button of Property: "database" and set the value to: "db"
    And Click on the Macro button of Property: "tableName" and set the value to: "tableTarget"
    And Click on the Macro button of Property: "dbSchemaName" and set the value to: "schemaName"
    And Click on the Validate button
