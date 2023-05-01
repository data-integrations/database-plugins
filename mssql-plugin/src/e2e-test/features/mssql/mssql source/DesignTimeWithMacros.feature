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
Feature: MSSQL Server - Design time scenarios (macro)

  @MSSQL_SOURCE_TEST @Mssql_Required
  Scenario: Verify user should be able to validate plugin with macros for Connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "SQL Server" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "SQL Server"
    And Click on the Macro button of Property: "jdbcPluginName" and set the value to: "DriverName"
    And Click on the Macro button of Property: "host" and set the value to: "Host"
    And Click on the Macro button of Property: "port" and set the value to: "Port"
    And Click on the Macro button of Property: "user" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Validate "SQL Server" plugin properties

  @MSSQL_SOURCE_TEST @Mssql_Required
    Scenario: Verify user should be able to validate plugin with macros for Basic section
      Given Open Datafusion Project to configure pipeline
      When Expand Plugin group in the LHS plugins list: "Source"
      When Select plugin: "SQL Server" from the plugins list as: "Source"
      Then Navigate to the properties page of plugin: "SQL Server"
      Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "sqlserver42"
      Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
      Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
      Then Select radio button plugin property: "authenticationType" with value: "SQL Login"
      Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
      Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
      Then Enter input plugin property: "referenceName" with value: "sourceRef"
      And Click on the Macro button of Property: "database" and set the value to: "db"
      And Click on the Macro button of Property: "fetchSize" and set the value to: "fetchsize"
      And Click on the Macro button of Property: "splitBy" and set the value to: "splitbyfield"
      Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
      Then Validate "SQL Server" plugin properties
