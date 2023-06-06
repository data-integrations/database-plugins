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
Feature: Oracle source- Verify Oracle source plugin design time macro scenarios

  Scenario: To verify Oracle source plugin validation with macro enabled fields for connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "Oracle" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "Oracle"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "oracleDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "oracleHost"
    Then Click on the Macro button of Property: "port" and set the value to: "oraclePort"
    Then Click on the Macro button of Property: "user" and set the value to: "oracleUser"
    Then Click on the Macro button of Property: "password" and set the value to: "oraclePassword"
    Then Click on the Macro button of Property: "transactionIsolationLevel" and set the value to: "oracleTransactionLevel"
    Then Click on the Macro button of Property: "database" and set the value to: "oracleDatabase"
    Then Click on the Macro button of Property: "connectionArguments" and set the value to: "oracleConnectionArguments"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Validate "Oracle" plugin properties
    Then Close the Plugin Properties page

  Scenario: To verify Oracle source plugin validation with macro enabled fields for basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "Oracle" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "Oracle"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Select radio button plugin property: "connectionType" with value: "service"
    Then Select radio button plugin property: "role" with value: "normal"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "splitBy" and set the value to: "oracleSplitBy"
    Then Click on the Macro button of Property: "fetchSize" and set the value to: "oracleFetchSize"
    Then Click on the Macro button of Property: "boundingQuery" and set the value in textarea: "oracleBoundingQuery"
    Then Click on the Macro button of Property: "importQuery" and set the value in textarea: "oracleImportQuery"
    Then Validate "Oracle" plugin properties
    Then Close the Plugin Properties page
