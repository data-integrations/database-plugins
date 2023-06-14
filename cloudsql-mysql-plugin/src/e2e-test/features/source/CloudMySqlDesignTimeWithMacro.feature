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

@CloudMySql @CloudMySql_Required
Feature: CloudMySql source- Verify CloudMySql source plugin design time macro scenarios

  @CLOUDMYSQL_SOURCE_TEST
  Scenario: To verify CloudMySql source plugin validation with macro enabled fields for connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "DriverName"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Click on the Macro button of Property: "user" and set the value to: "username"
    Then Click on the Macro button of Property: "password" and set the value to: "password"
    Then Click on the Macro button of Property: "connectionArguments" and set the value to: "connectionArgumentsList"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Validate button
    Then Close the Plugin Properties page

  @CLOUDMYSQL_SOURCE_TEST
  Scenario: To verify cloudsql source plugin validation with macro enabled fields for basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Validate button
    Then Close the Plugin Properties page
