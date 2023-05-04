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

@CloudMySql
Feature: CloudMySql source- Verify CloudMySql source plugin design time scenarios

  Scenario: To verify CloudMySql source plugin validation with mandatory properties
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Enter input plugin property: "connectionName" with value: "ConnectionName"
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "outputSchema"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page

  Scenario: To verify CloudMySql source plugin validation with connection and basic details for connectivity
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Enter input plugin property: "connectionName" with value: "ConnectionName"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "outputSchema"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page

  Scenario: To verify CloudMySql source plugin validation setting up connection arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "cloudsql-mysql"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Enter input plugin property: "connectionName" with value: "ConnectionName"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter key value pairs for plugin property: "connectionArguments" with values from json: "connectionArgumentsList"
    Then Enter input plugin property: "referenceName" with value: "referencename"
    Then Enter input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "outputSchema"
    Then Validate "CloudSQL MySQL" plugin properties
    Then Close the Plugin Properties page