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
# the License

@CloudMySql @CloudMySql_Required
Feature: CloudMySql source- Verify CloudMySql source plugin design time validation scenarios

  Scenario: To verify CloudMySql source plugin validation error message with invalid database
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "RefName"
    Then Replace input plugin property: "database" with value: "invalidDatabase"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidSourceDatabase" on the header

  Scenario: To verify CloudMySql source plugin validation error message with invalid import query
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "invalidImportQuery"
    Then Replace input plugin property: "numSplits" with value: "numberOfSplits"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "importQuery" is displaying an in-line error message: "errorMessageInvalidImportQuery"

  Scenario: To verify CloudMySql source plugin validation error message with invalid reference name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "invalidRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "referenceName" is displaying an in-line error message: "errorMessageCloudMySqlInvalidReferenceName"

  Scenario: To verify CloudMySql source plugin validation error message with blank username
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "user" is displaying an in-line error message: "errorMessageBlankUsername"

  Scenario: To verify CloudMySql source plugin validation error message with blank password
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageBlankPassword" on the header

  Scenario: To verify CloudMySql source plugin validation error message when fetch size is changed to zero
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Replace input plugin property: "fetchSize" with value: "zeroValue"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "fetchSize" is displaying an in-line error message: "errorMessageInvalidFetchSize"

  Scenario: To verify CloudMySql source plugin validation error message with number of splits without split by field name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Replace input plugin property: "numSplits" with value: "numberOfSplits"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "numSplits" is displaying an in-line error message: "errorMessageBlankSplitBy"
    Then Verify that the Plugin Property: "splitBy" is displaying an in-line error message: "errorMessageBlankSplitBy"

  Scenario: To verify CloudMySql source plugin validation error message when number of Split value is changed to zero
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Replace input plugin property: "numSplits" with value: "zeroValue"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "numSplits" is displaying an in-line error message: "errorMessageInvalidNumberOfSplits"

  Scenario: To verify CloudMySql source plugin validation error message when number of Split value is not a number
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Replace input plugin property: "numSplits" with value: "zeroSplits"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "numSplits" is displaying an in-line error message: "errorMessageNumberOfSplitNotNumber"

  Scenario: To verify CloudMySql source plugin validation error message with blank bounding query
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "CloudSQL MySQL" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "CloudSQL MySQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "DatabaseName"
    Then Enter textarea plugin property: "importQuery" with value: "insertQuery"
    Then Replace input plugin property: "numSplits" with value: "numberOfSplits"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "boundingQuery" is displaying an in-line error message: "errorMessageBoundingQuery"
    Then Verify that the Plugin Property: "numSplits" is displaying an in-line error message: "errorMessageNumberOfSplits"
