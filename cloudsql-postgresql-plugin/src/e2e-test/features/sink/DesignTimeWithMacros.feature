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
Feature: CloudSQL-PostgreSQL sink- Verify CloudSQL-PostgreSQL sink plugin design time macro scenarios

  Scenario: To verify CloudSQLPostgreSQL sink plugin validation with macro enabled fields for connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "cloudSQLPostGreSQLDriverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "user" and set the value to: "cloudSQLPostGreSQLUser"
    Then Click on the Macro button of Property: "password" and set the value to: "cloudSQLPostGreSQLPassword"
    Then Click on the Macro button of Property: "connectionArguments" and set the value to: "cloudSQLPostGreSQLConnectionArguments"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page

  Scenario: To verify CloudSQLPostgreSQL sink plugin validation with macro enabled fields for basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "CloudSQL PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "CloudSQL PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Select radio button plugin property: "instanceType" with value: "public"
    Then Replace input plugin property: "connectionName" with value: "connectionName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "cloudSQLPostGreSQLTableName"
    Then Click on the Macro button of Property: "dbSchemaName" and set the value to: "cloudSQLPostGreSQLSchemaName"
    Then Validate "CloudSQL PostgreSQL" plugin properties
    Then Close the Plugin Properties page
