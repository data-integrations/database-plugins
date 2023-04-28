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

@PostgreSQL_Sink
Feature: PostgreSQL sink- Verify PostgreSQL sink plugin design time macro scenarios

  Scenario: To verify PostgreSQL sink plugin validation with macro enabled fields for connection section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Click on the Macro button of Property: "jdbcPluginName" and set the value to: "postGreSQLDriverName"
    Then Click on the Macro button of Property: "host" and set the value to: "postGreSQLHost"
    Then Click on the Macro button of Property: "port" and set the value to: "postGreSQLPort"
    Then Click on the Macro button of Property: "user" and set the value to: "postGreSQLUser"
    Then Click on the Macro button of Property: "password" and set the value to: "postGreSQLPassword"
    Then Click on the Macro button of Property: "connectionArguments" and set the value to: "postGreSQLConnectionArguments"
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page

  Scenario: To verify PostgreSQL sink plugin validation with macro enabled fields for basic section
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Click on the Macro button of Property: "tableName" and set the value to: "postGreSQLTableName"
    Then Click on the Macro button of Property: "dbSchemaName" and set the value to: "postGreSQLSchemaName"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
