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

@Postgresql
Feature: PostgreSQL - Verify Postgresql source data transfer
  @POSTGRESQL_SOURCE_TEST @POSTGRESQL_SINK_TEST @Postgresql_Required
  Scenario: To verify data is getting transferred from PostgreSQL to PostgreSQL successfully
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "PostgreSQL" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Connect plugins: "PostgreSQL" and "PostgreSQL2" to establish connection
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Enter textarea plugin property: "importQuery" with value: "selectQuery"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "datatypesSchema"
    Then Validate "PostgreSQL" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "PostgreSQL2"
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "driverName"
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Replace input plugin property: "database" with value: "databaseName"
    Then Replace input plugin property: "tableName" with value: "targetTable"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter input plugin property: "referenceName" with value: "targetRef"
    Then Replace input plugin property: "dbSchemaName" with value: "schema"
    Then Validate "PostgreSQL2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on preview data for PostgreSQL sink
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to target table is equal to the values from source table
