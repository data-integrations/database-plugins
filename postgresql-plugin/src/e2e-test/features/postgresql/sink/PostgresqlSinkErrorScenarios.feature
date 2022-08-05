#
# Copyright © 2022 Cask Data, Inc.
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
Feature:PostgreSQL Sink - Verify PostgreSQL Sink Plugin Error scenarios

  Scenario:Verify PostgreSQL sink plugin validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "PostgreSQL" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "PostgreSQL"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | jdbcPluginName |
      | referenceName  |
      | database       |
      | tableName      |
