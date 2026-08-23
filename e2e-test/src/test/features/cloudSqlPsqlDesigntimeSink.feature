Feature: CloudSQLPostgreSQL Sink and Error Validation

  @cloudSQLPostgreSQL
  Scenario Outline:Verify CloudSQLPostgreSQL Sink properties validation errors for mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLPostgreSQL
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the CloudSQLPostgreSQL Sink Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | database        |
      | connectionName  |
      | tableName       |
      | jdbcPluginName  |

  @cloudSQLPostgreSQL
  Scenario: Verify error is displayed and validation fails for incorrect Driver name value
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLPostgreSQL
    Then Open cloudSQLPostgreSQL Properties
    Then Enter Reference Name & Database Name with valid test data
    Then Enter Table Name "cloudPSQLTableName" and Connection Name
    Then Validate Connector properties
    Then Enter Driver Name with Invalid value for Driver name field "cloudPSQLDriverNameInvalid"
    Then Verify invalid Driver name error message is displayed for Driver "cloudPSQLDriverNameInvalid"
    Then Verify plugin validation fails with error
    Then Close the cloudSQLPostgreSQL properties

  @cloudSQLPostgreSQL
  Scenario:Verify error is displayed for Reference name and Public connection name with incorrect values
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLPostgreSQL
    Then Enter Reference Name and Public Connection Name with incorrect values and table "cloudPSQLTableName"
    Then Verify error is displayed for Reference name & connection name with incorrect values
    Then Close the cloudSQLPostgreSQL properties

  @cloudSQLPostgreSQL
  Scenario:Verify error is displayed for Reference name and Private connection name with incorrect values
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLPostgreSQL
    Then Enter Reference Name and Private Connection Name with incorrect values and table "cloudPSQLTableName"
    Then Verify error is displayed for incorrect Connection Name with private instance type
    Then Close the cloudSQLPostgreSQL properties
