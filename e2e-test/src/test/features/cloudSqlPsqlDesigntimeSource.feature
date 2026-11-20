Feature: CloudSQLPostgreSQL Source Design Time and error validation

  @cloudSQLPostgreSQL
  Scenario Outline:Verify CloudSQLPostgreSQL Source properties validation errors for mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostgreSQL
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the CloudSQLPostgreSQL Source Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property         |
      | referenceName    |
      | database         |
      | connectionName   |
      | importQuery      |
      | jdbcPluginName   |

  @cloudSQLPostgreSQL
  Scenario:Verify Driver Name field validation error with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostgreSQL
    Then Open cloudSQLPostgreSQL Properties
    Then Enter Reference Name & Database Name with valid test data
    Then Enter Connection Name and Import Query "cloudPSQLImportQuery"
    Then Validate Connector properties
    Then Enter Driver Name with Invalid value for Driver name field "cloudPSQLDriverNameInvalid"
    Then Verify invalid Driver name error message is displayed for Driver "cloudPSQLDriverNameInvalid"
    Then Verify plugin validation fails with error
    Then Close the cloudSQLPostgreSQL properties

  @cloudSQLPostgreSQL
  Scenario:Verify error is displayed for Reference name and Public connection name with incorrect values
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostgreSQL
    Then Enter Reference Name & Connection Name with incorrect values and import query "cloudPSQLImportQuery"
    Then Verify error is displayed for Reference name & connection name with incorrect values
    Then Close the cloudSQLPostgreSQL properties

  @cloudSQLPostgreSQL
  Scenario:Verify error is displayed for Reference name and Private connection name with incorrect values
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostgreSQL
    Then Enter Reference Name and private Connection Name with incorrect values and import query "cloudPSQLImportQuery"
    Then Verify error is displayed for incorrect Connection Name with private instance type
    Then Close the cloudSQLPostgreSQL properties
