Feature: CloudSQLMySQl Sink Design Time

  @CLDMYSQL @TC-Mandatory-fields
  Scenario Outline:Verify CloudSQLMYSQL Sink properties validation errors for mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLMySQL
    Then Open CloudSQLMySQL Properties
    Then Enter the CloudSQLMySQL Sink Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property       |
      | referenceName  |
      | database       |
      | connectionName |
      | tableName      |
      | jdbcPluginName |

  @CLDMYSQL @TC-Invalid-TestData-for-DriverName_Field:
  Scenario: TC-CLDMYSQL-DSGN-02:Verify Driver Name field validation error with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLMySQL
    Then Open CloudSQLMySQL Properties
    Then Enter Reference Name "clsReferenceNameValid" & Database Name "clsDatabaseName" with Test Data
    Then Enter Table Name "clsTableNameBQCS" and Connection Name "clsConnectionNameValid"
    Then Validate Connector properties
    Then Enter Driver Name with Invalid value for Driver name field "clsDriverNameInvalid"
    Then Verify invalid Driver name error message is displayed for Driver "clsDriverNameInvalid"
    Then Verify plugin validation fails with error
    Then Close the CloudSQLMySQL Properties

  @CLDMYSQL @TC-Invalid-TestData-for-ReferenceName&ConnectionName
  Scenario: TC-CLDMYSQL-DSGN-03:Verify properties validation errors for invalid test data for Reference name & connection name
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLMySQL
    Then Enter Reference Name & Connection Name with Invalid Test Data in Sink
    Then Verify Reference Name "clsReferenceNameInvalid" Field with Invalid Test Data
    Then Verify Connection Name "clsConnectionNameInvalid" fields with Invalid Test Data
    Then Enter Connection Name with private instance type
    Then Verify Connection Name with private instance type "clsConnectionNameInvalid"
    Then Close the CloudSQLMySQL Properties

  @CLDMYSQL @TC-Add-Comments
  Scenario: TC-CLDMYSQL-DSGN-04:Verify the Add Comments functionality for CloudSQL MySQL connector
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLMySQL
    Then Add and Save Comments sink "clsPluginComment"
    Then Validate Sink Comment

  @CLDMYSQL @TC-Edit-Comments
  Scenario: TC-CLDMYSQL-DSGN-05:Verify the Edit added Comments functionality for CloudSQL MySQL connector
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLMySQL
    Then Add and Save Comments sink "clsPluginComment"
    Then Edit Sink Comments "clsPluginUpdateComment"
    Then Validate Sink Update Comment

  @CLDMYSQL @TC-Delete-Comments
  Scenario: TC-CLDMYSQL-DSGN-06:Verify the Delete added Comments functionality for CloudSQL MySQL connector
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLMySQL
    Then Add and Save Comments sink "clsPluginComment"
    Then Edit Sink Comments "clsPluginUpdateComment"
    Then Delete Comments
    Then Validate Comment has been deleted successfully
