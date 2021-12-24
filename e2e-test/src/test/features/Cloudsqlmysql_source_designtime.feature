Feature: CloudSqlMySql Source Design Time

  @CLDMYSQL @TC-Mandatory-fields
  Scenario Outline:Verify CloudSQLMYSQL Source properties validation errors for mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Open CloudSQLMySQL Properties
    Then Enter the CloudSQLMySQL Source Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property       |
      | referenceName  |
      | database       |
      | connectionName |
      | importQuery    |
      | jdbcPluginName |

  @CLDMYSQL @TC-Invalid-TestData-for-DriverName_Field:
  Scenario: TC-CLDMYSQL-DSGN-02:Verify Driver Name field validation error with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Open CloudSQLMySQL Properties
    Then Enter Reference Name "clsReferenceNameValid" & Database Name "clsDatabaseName" with Test Data
    Then Enter Connection Name "clsConnectionNameValid" and Import Query "clsImportQuery"
    Then Enter Driver Name with Invalid value for Driver name field "clsDriverNameInvalid"
    Then Verify invalid Driver name error message is displayed for Driver "clsDriverNameInvalid"
    Then Verify plugin validation fails with error
    Then Close the CloudSQLMySQL Properties

  @CLDMYSQL @TC-Invalid-TestData-for-ImportQuery_Field:
  Scenario: TC-CLDMYSQL-DSGN-03:Verify ImportQuery Field validation error with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Open CloudSQLMySQL Properties
    Then Enter Reference Name "clsReferenceNameValid" & Database Name "clsDatabaseName" with Test Data
    Then Enter Connection Name "clsConnectionNameValid" and Import Query "clsInvalidImportQuery"
    Then Validate Connector properties
    Then Verify invalid import query error message is displayed for import query "clsInvalidImportQuery"
    Then Verify plugin validation fails with error
    Then Close the CloudSQLMySQL Properties

  @CLDMYSQL @TC-Invalid-TestData-for-ReferenceName&ConnectionName
  Scenario: TC-CLDMYSQL-DSGN-04:Verify Reference Name & Connection Name field validation errors with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Enter Reference Name & Connection Name with Invalid Test Data and import query "clsImportQuery"
    Then Verify Reference Name "clsReferenceNameInvalid" Field with Invalid Test Data
    Then Verify Connection Name "clsConnectionNameInvalid" fields with Invalid Test Data
    Then Enter Connection Name with private instance type
    Then Verify Connection Name with private instance type "clsConnectionNameInvalid"
    Then Close the CloudSQLMySQL Properties

  @CLDMYSQL @TC-Blank-value-error-Validation-for-SplitColumn-Invalid-value-for-Number-of-splits:
  Scenario: TC-CLDMYSQL-DSGN-05:Verify the Split-By field validation error and invalid Number of Splits
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Enter the source CloudSQLMySQL properties with import query "clsImportQuery1" bounding query "clsBoundingQuery"
    Then Provide blank values in Split column and invalid Number of splits
    Then Click on Validate button
    Then Verify Split-by column field error
    Then Verify Number of splits field error
    Then Close the CloudSQLMySQL Properties

  @CLDMYSQL @TC-Blank-value-error-Validation-for-Bounding-Query:
  Scenario: TC-CLDMYSQL-DSGN-06:Verify the Bounding Query validation error when values are not given
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Enter the source CloudSQL-MySQL properties with import query "clsImportQuery1" and blank bounding query
    Then Provide blank values in Split column and invalid Number of splits
    Then Verify Bounding Query field error
    Then Close the CloudSQLMySQL Properties

  @CLDMYSQL @TC-Add-Comments
  Scenario: TC-CLDMYSQL-DSGN-07:Verify the Add Comments functionality for CloudSQL MySQL connector
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Add and Save Comments "clsPluginComment"
    Then Validate Source Comment

  @CLDMYSQL @TC-Edit-Comments
  Scenario: TC-CLDMYSQL-DSGN-08:Verify the Edit added Comments functionality for CloudSQL MySQL connector
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Add and Save Comments "clsPluginComment"
    Then Edit Source Comments "clsPluginUpdateComment"
    Then Validate Source Update Comment

  @CLDMYSQL @TC-Delete-Comments
  Scenario: TC-CLDMYSQL-DSGN-09:Verify the Delete added Comments functionality for CloudSQL MySQL connector
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLMySQL
    Then Add and Save Comments "clsPluginComment"
    Then Edit Source Comments "clsPluginUpdateComment"
    Then Delete Comments
    Then Validate Comment has been deleted successfully
