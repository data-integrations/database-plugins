/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.cloudsqlmysql.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.cloudsqlmysql.actions.CdfCloudSqlMySqlActions;
import io.cdap.plugin.cloudsqlmysql.locators.CdfCloudSqlMySqlLocators;
import io.cdap.plugin.utils.CloudMySqlClient;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cdap.plugin.utils.E2ETestUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
/**a
 * CloudSqlMySql related step design.
 */
public class CloudSqlMySql implements CdfHelper {
    public static String folderName;
    List<String> propertiesSchemaColumnList = new ArrayList<>();
    Map<String, String> sourcePropertiesOutputSchema = new HashMap<>();
    static int i = 0;
    GcpClient gcpClient = new GcpClient();
    int cloudSqlMySqlPreRecordsCount;
    int cloudSqlMySqlPostRecordsCount;

    @Given("Open DataFusion Project to configure pipeline")
    public void openDataFusionProjectToConfigurePipeline() throws IOException, InterruptedException {
        openCdf();
    }

    @When("Source is GCS bucket")
    public void sourceIsGCSBucket() throws InterruptedException {
        CdfStudioActions.selectGCS();
    }

    @When("Source is CloudSQLMySQL")
    public void sourceIsCloudSQLMySQL() throws InterruptedException {
        CdfCloudSqlMySqlActions.selectCloudSQLMySQL();
    }

    @When("Source is BigQuery")
    public void sourceIsBigQuery() throws InterruptedException {
        CdfCloudSqlMySqlActions.selectBigQuerySource();
    }

    @When("Target is CloudSQLMySQL")
    public void targetIsCloudSQLMySQL() {
        CdfCloudSqlMySqlActions.sinkCloudSQLMySQL();
    }

    @When("Target is GCS")
    public void targetIsGCS() throws InterruptedException {
        CdfStudioActions.sinkGcs();
    }

    @Then("Open CloudSQLMySQL Properties")
    public void openCloudSQLMySQLProperties() throws InterruptedException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.validateButton, 10);
    }

    @Then("Validate Connector properties")
    public void validatePipeline() throws InterruptedException {
        CdfCloudSqlMySqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.closeButton, 10);
    }

    @Then("Enter the CloudSQLMySQL Source Properties with blank property {string}")
    public void enterTheCloudSQLMySQLSourcePropertiesWithBlankProperty(String property) throws IOException,
            InterruptedException {
        if (property.equalsIgnoreCase("referenceName")) {
            CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
            CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
            CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp("clsImportQuery"));
        } else if (property.equalsIgnoreCase("database")) {
            CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
            CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
            CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp("clsImportQuery"));
        } else if (property.equalsIgnoreCase("connectionName")) {
            CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
            CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
            CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp("clsImportQuery"));
        } else if (property.equalsIgnoreCase("importQuery")) {
            CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
            CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
            CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
        } else if (property.equalsIgnoreCase("jdbcPluginName")) {
            CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
            SeleniumHelper.replaceElementValue(CdfCloudSqlMySqlLocators.driverName, "");
            CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
            CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
            CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp("clsImportQuery"));
        } else {
            Assert.fail("Invalid CloudSQlMySQL source mandatory field " + property);
        }
    }

    @Then("Enter the CloudSQLMySQL Sink Properties with blank property {string}")
    public void enterTheCloudSQLMySQLSinkPropertiesWithBlankProperty(String property) throws IOException,
            InterruptedException {
        if (property.equalsIgnoreCase("referenceName")) {
            CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
            CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
            CdfCloudSqlMySqlActions.enterTableName(E2ETestUtils.pluginProp("clsTableNameBQCS"));
        } else if (property.equalsIgnoreCase("database")) {
            CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
            CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
            CdfCloudSqlMySqlActions.enterTableName(E2ETestUtils.pluginProp("clsTableNameBQCS"));
        } else if (property.equalsIgnoreCase("connectionName")) {
            CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
            CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
            CdfCloudSqlMySqlActions.enterTableName(E2ETestUtils.pluginProp("clsTableNameBQCS"));
        } else if (property.equalsIgnoreCase("tableName")) {
            CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
            CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
            CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
        } else if (property.equalsIgnoreCase("jdbcPluginName")) {
            CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
            SeleniumHelper.replaceElementValue(CdfCloudSqlMySqlLocators.driverName, "");
            CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
            CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
            CdfCloudSqlMySqlActions.enterTableName(E2ETestUtils.pluginProp("clsTableNameBQCS"));
        } else {
            Assert.fail("Invalid CloudSQLMYSQL sink mandatory field " + property);
        }
    }

    @Then("Validate mandatory property error for {string}")
    public void validateMandatoryPropertyErrorFor(String property) {
        CdfGcsActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton, 7L);
        E2ETestUtils.validateMandatoryPropertyError(property);
    }

    @Then("Enter Reference Name & Connection Name with Invalid Test Data and import query {string}")
    public void enterReferenceNameConnectionNameWthTheInvalidTestData(String query) throws InterruptedException,
            IOException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameInvalid"));
        CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
        CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameInvalid"));
        CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp(query));
    }

    @Then("Validate CloudSQLMySQL properties")
    public void validateCloudSQLMySQLProperties() {
        CdfGcsActions.clickValidateButton();
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Enter Connection Name with private instance type")
    public void enterTheInvalidPrivate() throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.clickPrivateInstance();
        CdfCloudSqlMySqlActions.clickValidateButton();
    }

    @Then("Enter Reference Name & Connection Name with Invalid Test Data in Sink")
    public void enterReferenceNameConnectionNameWithInvalidTestDataInSink() throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameInvalid"));
        CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
        CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameInvalid"));
        CdfCloudSqlMySqlActions.enterTableName(E2ETestUtils.pluginProp("clsTableNameBQCS"));
    }

    @Then("Verify Reference Name {string} Field with Invalid Test Data")
    public void verifyReferenceNameFieldWithInvalidTestData(String referenceName) throws InterruptedException {
        CdfCloudSqlMySqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.validateButton);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_INVALID_REFERENCE_NAME)
                .replace("REFERENCE_NAME", E2ETestUtils.pluginProp(referenceName));
        String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("referenceName").getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
        String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("referenceName"));
        String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
        Assert.assertEquals(expectedColor, actualColor);
        CdfCloudSqlMySqlActions.clickValidateButton();
    }

    @Then("Verify Connection Name with private instance type {string}")
    public void verifyConnectionNameWithPrivateInstanceType(String connectionName) throws InterruptedException {
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.validateButton);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_INVALID_CONNECTION_PRIVATE)
                .replace("CONNECTION_NAME", E2ETestUtils.pluginProp(connectionName));
        String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("connectionName").getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
        String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("connectionName"));
        String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
        Assert.assertEquals(expectedColor, actualColor);
        CdfCloudSqlMySqlActions.clickValidateButton();
    }

    @Then("Close the CloudSQLMySQL Properties")
    public void closeTheCloudSQLMySQLProperties() {
        CdfCloudSqlMySqlActions.closeButton();
    }

    @Then("Enter the source CloudSQLMySQL Properties with import query {string}")
    public void enterTheSourceCloudSQLMySQLPropertiesWithImportQuery(String query) throws IOException,
            InterruptedException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
        CdfCloudSqlMySqlActions.enterDefaultDriver(E2ETestUtils.pluginProp("clsDriverNameValid"));
        CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
        CdfCloudSqlMySqlActions.enterUserName(System.getenv("Cloud_Mysql_User_Name"));
        CdfCloudSqlMySqlActions.enterPassword(System.getenv("Cloud_Mysql_Password"));
        CdfCloudSqlMySqlActions.clickPrivateInstance();
        CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
        CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp(query));
        CdfCloudSqlMySqlActions.getSchema();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.getSchemaButton, 30);
    }

    @Then("Enter the sink CloudSQLMySQL Properties for table {string}")
    public void enterSinkMandatoryFields(String tableName) throws IOException, InterruptedException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
        CdfCloudSqlMySqlActions.enterDefaultDriver(E2ETestUtils.pluginProp("clsDriverNameValid"));
        CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
        CdfCloudSqlMySqlActions.enterUserName(System.getenv("Cloud_Mysql_User_Name"));
        CdfCloudSqlMySqlActions.enterPassword(System.getenv("Cloud_Mysql_Password"));
        CdfCloudSqlMySqlActions.enterTableName(E2ETestUtils.pluginProp(tableName));
        CdfCloudSqlMySqlActions.clickPrivateInstance();
        CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
        CdfCloudSqlMySqlActions.enterConnectionTimeout(E2ETestUtils.pluginProp("clsConnectionTimeout"));
    }

    @Then("Click on Validate button")
    public void clickValidate() throws IOException, InterruptedException {
        CdfCloudSqlMySqlActions.clickValidateButton();
    }

    @Then("Enter Reference Name {string} & Database Name {string} with Test Data")
    public void enterReferenceNameDatabaseNameWithValidTestData(String referenceName, String databaseName)
            throws InterruptedException,
            IOException {
        CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp(referenceName));
        CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp(databaseName));
    }

    @Then("Verify the get schema status")
    public void verifyTheGetSchemaStatus() throws InterruptedException {
        Assert.assertFalse(CdfCloudSqlMySqlLocators.getSchemaStatus.isDisplayed());
    }

    @Then("Validate the Schema")
    public void validateTheSchema() throws InterruptedException {
        CdfCloudSqlMySqlActions.getSchema();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.getSchemaLoadComplete, 10L);
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.outputSchemaColumnNames.get(0), 2L);
        int index = 0;
        for (WebElement element : CdfCloudSqlMySqlLocators.outputSchemaColumnNames) {
            propertiesSchemaColumnList.add(element.getAttribute("value"));
            sourcePropertiesOutputSchema.put(element.getAttribute("value"),
                    CdfCloudSqlMySqlLocators.outputSchemaDataTypes.get(index).getAttribute("title"));
            index++;
        }
        Assert.assertTrue(propertiesSchemaColumnList.size() >= 1);
    }

    @Then("Verify the Connector status")
    public void verifyTheConnectorStatus() throws InterruptedException {
        CdfStudioActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pluginValidationSuccessMsg, 20);
        Assert.assertTrue(SeleniumDriver.getDriver().findElement
                (By.xpath("//*[@data-cy='plugin-validation-success-msg']")).isDisplayed());
    }

    @When("Target is BigQuery")
    public void targetIsBigQuery() {
        CdfStudioActions.sinkBigQuery();
    }

    @Then("Add and Save Comments {string}")
    public void addComments(String comment) throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.clickComment();
        CdfCloudSqlMySqlActions.addComment(E2ETestUtils.pluginProp(comment));
        CdfCloudSqlMySqlActions.saveComment();
    }

    @Then("Add and Save Comments sink {string}")
    public void addCommentSink(String sinkComment) throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.clickComment();
        CdfCloudSqlMySqlActions.addCommentSink(E2ETestUtils.pluginProp(sinkComment));
        CdfCloudSqlMySqlActions.saveComment();
    }

    @Then("Edit Source Comments {string}")
    public void editComments(String updateComment) throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.editComment();
        CdfCloudSqlMySqlActions.clickEdit();
        CdfCloudSqlMySqlActions.updateSourceComment(E2ETestUtils.pluginProp(updateComment));
        CdfCloudSqlMySqlActions.saveComment();
    }

    @Then("Edit Sink Comments {string}")
    public void editSinkComments(String updateComment) throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.editComment();
        CdfCloudSqlMySqlActions.clickEdit();
        CdfCloudSqlMySqlActions.updateSinkComment(E2ETestUtils.pluginProp(updateComment));
        CdfCloudSqlMySqlActions.saveComment();
    }

    @Then("Delete Comments")
    public void deleteComments() throws InterruptedException {
        CdfCloudSqlMySqlActions.editComment();
        CdfCloudSqlMySqlActions.clickDelete();
    }

    @Then("Validate Source Comment")
    public void validateSourceComment() throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.validateComment
                (E2ETestUtils.pluginProp("clsPluginValidateComment"),
                        CdfCloudSqlMySqlLocators.validateComment.getText());
    }

    @Then("Validate Sink Comment")
    public void validateSinkComment() throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.validateComment
                (E2ETestUtils.pluginProp("clsPluginValidateComment"),
                        CdfCloudSqlMySqlLocators.validateSinkComment.getText());
    }

    @Then("Validate Source Update Comment")
    public void validateSourceUpdateComment() throws InterruptedException, IOException {
        CdfCloudSqlMySqlActions.validateComment
                (E2ETestUtils.pluginProp("clsPluginValidateUpdateComment"),
                        CdfCloudSqlMySqlLocators.validateComment.getText());
    }

    @Then("Link CloudSQLMySQL to BigQuery to establish connection")
    public void linkCldMySqlToBQEstablishConnection() throws InterruptedException {
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.toBigQiery);
        SeleniumHelper.dragAndDrop(CdfCloudSqlMySqlLocators.fromCloudSqlMysqlSource, CdfStudioLocators.toBigQiery);
    }

    @Then("Link CloudSQLMySQL to GCS to establish connection")
    public void linkCldMySqlToGCSEstablishConnection() throws InterruptedException {
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.toGCS);
        SeleniumHelper.dragAndDrop(CdfCloudSqlMySqlLocators.fromCloudSqlMysqlSource, CdfStudioLocators.toGCS);
    }

    @Then("Enter the GCS Properties and {string} file format")
    public void enterTheGCSProperties(String fileFormat) throws InterruptedException, IOException {
        CdfGcsActions.gcsProperties();
        CdfGcsActions.enterReferenceName();
        CdfGcsActions.enterProjectId();
        CdfGcsActions.getGcsBucket(E2ETestUtils.pluginProp("clsGCSFilePath"));
        CdfGcsActions.selectFormat(fileFormat);
        CdfGcsActions.clickValidateButton();
    }

    @Then("Enter the Source BigQuery Properties for table {string}")
    public void enterTheBQSourceProperties(String tableName) throws InterruptedException, IOException {
        CdfStudioActions.clickProperties("BigQuery");
        CdfBigQueryPropertiesActions.enterProjectId(E2ETestUtils.pluginProp("clsProjectID"));
        CdfBigQueryPropertiesActions.enterBigQueryReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
        CdfBigQueryPropertiesActions.enterBigQueryDataset(E2ETestUtils.pluginProp("clsDataset"));
        CdfBigQueryPropertiesActions.enterBigQueryTable(E2ETestUtils.pluginProp(tableName));
    }

    @Then("Enter the Source BigQuery with filter {string} option")
    public void enterTheBQSourcePropertiesFilter(String filter) throws InterruptedException, IOException {
        CdfBigQueryPropertiesActions.enterFilter(E2ETestUtils.pluginProp(filter));
    }

    @Then("Link GCS to CloudSQLMySQL to establish connection")
    public void linkGCStoCloudSQLMySQLEstablishConnection() throws InterruptedException {
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.toCloudSqlMysqlSink);
        SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, CdfCloudSqlMySqlLocators.toCloudSqlMysqlSink);
    }

    @Then("Enter the GCS Properties with {string} GCS bucket and format {string}")
    public void enterTheGCSPropertiesWithGCSBucket(String bucket, String format) throws InterruptedException,
            IOException {
        CdfGcsActions.gcsProperties();
        CdfGcsActions.enterReferenceName();
        CdfGcsActions.enterProjectId();
        CdfGcsActions.getGcsBucket(E2ETestUtils.pluginProp(bucket));
        CdfGcsActions.selectFormat(E2ETestUtils.pluginProp(format));
        CdfGcsActions.skipHeader();
        CdfGcsActions.getSchema();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.getSchemaButton, 30);
    }

    @Then("Run and Preview")
    public void runAndPreview() throws InterruptedException {
        CdfStudioActions.runAndPreviewData();
    }

    @Then("Save and Deploy Pipeline")
    public void saveAndDeployPipeline() throws InterruptedException {
        CdfStudioActions.pipelineName();
        CdfStudioActions.pipelineNameIp("TestPipeline" + UUID.randomUUID().toString());
        CdfStudioActions.pipelineSave();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.statusBanner);
        CdfStudioActions.pipelineDeploy();
    }

    @Then("Run the Pipeline in Runtime")
    public void runThePipelineInRuntime() throws InterruptedException {
        CdfPipelineRunAction.runClick();
    }

    @Then("Wait till pipeline is in running state")
    public void waitTillPipelineIsInRunningState() throws InterruptedException {
        Boolean bool = true;
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 240000);
        wait.until(ExpectedConditions.or(ExpectedConditions.
                        visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
                ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
    }

    @Then("Verify the pipeline status is {string}")
    public void verifyThePipelineStatusIs(String status) {
        boolean webelement = false;
        webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
        Assert.assertTrue(webelement);
    }

    @Then("Open and capture Logs")
    public void openAndCaptureLogs() throws FileNotFoundException {
        captureLogs();
    }

    @Then("Validate successMessage is displayed")
    public void validateSuccessMessageIsDisplayed() {
        CdfLogActions.validateSucceeded();
    }

    @Then("Click on Advance logs and validate the success message")
    public void clickOnAdvanceLogsAndValidateTheSuccessMessage() {
        CdfLogActions.goToAdvanceLogs();
        CdfLogActions.validateSucceeded();
    }

    @Then("Enter the BigQuery Properties for table {string}")
    public void enterTheBigQueryPropertiesForTable(String tableName) throws InterruptedException, IOException {
        CdfBigQueryPropertiesActions.enterBigQueryProperties(E2ETestUtils.pluginProp(tableName));
    }

    @Then("Verify Schema in output")
    public void verifySchemaInOutput() {
        CdfGcsActions.validateSchema();
    }

    @Then("Close the GCS Properties")
    public void closeTheGCSProperties() {
        CdfGcsActions.closeButton();
    }

    @Then("Close the BigQuery Properties")
    public void closeTheBigQueryProperties() {
        CdfGcsActions.closeButton();
    }

    @Then("Get Count of no of records transferred to BigQuery in {string}")
    public void getCountOfNoOfRecordsTransferredToBigQueryIn(String table) throws IOException, InterruptedException {
        int countRecords;
        countRecords = gcpClient.countBqQuery(E2ETestUtils.pluginProp(table));
        BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
        Assert.assertEquals(countRecords, recordOut());
    }

    @Then("Validate count of records transferred from importQuery {string} to BigQuery in {string}")
    public void validateCountOfRecordsTransferredFromImportQueryToBigQueryIn(String importQuery, String bqTable) throws
            IOException, InterruptedException {
        int bqRecordsCount = GcpClient.countBqQuery(E2ETestUtils.pluginProp(bqTable));
        BeforeActions.scenario.write("**No of Records Transferred to BQ ******************:" + bqRecordsCount);
        int cloudSqlMySqlRecordsCount = CloudMySqlClient.getRecordsCount(E2ETestUtils.pluginProp(importQuery));
        BeforeActions.scenario.write("**No of Records fetched with cloudSqlMySql import query ******************:" +
                cloudSqlMySqlRecordsCount);
        Assert.assertEquals(cloudSqlMySqlRecordsCount, bqRecordsCount);
    }

    @Then("Delete the table {string}")
    public void deleteTheTable(String table) throws IOException, InterruptedException {
        gcpClient.dropBqQuery(E2ETestUtils.pluginProp(table));
        BeforeActions.scenario.write("Table Deleted Successfully");
    }

    @Then("Verify Split-by column field error")
    public void verifySplitByColumnFieldError() throws Exception {
        CdfCloudSqlMySqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.splitColumnError, 10L);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MESSAGE_SPLIT_COLUMN_NAME);
        String actualErrorMessage = CdfCloudSqlMySqlLocators.splitColumnError.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Verify Number of splits field error")
    public void verifyNumberOfSplitsFieldError() throws Exception {
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.numberOfSplitError, 10L);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MESSAGE_NUMBER_OF_SPLITS_NAME);
        String actualErrorMessage = CdfCloudSqlMySqlLocators.numberOfSplitError.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Verify Bounding Query field error")
    public void verifyBoundingQueryFieldError() throws Exception {
        CdfCloudSqlMySqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.boundingQueryError, 10L);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MESSAGE_BOUNDING_QUERY);
        String actualErrorMessage = CdfCloudSqlMySqlLocators.boundingQueryError.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Link BigQuery to CloudSQLMySQL to establish connection")
    public void linkBigQueryToCloudSQLMySQLToEstablishConnection() {
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.fromBigQuerySource);
        SeleniumHelper.dragAndDrop(
                CdfCloudSqlMySqlLocators.fromBigQuerySource, CdfCloudSqlMySqlLocators.toCloudSqlMysqlSink);
    }

    @Then("Enter Table Name {string} and Connection Name {string}")
    public void enterTableNameInTableField(String tableName, String connectionName) throws IOException {
        CdfCloudSqlMySqlActions.enterTableName(E2ETestUtils.pluginProp(tableName));
        CdfCloudSqlMySqlActions.clickPrivateInstance();
        CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp(connectionName));
    }

    @Then("Enter Connection Name {string} and Import Query {string}")
    public void enterConnectionImportField(String connection, String query) throws IOException, InterruptedException {
        CdfCloudSqlMySqlActions.enterUserName(System.getenv("Cloud_Mysql_User_Name"));
        CdfCloudSqlMySqlActions.enterPassword(System.getenv("Cloud_Mysql_Password"));
        CdfCloudSqlMySqlActions.clickPrivateInstance();
        CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp(connection));
        CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp(query));
    }

    @Then("Replace and enter Invalid Driver value")
    public void enterInvalidDriverName() throws IOException {
        SeleniumHelper.replaceElementValue(CdfCloudSqlMySqlLocators.driverName,
                E2ETestUtils.pluginProp("clsDriverNameInvalid"));
    }

    @Then("Enter Driver Name with Invalid value for Driver name field {string}")
    public void enterDriverNameDefaultValue(String driverName) throws IOException {
        CdfCloudSqlMySqlActions.enterDefaultDriver(E2ETestUtils.pluginProp(driverName));
    }

    @Then("Create Duplicate pipeline")
    public void createDuplicatePipeline() throws InterruptedException {
        CdfCloudSqlMySqlActions.clickActionButton();
        CdfCloudSqlMySqlActions.clickDuplicateButton();
    }

    @Then("Verify invalid Driver name error message is displayed for Driver {string}")
    public void verifyInvalidDriverNameErrorMessageIsDisplayedForDriver(String driverName) {
        CdfCloudSqlMySqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.validateButton);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_INVALID_DRIVER_NAME)
                .replaceAll("DRIVER_NAME", E2ETestUtils.pluginProp(driverName));
        String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("jdbcPluginName").getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
        String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("jdbcPluginName"));
        String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
        Assert.assertEquals(expectedColor, actualColor);
    }

    @Then("Validate the output record count")
    public void validateTheOutputRecordCount() {
        Assert.assertTrue(recordIn() > 0);
    }

    @Then("Validate studio is opened with duplicate pipeline")
    public void validateStudioIsOpened() {
        Assert.assertTrue(CdfStudioLocators.pipelineDeploy.isDisplayed());
    }

    @Then("Enter the source CloudSQLMySQL properties with import query {string} bounding query {string}")
    public void enterTheAdvancedSourceCloudSQLMySQLProperties(String importQuery,
                                                                           String boundingQuery) throws IOException,
            InterruptedException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
        CdfCloudSqlMySqlActions.enterDefaultDriver(E2ETestUtils.pluginProp("clsDriverNameValid"));
        CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
        CdfCloudSqlMySqlActions.enterUserName(System.getenv("Cloud_Mysql_User_Name"));
        CdfCloudSqlMySqlActions.enterPassword(System.getenv("Cloud_Mysql_Password"));
        CdfCloudSqlMySqlActions.clickPrivateInstance();
        CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
        CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
        CdfCloudSqlMySqlActions.getSchema();
        CdfCloudSqlMySqlActions.enterBoundingQuery(E2ETestUtils.pluginProp(boundingQuery));
    }

    @Then("Enter the source CloudSQL-MySQL properties with import query {string} and blank bounding query")
    public void enterTheSplitColumnInSourceCloudSQLMySQLProperties(String importQuery) throws IOException,
            InterruptedException {
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.enterReferenceName(E2ETestUtils.pluginProp("clsReferenceNameValid"));
        CdfCloudSqlMySqlActions.enterDefaultDriver(E2ETestUtils.pluginProp("clsDriverNameValid"));
        CdfCloudSqlMySqlActions.enterDatabaseName(E2ETestUtils.pluginProp("clsDatabaseName"));
        CdfCloudSqlMySqlActions.enterUserName(System.getenv("Cloud_Mysql_User_Name"));
        CdfCloudSqlMySqlActions.enterPassword(System.getenv("Cloud_Mysql_Password"));
        CdfCloudSqlMySqlActions.clickPrivateInstance();
        CdfCloudSqlMySqlActions.enterConnectionName(E2ETestUtils.pluginProp("clsConnectionNameValid"));
        CdfCloudSqlMySqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
    }

    @Then("Enter the source CloudSQL-MySQL with Split and Number of splits CloudSQLMySQL properties")
    public void enterSplitNumberOfSplitsCloudSQLMySQLProperties() throws IOException, InterruptedException {
        CdfCloudSqlMySqlActions.enterSplitColumn(E2ETestUtils.pluginProp("clsSplitColumn"));
        CdfCloudSqlMySqlActions.enterNumberOfSplits(E2ETestUtils.pluginProp("clsNumberOfSplits"));
    }

    @Then("Capture output schema")
    public void captureOutputSchema() {
        CdfCloudSqlMySqlActions.getSchema();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.getSchemaLoadComplete, 10L);
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.outputSchemaColumnNames.get(0), 2L);
        int index = 0;
        for (WebElement element : CdfCloudSqlMySqlLocators.outputSchemaColumnNames) {
            propertiesSchemaColumnList.add(element.getAttribute("value"));
            sourcePropertiesOutputSchema.put(element.getAttribute("value"),
                    CdfCloudSqlMySqlLocators.outputSchemaDataTypes.get(index).getAttribute("title"));
            index++;
        }
        Assert.assertTrue(propertiesSchemaColumnList.size() >= 1);
    }

    @Then("Save the pipeline")
    public void saveThePipeline() {
        CdfStudioActions.pipelineName();
        CdfStudioActions.pipelineNameIp("CloudSQLMySQL" + UUID.randomUUID().toString());
        CdfStudioActions.pipelineSave();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.statusBanner);
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
        wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
    }

    @Then("Preview and run the pipeline")
    public void previewAndRunThePipeline() {
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.preview, 5);
        CdfStudioLocators.preview.click();
        CdfStudioLocators.runButton.click();
    }

    @Then("Verify the preview of pipeline is {string}")
    public void verifyThePreviewOfPipelineIs(String previewStatus) {
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 180);
        wait.until(ExpectedConditions.visibilityOfElementLocated(
                By.xpath("//*[@data-cy='valium-banner-hydrator']//span[contains(text(),'" + previewStatus + "')]")));
        if (!previewStatus.equalsIgnoreCase("failed")) {
            wait.until(ExpectedConditions.invisibilityOfElementLocated(By.xpath("//*[@data-cy=" +
                    "'valium-banner-hydrator']")));
        }
    }

    @Then("Click on PreviewData for CloudSQL MySQL")
    public void clickOnPreviewDataForCloudSql() {
        CdfCloudSqlMySqlActions.clickCloudSqlPreviewData();
    }

    @Then("Verify Preview output schema matches the outputSchema captured in properties")
    public void verifyPreviewOutputSchemaMatchesTheOutputSchemaCapturedInProperties() {
        List<String> previewSchemaColumnList = new ArrayList<>();
        for (WebElement element : CdfCloudSqlMySqlLocators.previewInputRecordColumnNames) {
            previewSchemaColumnList.add(element.getAttribute("title"));
        }
        Assert.assertTrue(previewSchemaColumnList.equals(propertiesSchemaColumnList));
        CdfCloudSqlMySqlLocators.previewPropertiesTab.click();
        Map<String, String> previewSinkInputSchema = new HashMap<>();
        int index = 0;
        for (WebElement element : CdfCloudSqlMySqlLocators.inputSchemaColumnNames) {
            previewSinkInputSchema.put(element.getAttribute("value"),
                    CdfCloudSqlMySqlLocators.inputSchemaDataTypes.get(index).getAttribute("title"));
            index++;
        }
        Assert.assertTrue(previewSinkInputSchema.equals(sourcePropertiesOutputSchema));
    }

    @Then("Close the Preview")
    public void closeThePreview() {
        CdfCloudSqlMySqlActions.clickPreviewCloseButton();
        CdfStudioActions.previewSelect();
    }

    @Then("Deploy the pipeline")
    public void deployThePipeline() {
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
        CdfStudioActions.pipelineDeploy();
    }

    @Then("Enter the incorrect values in split column with number of splits")
    public void enterTheIncorrectValuesInSplitColumnWithDefaultNumberOfSplits() throws IOException {
        CdfCloudSqlMySqlLocators.splitColumn.sendKeys(E2ETestUtils.pluginProp("clsIncorrectSplit"));
        SeleniumHelper.replaceElementValue(CdfCloudSqlMySqlLocators.numberOfSplits,
                E2ETestUtils.pluginProp("clsNumberOfSplits"));
    }

    @Then("Provide blank values in Split column and invalid Number of splits")
    public void provideBlankValuesInSplitColumnAndNumberOfSplitsFields() throws IOException {
        SeleniumHelper.replaceElementValue(CdfCloudSqlMySqlLocators.numberOfSplits,
                E2ETestUtils.pluginProp("clsInvalidNumberOfSplits"));
    }

    @Then("Click on PreviewData for BigQuery")
    public void clickOnPreviewDataForBigQuery() {
        CdfBigQueryPropertiesActions.clickPreviewData();
    }

    @Then("Click on PreviewData for GCS")
    public void clickOnPreviewDataForGCS() {
        CdfCloudSqlMySqlLocators.gcsPreviewData.click();
    }

    @Then("Validate Comment has been deleted successfully")
    public void validateCommentHasBeenDeletedSuccessfully() throws IOException {
        CdfCloudSqlMySqlActions.closeButton();
        CdfCloudSqlMySqlActions.clickCloudSqlMySqlProperties();
        CdfCloudSqlMySqlActions.clickComment();
        Assert.assertFalse(CdfCloudSqlMySqlLocators.disabledComment.isEnabled());
    }

    @Then("Verify invalid import query error message is displayed for import query {string}")
    public void verifyInvalidImportQueryErrorMessageIsDisplayedForImportQuery(String importQuery) {
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.importQueryError, 10L);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_INVALID_IMPORT_QUERY);
        String actualErrorMessage = CdfCloudSqlMySqlLocators.importQueryError.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Verify plugin validation fails with error")
    public void verifyPluginValidationFailsWithError() {
        CdfStudioActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pluginValidationErrorMsg, 10L);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_ERROR_FOUND_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationErrorMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Verify Connection Name {string} fields with Invalid Test Data")
    public void verifyConnectionNameFieldsWithInvalidTestData(String connectionName) {
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.validateButton);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_INVALID_CONNECTION_PUBLIC)
                .replace("CONNECTION_NAME", E2ETestUtils.pluginProp(connectionName));
        String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("connectionName").getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
        String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("connectionName"));
        String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
        Assert.assertEquals(expectedColor, actualColor);
        CdfCloudSqlMySqlActions.clickValidateButton();
    }

    @Given("Cloud Storage bucket should not exist in {string} with the name {string}")
    public void projectIdCloudStorageBucketShouldNotExistInWithTheName(String projectId, String bucketName) {
        E2ETestUtils.deleteBucket(E2ETestUtils.pluginProp(projectId), E2ETestUtils.pluginProp(bucketName));
    }

    @Then("Verify the folder created in {string} with bucket name {string}")
    public void verifyTheFolderCreatedInWithBucketName(String projectID, String bucketName) {
        folderName = E2ETestUtils.listObjects(E2ETestUtils.pluginProp(projectID),
                E2ETestUtils.pluginProp(bucketName));
        Assert.assertTrue(folderName != null);

    }

    @Then("Validate the count of records transferred from BigQuery {string} to CloudSqlMySql {string}")
    public void validateTheCountOfRecordsTransferredFromBigQueryToCloudSqlMySql(String bqTable, String sqlTable) throws
            IOException, InterruptedException {
        int bqRecordsCount = GcpClient.countBqQuery(E2ETestUtils.pluginProp(bqTable));
        BeforeActions.scenario.write("**No of Records Transferred from BQ ******************:" + bqRecordsCount);
        int cloudSqlMySqlRecordsCount = cloudSqlMySqlPostRecordsCount - cloudSqlMySqlPreRecordsCount;
        BeforeActions.scenario.write("**No of Records Transferred into the cloudSqlMySql table ******************:" +
                cloudSqlMySqlRecordsCount);
        Assert.assertEquals(bqRecordsCount, cloudSqlMySqlRecordsCount);
    }

    @Then("Pre records count from CloudSQLMySQL table {string}")
    public void preRecordsCountFromCloudSQLMySQLTable(String sqlTable) {
        cloudSqlMySqlPreRecordsCount = CloudMySqlClient.countCloudSqlMySqlQuery(E2ETestUtils.pluginProp(sqlTable));
    }

    @Then("Post records count from CloudSQLMySQL table {string}")
    public void postRecordsCountFromCloudSQLMySQLTable(String sqlTable) {
        cloudSqlMySqlPostRecordsCount = CloudMySqlClient.countCloudSqlMySqlQuery(E2ETestUtils.pluginProp(sqlTable));
    }

    public int getRowCountFromBigQueryTableOnTheBasisOfFilter(String bqTable, String filter)
            throws IOException, InterruptedException {
        String projectId = (E2ETestUtils.pluginProp("clsProjectID"));
        String datasetName = (E2ETestUtils.pluginProp("clsDataset"));
        String selectQuery = "SELECT count(*)  FROM `" + projectId + "." + datasetName + "." + E2ETestUtils.pluginProp
                (bqTable) + "` WHERE " +
                E2ETestUtils.pluginProp(filter);
        int rowCount = GcpClient.getSoleQueryResult(selectQuery).map(Integer::parseInt).orElse(0);;
        return rowCount;
    }

    @Then("Validate the count of records transferred from BigQuery {string} to CloudSqlMySql with filter {string}")
    public void validateTheCountOfRecordsTransferredFromBigQueryToCloudSqlMySqlWithFilter(String bqTable, String filter)
            throws IOException, InterruptedException {
        int bqRecordsCount = getRowCountFromBigQueryTableOnTheBasisOfFilter(bqTable, filter);
        BeforeActions.scenario.write("**No of Records Transferred from BQ ******************:" + bqRecordsCount);
        int cloudSqlMySqlRecordsCount = cloudSqlMySqlPostRecordsCount - cloudSqlMySqlPreRecordsCount;
        BeforeActions.scenario.write("**No of Records Transferred into the cloudSqlMySql table ******************:" +
                cloudSqlMySqlRecordsCount);
        Assert.assertEquals(bqRecordsCount, cloudSqlMySqlRecordsCount);
    }

    @Then("Validate Sink Update Comment")
    public void validateSinkUpdateComment() throws IOException {
        CdfCloudSqlMySqlActions.validateComment
                (E2ETestUtils.pluginProp("clsPluginValidateUpdateComment"),
                        CdfCloudSqlMySqlLocators.validateSinkComment.getText());
    }
}
