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
package io.cdap.plugin.cloudsqlpostgresql.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.cloudsqlpostgresql.actions.CloudSqlPostgreSqlActions;
import io.cdap.plugin.cloudsqlpostgresql.locators.CloudSqlPostgreSqlLocators;
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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
/**
 * StepsDesign for CloudSqlPostgreSql.
 */
public class CloudSqlPostgreSql implements CdfHelper {
    List<String> propertiesSchemaColumnList = new ArrayList<>();
    Map<String, String> sourcePropertiesOutputSchema = new HashMap<>();
    static PrintWriter out;

    static {
        try {
            out = new PrintWriter(BeforeActions.myObj);
        } catch (FileNotFoundException e) {
            BeforeActions.scenario.write(e.toString());
        }
    }

    @Given("Open DataFusion Project to configure pipeline")
    public void openDataFusionProjectToConfigurePipeline() throws IOException, InterruptedException {
        openCdf();
    }

    @When("Source is CloudSQLPostgreSQL")
    public void sourceIsCloudSQLPostgreSQL() throws InterruptedException {
        CloudSqlPostgreSqlActions.selectCloudSQLPostgreSQLSource();
    }

    @When("Target is CloudSQLPostgreSQL")
    public void targetIsCloudSQLPostgreSQL() throws InterruptedException {
        CloudSqlPostgreSqlActions.selectCloudSQLPostgreSQLSink();
    }

    @When("Sink is BigQuery")
    public void sinkIsBigQuery() {
        CdfStudioActions.sinkBigQuery();
    }

    @Then("Validate Connector properties")
    public void validatePipeline() throws InterruptedException {
        CloudSqlPostgreSqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CloudSqlPostgreSqlLocators.closeButton, 10);
    }

    @Then("Enter Reference Name & Connection Name with Invalid Test Data in Sink")
    public void enterTheSinkInvalidData() throws InterruptedException, IOException {
        CloudSqlPostgreSqlActions.clickCloudSqlPostgreSqlProperties();
        CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPsqlReferenceNameInvalid"));
        CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
        CloudSqlPostgreSqlActions.enterConnectionName(E2ETestUtils.pluginProp("cloudPSQLConnectionNameInvalid"));
        CloudSqlPostgreSqlActions.enterTableName(E2ETestUtils.pluginProp("cloudPSQLTableName"));
    }

    @Then("Verify error is displayed for Reference name & connection name with incorrect values")
    public void verifyErrorIsDisplayedForReferenceNameConnectionNameWithIncorrectValues() {
        Assert.assertTrue(CloudSqlPostgreSqlLocators.referenceNameError.isDisplayed());
        Assert.assertTrue(CloudSqlPostgreSqlLocators.connectionNameFormatError.isDisplayed());
    }

    @Then("Enter Connection Name with private instance type")
    public void enterTheInvalidPrivate() throws InterruptedException, IOException {
        CloudSqlPostgreSqlActions.clickPrivateInstance();
        CloudSqlPostgreSqlActions.clickValidateButton();
    }

    @Then("Verify error is displayed for incorrect Connection Name with private instance type")
    public void verifyTheCldMySqlInvalidPrivate() throws InterruptedException {
        Assert.assertTrue(CloudSqlPostgreSqlLocators.connectionNameError.isDisplayed());
    }

    @Then("Enter the CloudSQLPostgreSQL Source Properties with blank property {string}")
    public void enterTheCloudSQLPostgreSQLSourcePropertiesWithBlankProperty(String property) throws IOException,
      InterruptedException {
        if (property.equalsIgnoreCase("referenceName")) {
            CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
            CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
            CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp("cloudPSQLImportQuery"));
        } else if (property.equalsIgnoreCase("database")) {
            CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
            CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
            CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp("cloudPSQLImportQuery"));
        } else if (property.equalsIgnoreCase("connectionName")) {
            CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
            CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
            CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp("cloudPSQLImportQuery"));
        } else if (property.equalsIgnoreCase("importQuery")) {
            CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
            CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
            CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
        } else if (property.equalsIgnoreCase("jdbcPluginName")) {
            CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
            SeleniumHelper.replaceElementValue(CloudSqlPostgreSqlLocators.driverName, "");
            CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
            CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
            CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp("cloudPSQLImportQuery"));
        } else {
            Assert.fail("Invalid cloudSqlPsql Mandatory Field : " + property);
        }
    }

    @Then("Enter the CloudSQLPostgreSQL Sink Properties with blank property {string}")
    public void enterTheCloudSQLPostgreSQLSinkPropertiesWithBlankProperty(String property) throws IOException,
      InterruptedException {
        if (property.equalsIgnoreCase("referenceName")) {
            CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
            CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
            CloudSqlPostgreSqlActions.enterTableName(E2ETestUtils.pluginProp("cloudPSQLTableName"));
        } else if (property.equalsIgnoreCase("database")) {
            CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
            CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
            CloudSqlPostgreSqlActions.enterTableName(E2ETestUtils.pluginProp("cloudPSQLTableName"));
        } else if (property.equalsIgnoreCase("connectionName")) {
            CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
            CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
            CloudSqlPostgreSqlActions.enterTableName(E2ETestUtils.pluginProp("cloudPSQLTableName"));
        } else if (property.equalsIgnoreCase("tableName")) {
            CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
            CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
            CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
        } else if (property.equalsIgnoreCase("jdbcPluginName")) {
            CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
            SeleniumHelper.replaceElementValue(CloudSqlPostgreSqlLocators.driverName, "");
            CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
            CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
            CloudSqlPostgreSqlActions.enterTableName(E2ETestUtils.pluginProp("cloudPSQLTableName"));
        } else {
            Assert.fail("Invalid cLoudSqlPsql Mandatory Field : " + property);
        }
    }

    @Then("Validate mandatory property error for {string}")
    public void validateMandatoryPropertyErrorFor(String property) {
        CdfStudioActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton, 5L);
        E2ETestUtils.validateMandatoryPropertyError(property);
    }

    @Then("Enter Reference Name & Database Name with valid test data")
    public void enterReferenceNameAndDatabaseNameWithValidTestData() throws InterruptedException, IOException {
        CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameValid"));
        CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
    }

    @Then("Enter Table Name {string} and Connection Name")
    public void enterTableNameInTableField(String tableName) throws IOException {
        CloudSqlPostgreSqlActions.enterTableName(E2ETestUtils.pluginProp(tableName));
        CloudSqlPostgreSqlActions.clickPrivateInstance();
        CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
    }

    @Then("Enter Driver Name with Invalid value")
    public void enterDriverNameDefaultValue() throws IOException {
        CloudSqlPostgreSqlActions.enterDriverName(E2ETestUtils.pluginProp("cloudPSQLDriverNameInvalid"));
        CloudSqlPostgreSqlActions.clickValidateButton();
    }

    @Then("Verify Driver Name field with Invalid value entered")
    public void verifyDriverNameFieldWithInvalidValueEntered() {
        Assert.assertTrue(CloudSqlPostgreSqlLocators.driverNameError.isDisplayed());
    }

    @Then("Close the CloudSQLPostGreSQL Properties")
    public void closeTheCloudSQLPostGreSQLProperties() {
        CloudSqlPostgreSqlActions.closeButton();
    }

    @Then("Enter Connection Name and Import Query {string}")
    public void enterConnectionImportField(String query) throws IOException, InterruptedException {
        CloudSqlPostgreSqlActions.clickPrivateInstance();
        CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(query));
    }

    @Then("Enter Reference Name & Connection Name with incorrect values and import query {string}")
    public void enterReferenceNameConnectionNameWithIncorrectValuesAndImportQuery(String query)
            throws IOException, InterruptedException {
        CloudSqlPostgreSqlActions.clickCloudSqlPostgreSqlProperties();
        CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameInvalid"));
        CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
        CloudSqlPostgreSqlActions.enterConnectionName(E2ETestUtils.pluginProp("cloudPSQLConnectionNameInvalid"));
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(query));
        SeleniumHelper.waitAndClick(CloudSqlPostgreSqlLocators.validateButton, 50);
    }

    @Then("Enter Reference Name and Public Connection Name with incorrect values and table {string}")
    public void enterReferenceNameAndPublicConnectionNameWithIncorrectValuesAndTable(String tableName) {
        CloudSqlPostgreSqlActions.clickCloudSqlPostgreSqlProperties();
        CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameInvalid"));
        CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
        CloudSqlPostgreSqlActions.enterTableName(E2ETestUtils.pluginProp(tableName));
        CloudSqlPostgreSqlActions.enterConnectionName(E2ETestUtils.pluginProp("cloudPSQLConnectionNameInvalid"));
        SeleniumHelper.waitAndClick(CloudSqlPostgreSqlLocators.validateButton, 50);
    }
    @Then("Enter Reference Name and private Connection Name with incorrect values and import query {string}")
    public void enterReferenceNameAndPrivateConnectionNameWithIncorrectValuesAndImportQuery(String query)
            throws IOException, InterruptedException {
        CloudSqlPostgreSqlActions.clickCloudSqlPostgreSqlProperties();
        CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameInvalid"));
        CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
        CloudSqlPostgreSqlActions.clickPrivateInstance();
        CloudSqlPostgreSqlActions.enterConnectionName(E2ETestUtils.pluginProp("cloudPSQLConnectionNameInvalid"));
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(query));
        SeleniumHelper.waitAndClick(CloudSqlPostgreSqlLocators.validateButton, 50);
    }

    @Then("Enter Reference Name and Private Connection Name with incorrect values and table {string}")
    public void enterReferenceNameAndPrivateConnectionNameWithIncorrectValuesAndTable(String tableName) {
        CloudSqlPostgreSqlActions.clickCloudSqlPostgreSqlProperties();
        CloudSqlPostgreSqlActions.enterReferenceName(E2ETestUtils.pluginProp("cloudPSQLReferenceNameInvalid"));
        CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp("cloudPSQLDbName"));
        CloudSqlPostgreSqlActions.enterTableName(E2ETestUtils.pluginProp(tableName));
        CloudSqlPostgreSqlActions.clickPrivateInstance();
        CloudSqlPostgreSqlActions.enterConnectionName(E2ETestUtils.pluginProp("cloudPSQLConnectionNameInvalid"));
        SeleniumHelper.waitAndClick(CloudSqlPostgreSqlLocators.validateButton, 50);
    }

    @Then("Open cloudSQLPostgreSQL Properties")
    public void openCloudSQLPostgreSQLProperties() {
        CloudSqlPostgreSqlActions.clickCloudSqlPostgreSqlProperties();
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryToGetAllValues
      (String database, String importQuery) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
    }

    public void enterTheCloudSQLPostgreSQLPropertiesForDatabase(String database) throws IOException {
        CloudSqlPostgreSqlActions.enterReferenceName("cloudSQLPostgreSQL" + UUID.randomUUID().toString());
        CloudSqlPostgreSqlActions.enterDatabaseName(E2ETestUtils.pluginProp(database));
        CloudSqlPostgreSqlActions.clickPrivateInstance();
        CloudSqlPostgreSqlActions.clickPrivateInstance();
        CloudSqlPostgreSqlActions.enterUserName(System.getenv("Cloud_Psql_User_Name"));
        CloudSqlPostgreSqlActions.enterPassword(System.getenv("Cloud_Psql_Password"));
        CloudSqlPostgreSqlActions.enterConnectionName(System.getenv("Cloud_Psql_ConnectionName"));
    }

    @Then("Capture output schema")
    public void captureOutputSchema() {
        CloudSqlPostgreSqlActions.getSchema();
        SeleniumHelper.waitElementIsVisible(CloudSqlPostgreSqlLocators.getSchemaLoadComplete, 10L);
        SeleniumHelper.waitElementIsVisible(CloudSqlPostgreSqlLocators.outputSchemaColumnNames.get(0), 2L);
        int index = 0;
        for (WebElement element : CloudSqlPostgreSqlLocators.outputSchemaColumnNames) {
            propertiesSchemaColumnList.add(element.getAttribute("value"));
            sourcePropertiesOutputSchema.put(element.getAttribute("value"),
                    CloudSqlPostgreSqlLocators.outputSchemaDataTypes.get(index).getAttribute("title"));
            index++;
        }
        Assert.assertTrue(propertiesSchemaColumnList.size() >= 1);
    }

    @Then("Validate cloudSQLPostgreSQL properties")
    public void validateCloudSQLPostgreSQLProperties() {
        CloudSqlPostgreSqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CloudSqlPostgreSqlLocators.validateButton);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Close the cloudSQLPostgreSQL properties")
    public void closeTheCloudSQLPostgreSQLProperties() {
        CloudSqlPostgreSqlActions.closeButton();
    }

    @Then("Open BigQuery Target Properties")
    public void openBigQueryTargetProperties() {
        CdfStudioActions.clickProperties("BigQuery");
    }

    @Then("Enter the BigQuery Target Properties for table {string}")
    public void enterTheBigQueryTargetPropertiesForTable(String tableName) throws IOException {
        CdfBigQueryPropertiesActions.enterProjectId(E2ETestUtils.pluginProp("projectId"));
        CdfBigQueryPropertiesActions.enterDatasetProjectId(E2ETestUtils.pluginProp("projectId"));
        CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID().toString());
        CdfBigQueryPropertiesActions.enterBigQueryDataset(E2ETestUtils.pluginProp("dataset"));
        CdfBigQueryPropertiesActions.enterBigQueryTable(E2ETestUtils.pluginProp(tableName));
        CdfBigQueryPropertiesActions.clickUpdateTable();
        CdfBigQueryPropertiesActions.clickTruncatableSwitch();
    }

    @Then("Validate Bigquery properties")
    public void validateBigqueryProperties() {
        CdfGcsActions.clickValidateButton();
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Close the BigQuery properties")
    public void closeTheBigQueryProperties() {
        CdfStudioActions.clickCloseButton();
    }

    @Then("Connect Source as {string} and sink as {string} to establish connection")
    public void connectSourceAsAndSinkAsToEstablishConnection(String source, String sink) {
        CdfStudioActions.connectSourceAndSink(source, sink);
    }

    @Then("Add pipeline name")
    public void addPipelineName() {
        CdfStudioActions.pipelineName();
        CdfStudioActions.pipelineNameIp("cloudSQLPostgreSQL_BQ" + UUID.randomUUID().toString());
        CdfStudioActions.pipelineSave();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.statusBanner);
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
        wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
    }

    @Then("Preview and run the pipeline")
    public void previewAndRunThePipeline() {
        SeleniumHelper.waitAndClick(CdfStudioLocators.preview, 5L);
        CdfStudioLocators.runButton.click();
    }

    @Then("Verify the preview of pipeline is {string}")
    public void verifyThePreviewOfPipelineIs(String previewStatus) {
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 180);
        wait.until(ExpectedConditions.visibilityOf(CdfStudioLocators.statusBanner));
        Assert.assertTrue(CdfStudioLocators.statusBannerText.getText().contains(previewStatus));
        if (!previewStatus.equalsIgnoreCase("failed")) {
            wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
        }
    }

    @Then("Click on PreviewData for cloudSQLPostgreSQL")
    public void clickOnPreviewDataForCloudSQLPostgreSQL() {
        CloudSqlPostgreSqlActions.clickPreviewData();
    }

    @Then("Close the Preview and deploy the pipeline")
    public void closeThePreviewAndDeployThePipeline() {
        SeleniumHelper.waitAndClick(CdfStudioLocators.closeButton, 5L);
        CdfStudioActions.previewSelect();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
        CdfStudioActions.pipelineDeploy();
    }

    @Then("Open the Logs and capture raw logs")
    public void openTheLogsAndCaptureRawLogs() {
        CdfPipelineRunAction.logsClick();
    }

    @Then("Validate records out from cloudSQLPostgreSQL is equal to records transferred in " +
      "BigQuery {string} output records")
    public void validateRecordsOutFromCloudSQLPostgreSQLIsEqualToRecordsTransferredInBigQueryOutputRecords
      (String tableName) throws IOException, InterruptedException {
        int countRecords;
        countRecords = GcpClient.countBqQuery(E2ETestUtils.pluginProp(tableName));
        Assert.assertEquals(countRecords, recordOut());
    }

    @Then("Run the Pipeline in Runtime")
    public void runThePipelineInRuntime() throws InterruptedException {
        CdfPipelineRunAction.runClick();
    }

    @Then("Wait till pipeline is in running state")
    public void waitTillPipelineIsInRunningState() throws InterruptedException {
        Boolean bool = true;
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 200);
        wait.until(ExpectedConditions.or
          (ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
           ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
    }

    @Then("Verify the pipeline status is {string}")
    public void verifyThePipelineStatusIs(String status) {
        boolean webelement = false;
        webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
        Assert.assertTrue(webelement);
    }

    @Then("Get Count of no of records transferred to BigQuery in {string}")
    public void getCountOfNoOfRecordsTransferredToBigQueryIn(String table) throws IOException, InterruptedException {
        int countRecords;
        countRecords = GcpClient.countBqQuery(E2ETestUtils.pluginProp(table));
        BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
        Assert.assertEquals(countRecords, recordOut());
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} for {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForNull
      (String database, String importQuery, String splitColumnValue) throws IOException, InterruptedException {
      enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
        CloudSqlPostgreSqlActions.enterSplitColumn(E2ETestUtils.pluginProp(splitColumnValue));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using " +
      "query {string} for between values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForBetweenValues
      (String database, String importQuery, String cloudPostgresSQLSplitColumnBetweenValue)
      throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
        CloudSqlPostgreSqlActions.enterSplitColumn(E2ETestUtils.pluginProp(cloudPostgresSQLSplitColumnBetweenValue));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} for max and min {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForMaxAndMin
      (String database, String importQuery, String splitColumnField) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
       CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
        CloudSqlPostgreSqlActions.enterSplitColumn(E2ETestUtils.pluginProp(splitColumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} " +
      "for duplicate values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForDuplicateValues
      (String database, String importQuery, String splitColumnField) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
        CloudSqlPostgreSqlActions.enterSplitColumn(E2ETestUtils.pluginProp(splitColumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} for max values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForMaxValues
      (String database, String importQuery, String splitColumnField) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
        CloudSqlPostgreSqlActions.enterSplitColumn(E2ETestUtils.pluginProp(splitColumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} for min values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForMinValues
      (String database, String importQuery, String splitColumnField) throws IOException, InterruptedException {
      enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
     CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
     CloudSqlPostgreSqlActions.enterSplitColumn(E2ETestUtils.pluginProp(splitColumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} " +
      "using query {string} for distinct values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForDistinctValues
      (String database, String importQuery, String splitColumnField) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
        CloudSqlPostgreSqlActions.enterSplitColumn(E2ETestUtils.pluginProp(splitColumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using different join queries {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingDifferentJoinQueries
      (String database, String importQuery) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
    }

    @When("Sink is GCS")
    public void sinkIsGCS() {
        CdfStudioActions.sinkGcs();
    }

    @Then("Validate OUT record count is equal to IN record count")
    public void validateOUTRecordCountIsEqualToINRecordCount() {
        Assert.assertEquals(recordOut(), recordIn());
    }

    @Then("Enter the GCS Properties")
    public void enterTheGCSProperties() throws IOException, InterruptedException {
      CdfGcsActions.gcsProperties();
        CdfGcsActions.enterReferenceName();
        CdfGcsActions.enterProjectId();
        CdfGcsActions.getGcsBucket(E2ETestUtils.pluginProp("cloudPSQLGcsBucketName"));
        CdfGcsActions.selectFormat("json");
        CdfGcsActions.clickValidateButton();
    }

    @Then("Close the GCS Properties")
    public void closeTheGCSProperties() {
        CdfGcsActions.closeButton();
    }

    @Then("Verify Preview output schema matches the outputSchema captured in properties")
    public void verifyPreviewOutputSchemaMatchesTheOutputSchemaCapturedInProperties() {
        List<String> previewSchemaColumnList = new ArrayList<>();
        for (WebElement element : CloudSqlPostgreSqlLocators.previewInputRecordColumnNames) {
            previewSchemaColumnList.add(element.getAttribute("title"));
        }
        Assert.assertTrue(previewSchemaColumnList.equals(propertiesSchemaColumnList));
        CloudSqlPostgreSqlActions.clickPreviewPropertiesTab();
        Map<String, String> previewSinkInputSchema = new HashMap<>();
        int index = 0;
        for (WebElement element : CloudSqlPostgreSqlLocators.inputSchemaColumnNames) {
            previewSinkInputSchema.put(element.getAttribute("value"),
                    CloudSqlPostgreSqlLocators.inputSchemaDataTypes.get(index).getAttribute("title"));
            index++;
        }
        Assert.assertTrue(previewSinkInputSchema.equals(sourcePropertiesOutputSchema));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query " +
            "{string} for max values {string} with bounding query {string} and {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForMaxValuesWithBoundingQueryAnd
            (String database, String importQuery, String splitColumnField, String boundingQuery, String splitValue)
            throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CloudSqlPostgreSqlActions.enterImportQuery(E2ETestUtils.pluginProp(importQuery));
        CloudSqlPostgreSqlActions.enterSplitColumn(E2ETestUtils.pluginProp(splitColumnField));
        CloudSqlPostgreSqlActions.enterBoundingQuery(E2ETestUtils.pluginProp(boundingQuery));
        CloudSqlPostgreSqlActions.replaceSplitValue(E2ETestUtils.pluginProp(splitValue));
    }

    @Then("Enter Driver Name with Invalid value for Driver name field {string}")
    public void enterDriverNameWithInvalidValueForDriverNameField(String driverName) {
        CloudSqlPostgreSqlActions.enterDefaultDriver(E2ETestUtils.pluginProp(driverName));
    }

    @Then("Verify invalid Driver name error message is displayed for Driver {string}")
    public void verifyInvalidDriverNameErrorMessageIsDisplayedForDriver(String driverName) {
       CloudSqlPostgreSqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CloudSqlPostgreSqlLocators.validateButton);
        String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_INVALID_DRIVER_NAME)
                .replaceAll("DRIVER_NAME", E2ETestUtils.pluginProp(driverName));
        String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("jdbcPluginName").getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
        String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("jdbcPluginName"));
        String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
        Assert.assertEquals(expectedColor, actualColor);
    }

    @Then("Verify plugin validation fails with error")
    public void verifyPluginValidationFailsWithError() {
        CdfStudioActions.clickValidateButton();
            SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pluginValidationErrorMsg, 10L);
            String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_ERROR_FOUND_VALIDATION);
            String actualErrorMessage = CdfStudioLocators.pluginValidationErrorMsg.getText();
            Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
        }

    @Then("Click on PreviewData for BigQuery")
    public void clickOnPreviewDataForBigQuery() {
        CdfBigQueryPropertiesActions.clickPreviewData();
    }

    @Then("Click on PreviewData for GCS")
    public void clickOnPreviewDataForGCS() {
    CloudSqlPostgreSqlActions.clickPluginPreviewData("GCS");
    }
}

