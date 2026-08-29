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
package io.cdap.plugin.cloudsqlmysql.actions;

import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.cloudsqlmysql.locators.CdfCloudSqlMySqlLocators;
import io.cdap.plugin.utils.E2ETestUtils;
import org.junit.Assert;
import org.openqa.selenium.WebElement;

import java.io.IOException;

/**
 * cloudSqlMySql connector related Step Actions.
 */

public class CdfCloudSqlMySqlActions {

    static {
        SeleniumHelper.getPropertiesLocators(CdfCloudSqlMySqlLocators.class);
        SeleniumHelper.getPropertiesLocators(CdfBigQueryPropertiesLocators.class);
        SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
    }

    public static void clickActionButton() {
        SeleniumHelper.waitAndClick(CdfCloudSqlMySqlLocators.clickAction);
    }

    public static void enterReferenceName(String reference) {
        CdfCloudSqlMySqlLocators.referenceName.sendKeys(reference);
    }

    public static void enterDriverName(String driver) {
        CdfCloudSqlMySqlLocators.driverName.sendKeys(driver);
    }

    public static void enterDefaultDriver(String driverNameValid) {
        SeleniumHelper.replaceElementValue(CdfCloudSqlMySqlLocators.driverName, driverNameValid);
    }

    public static void enterDatabaseName(String database) {
        CdfCloudSqlMySqlLocators.database.sendKeys(database);
    }

    public static void enterUserName(String username) {
        CdfCloudSqlMySqlLocators.username.sendKeys(username);
    }

    public static void enterPassword(String password) {
        CdfCloudSqlMySqlLocators.password.sendKeys(password);
    }

    public static void enterConnectionName(String connection) {
        CdfCloudSqlMySqlLocators.connectionName.sendKeys(connection);
    }

    public static void enterSplitColumn(String splitColumn) {
        CdfCloudSqlMySqlLocators.splitColumn.sendKeys(splitColumn);
    }

    public static void enterNumberOfSplits(String numberOfSplits) {
        SeleniumHelper.replaceElementValue(CdfCloudSqlMySqlLocators.numberOfSplits, numberOfSplits);
    }

    public static void enterImportQuery(String query) throws IOException, InterruptedException {
        CdfCloudSqlMySqlLocators.importQuery.sendKeys(query);
    }

    public static void enterBoundingQuery(String boundingQuery) throws IOException, InterruptedException {
        CdfCloudSqlMySqlLocators.boundingQuery.sendKeys(boundingQuery);
    }

    public static void enterTableName(String table) {
        CdfCloudSqlMySqlLocators.sqlTableName.sendKeys(table);
    }

    public static void enterConnectionTimeout(String connectionTimeout) {
        SeleniumHelper.replaceElementValue(CdfCloudSqlMySqlLocators.connectionTimeout, connectionTimeout);
    }
    public static void clickDuplicateButton() {
        CdfCloudSqlMySqlLocators.actionDuplicateButton.click();
    }

    public static void closeButton() {
        CdfCloudSqlMySqlLocators.closeButton.click();
    }

    public static void clickCloudSqlMySqlProperties() {
        CdfCloudSqlMySqlLocators.cloudSqlMySqlProperties.click();
    }

    public static void clickValidateButton() {
        CdfCloudSqlMySqlLocators.validateButton.click();
    }

    public static void getSchema() {
        CdfCloudSqlMySqlLocators.getSchemaButton.click();
    }

    public static void addComment(String comment) throws IOException {
        CdfCloudSqlMySqlLocators.addComment.sendKeys(comment);
    }

    public static void addCommentSink(String sinkComment) throws IOException {
        CdfCloudSqlMySqlLocators.addCommentSink.sendKeys(sinkComment);
    }

    public static void updateSourceComment(String updateComment) throws IOException {
        CdfCloudSqlMySqlLocators.addComment.sendKeys(updateComment);
    }

    public static void updateSinkComment(String updateComment) throws IOException {
        CdfCloudSqlMySqlLocators.addCommentSink.sendKeys(updateComment);
    }

    public static void clickComment() {
        CdfCloudSqlMySqlLocators.clickComment.click();
    }

    public static void clickPrivateInstance() {
        CdfCloudSqlMySqlLocators.instanceType.click();
    }

    public static void saveComment() throws IOException {
        CdfCloudSqlMySqlLocators.saveComment.click();
    }

    public static void editComment() {
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlMySqlLocators.editComment, 1);
        CdfCloudSqlMySqlLocators.editComment.click();
    }

    public static void clickEdit() {
        CdfCloudSqlMySqlLocators.clickEdit.click();
    }

    public static void clickDelete() {
        CdfCloudSqlMySqlLocators.clickDelete.click();
    }

    public static void validateComment(String expected, String actual) throws IOException {
        Assert.assertEquals(expected, actual);
    }

    public static void selectCloudSQLMySQL() throws InterruptedException {
        SeleniumHelper.waitAndClick(CdfCloudSqlMySqlLocators.cloudSqlMysqlSource);
    }

    public static void sinkCloudSQLMySQL() {
        CdfCloudSqlMySqlLocators.sink.click();
        SeleniumHelper.waitAndClick(CdfCloudSqlMySqlLocators.cloudSqlMysqlSink);
    }

    public static void selectBigQuerySource() throws InterruptedException {
        SeleniumHelper.waitAndClick(CdfCloudSqlMySqlLocators.selectBigQuerySource);
    }

    public static void clickCloudSqlPreviewData() {
        CdfCloudSqlMySqlLocators.previewData.click();
    }

    public static void clickPreviewCloseButton() {
        SeleniumHelper.waitAndClick(CdfCloudSqlMySqlLocators.previewClose);
    }
}
