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
package io.cdap.plugin.cloudsqlpostgresql.actions;

import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.cloudsqlpostgresql.locators.CloudSqlPostgreSqlLocators;
import org.openqa.selenium.By;

import java.io.IOException;

/**
 * cloudSqlPostgreSql connector related Step Actions.
 */

public class CloudSqlPostgreSqlActions {

    static {
        SeleniumHelper.getPropertiesLocators(CloudSqlPostgreSqlLocators.class);
        SeleniumHelper.getPropertiesLocators(CdfBigQueryPropertiesLocators.class);
    }

    public static void selectCloudSQLPostgreSQLSource() throws InterruptedException {
        SeleniumHelper.waitAndClick(CloudSqlPostgreSqlLocators.cloudSqlPSqlSource);
    }

    public static void selectCloudSQLPostgreSQLSink() throws InterruptedException {
        CloudSqlPostgreSqlLocators.sink.click();
        SeleniumHelper.waitAndClick(CloudSqlPostgreSqlLocators.cloudSqlPSqlSink);
    }

    public static void clickCloudSqlPostgreSqlProperties() {
        CloudSqlPostgreSqlLocators.cloudSqlPSqlProperties.click();
    }

    public static void clickValidateButton() {
        CloudSqlPostgreSqlLocators.validateButton.click();
    }

    public static void enterReferenceName(String reference) {
        CloudSqlPostgreSqlLocators.referenceName.sendKeys(reference);
    }

    public static void enterDriverName(String driver) {
        CloudSqlPostgreSqlLocators.driverName.sendKeys(driver);
    }

    public static void enterDefaultDriver(String driverNameValid) {
        SeleniumHelper.replaceElementValue(CloudSqlPostgreSqlLocators.driverName, driverNameValid);
    }

    public static void enterDatabaseName(String database) {
        CloudSqlPostgreSqlLocators.database.sendKeys(database);
    }

    public static void enterUserName(String username) {
        CloudSqlPostgreSqlLocators.username.sendKeys(username);
    }

    public static void enterPassword(String password) {
        CloudSqlPostgreSqlLocators.password.sendKeys(password);
    }

    public static void enterConnectionName(String connection) {
        CloudSqlPostgreSqlLocators.connectionName.sendKeys(connection);
    }

    public static void closeButton() {
        CloudSqlPostgreSqlLocators.closeButton.click();
    }

    public static void enterSplitColumn(String splitColumn) {
        CloudSqlPostgreSqlLocators.splitColumn.sendKeys(splitColumn);
    }

    public static void enterNumberOfSplits(String numberOfSplits) {
        SeleniumHelper.replaceElementValue(CloudSqlPostgreSqlLocators.numberOfSplits, numberOfSplits);
    }

    public static void replaceTableValue(String tableName) {
        SeleniumHelper.replaceElementValue(CloudSqlPostgreSqlLocators.sqlTableName, tableName);
    }

    public static void enterImportQuery(String query) throws IOException, InterruptedException {
        CloudSqlPostgreSqlLocators.importQuery.sendKeys(query);
        SeleniumHelper.waitElementIsVisible(CloudSqlPostgreSqlLocators.getSchemaButton, 30);
    }

    public static void enterBoundingQuery(String query) throws IOException, InterruptedException {
        CloudSqlPostgreSqlLocators.boundingQuery.sendKeys(query);
    }

    public static void enterTableName(String table) {
        CloudSqlPostgreSqlLocators.sqlTableName.sendKeys(table);
    }

    public static void enterConnectionTimeout(String connectionTimeout) {
        SeleniumHelper.replaceElementValue(CloudSqlPostgreSqlLocators.connectionTimeout, connectionTimeout);
    }

    public static void clickPrivateInstance() {
        CloudSqlPostgreSqlLocators.instanceType.click();
    }

    public static void getSchema() {
        CloudSqlPostgreSqlLocators.getSchemaButton.click();
    }

    public static void clickPreviewData() {
        SeleniumHelper.waitElementIsVisible(CloudSqlPostgreSqlLocators.previewData);
        CloudSqlPostgreSqlLocators.previewData.click();
    }

    public static void replaceSplitValue(String numberOfSplits) throws IOException {
        SeleniumHelper.replaceElementValue(CloudSqlPostgreSqlLocators.numberOfSplits, numberOfSplits);
        }

    public static void clickPreviewPropertiesTab() {
        CloudSqlPostgreSqlLocators.previewPropertiesTab.click();
    }

    public static void clickPluginPreviewData(String plugin) {
        SeleniumDriver.getDriver().findElement(
                By.xpath("//*[contains(@data-cy,'" + plugin + "') and contains(@data-cy,'-preview-data-btn') " +
                        "and @class='node-preview-data-btn ng-scope']")).click();
    }
}
