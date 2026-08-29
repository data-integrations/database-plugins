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
package io.cdap.plugin.cloudsqlmysql.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

import java.util.List;

/**
 * CloudSqlMySql Connector Locators.
 */

public class CdfCloudSqlMySqlLocators {

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-CloudSQLMySQL-batchsource']")
  public static WebElement cloudSqlMysqlSource;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-CloudSQLMySQL-batchsink']")
  public static WebElement cloudSqlMysqlSink;

  @FindBy(how = How.XPATH, using = "//*[text()='Sink ']")
  public static WebElement sink;

  @FindBy(how = How.XPATH, using = "//*[contains(@class,'plugin-endpoint_CloudSQL-MySQL')]")
  public static WebElement fromCloudSqlMysqlSource;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='CloudSQLMySQL-preview-data-btn']")
  public static WebElement previewData;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement previewClose;

  @FindBy(how = How.XPATH, using = "//*[contains(@class,'plugin-endpoint_BigQuery')]")
  public static WebElement fromBigQuerySource;

  @FindBy(how = How.XPATH, using = "//*[@title=\"CloudSQL MySQL\"]//following-sibling::div")
  public static WebElement toCloudSqlMysqlSink;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='referenceName' and @class='MuiInputBase-input']")
  public static WebElement referenceName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='jdbcPluginName' and @class='MuiInputBase-input']")
  public static WebElement driverName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='database' and @class='MuiInputBase-input']")
  public static WebElement database;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='importQuery']//textarea")
  public static WebElement importQuery;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='boundingQuery']//textarea")
  public static WebElement boundingQuery;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='user' and @class='MuiInputBase-input']")
  public static WebElement username;

  @FindBy(how = How.XPATH, using = "//*[@placeholder='The password to use to connect to the CloudSQL database']")
  public static WebElement password;

  @FindBy(how = How.XPATH, using = "//input [@type='radio' and @value='private']")
  public static WebElement instanceType;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='connectionName' and @class='MuiInputBase-input']")
  public static WebElement connectionName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'Split-By Field')]")
  public static WebElement splitColumnError;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'Invalid value')]")
  public static WebElement numberOfSplitError;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'Bounding Query must')]")
  public static WebElement boundingQueryError;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='splitBy' and @class='MuiInputBase-input']")
  public static WebElement splitColumn;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='numSplits' and @class='MuiInputBase-input']")
  public static WebElement numberOfSplits;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='tableName' and @class='MuiInputBase-input']")
  public static WebElement sqlTableName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='connectionTimeout' and @class='MuiInputBase-input']")
  public static WebElement connectionTimeout;

  @FindBy(how = How.XPATH, using = "//*[@class='plugin-comments-wrapper ng-scope']")
  public static WebElement clickComment;

  @FindBy(how = How.XPATH, using = "(//div[contains(@class,'MuiPaper-rounded')])[4]/div/textarea[1]")
  public static WebElement addComment;

  @FindBy(how = How.XPATH, using = "(//div[contains(@class,'MuiPaper-rounded')])[4]/div[2]/div/p")
  public static WebElement validateComment;

  @FindBy(how = How.XPATH, using = "(//div[contains(@class,'MuiPaper-rounded')])[3]/div[2]/div/p")
  public static WebElement validateSinkComment;

  @FindBy(how = How.XPATH, using = "(//*[contains(text(), 'Comment')])[2]")
  public static WebElement saveComment;

  @FindBy(how = How.XPATH, using = "(//*[contains(@class,'MuiIconButton-sizeSmall') and @tabindex='0'])")
  public static WebElement editComment;

  @FindBy(how = How.XPATH, using = "(//*[@id='menu-list-grow']//child::li)[1]")
  public static WebElement clickEdit;

  @FindBy(how = How.XPATH, using = "(//*[@id='menu-list-grow']//child::li)[2]")
  public static WebElement clickDelete;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-properties-validate-btn']")
  public static WebElement validateButton;

  @FindBy(how = How.XPATH, using = "//*[@class='text-danger']")
  public static WebElement importQueryError;

  @FindBy(how = How.XPATH, using = "//*[@title=\"CloudSQL MySQL\"]//following-sibling::div")
  public static WebElement cloudSqlMySqlProperties;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Get Schema')]")
  public static WebElement getSchemaButton;

  @FindBy(how = How.XPATH, using = "//*[@class='btn pipeline-action-btn pipeline-actions-btn']")
  public static WebElement clickAction;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Duplicate')]")
  public static WebElement actionDuplicateButton;

  @FindBy(how = How.XPATH, using = "(//*[@data-cy='plugin-properties-errors-found']")
  public static WebElement getSchemaStatus;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-BigQueryTable-batchsource']")
  public static WebElement selectBigQuerySource;

  @FindBy(how = How.XPATH, using = "//*[@placeholder='Add a comment']")
  public static WebElement addCommentSink;

  @FindBy(how = How.XPATH, using = "//*[contains(@class,'Mui-disabled Mui-disabled')]")
  public static WebElement disabledComment;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")
  public static WebElement getSchemaLoadComplete;

  @FindBy(how = How.XPATH,
          using = "//div[@data-cy='Output Schema']//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']")
  public static List<WebElement> outputSchemaColumnNames;

  @FindBy(how = How.XPATH,
          using = "//div[@data-cy='Output Schema']//div[@data-cy='schema-fields-list']//select")
  public static List<WebElement> outputSchemaDataTypes;

  @FindBy(how = How.XPATH,
          using = "//div[@data-cy='Input Schema']//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']")
  public static List<WebElement> inputSchemaColumnNames;

  @FindBy(how = How.XPATH,
          using = "//div[@data-cy='Input Schema']//div[@data-cy='schema-fields-list']//select")
  public static List<WebElement> inputSchemaDataTypes;

  @FindBy(how = How.XPATH, using = "(//h2[text()='Input Records']/parent::div/div/div/div/div)[1]//div[text()!='']")
  public static List<WebElement> previewInputRecordColumnNames;

  @FindBy(how = How.XPATH, using = "(//h2[text()='Output Records']/parent::div/div/div/div/div)[1]//div[text()!='']")
  public static List<WebElement> previewOutputRecordColumnNames;

  @FindBy(how = How.XPATH, using = "//*[@role='tablist']/li[contains(text(),'Properties')]")
  public static WebElement previewPropertiesTab;

  @FindBy(how = How.XPATH, using = "//*[contains(@data-cy,'GCS') and contains(@data-cy,'-preview-data-btn') and " +
          "@class='node-preview-data-btn ng-scope']")
  public static WebElement gcsPreviewData;

}
