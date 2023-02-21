/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.oracle.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.pages.locators.CdfSchemaLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.OracleClient;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import org.junit.Before;
import org.openqa.selenium.WebElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stepsdesign.BeforeActions;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Oracle Plugin related step design.
 */
public class Oracle implements CdfHelper {

  List<String> propertiesSchemaColumnList = new ArrayList<>();
  Map<String, String> sourcePropertiesOutputSchema = new HashMap<>();

  //-----------
  @Then("Click on preview data for Oracle sink")
  public void clickOnPreviewDataForOracleSink() {
    openSinkPluginPreviewData("Oracle");
  }

  @Then("Get count of no of records transferred to target Oracle Table")
  public void getCountOfNoOfRecordsTransferredToTargetOracleTable() throws SQLException, ClassNotFoundException {
    int countRecords = OracleClient.countRecord(PluginPropertyUtils.pluginProp("targetTable"),
                                                PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("**********No of Records Transferred******************: " + countRecords);
    Assert.assertEquals("Number of records transferred should be equal to records out ",
                        countRecords, recordOut());
  }

  @Then("Validate records transferred to target table is equal to number of records from source table")
  public void validateRecordsTransferredToTargetTableIsEqualToNumberOfRecordsFromSourceTable()
    throws SQLException, ClassNotFoundException {
    int countRecordsTarget = OracleClient.countRecord(PluginPropertyUtils.pluginProp("targetTable"),
                                                      PluginPropertyUtils.pluginProp("schema"));
    int countRecordsSource = OracleClient.countRecord(PluginPropertyUtils.pluginProp("sourceTable"),
                                                      PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("Number of records transferred:" + countRecordsSource);
    Assert.assertEquals(countRecordsSource, countRecordsTarget);
  }

  @Then("Validate the values of records transferred to target table is equal to the values from source table")
  public void validateTheValuesOfRecordsTransferredToTargetTableIsEqualToTheValuesFromSourceTable() throws
    SQLException, ClassNotFoundException {
    int validateValues = OracleClient.validateRecordValues(PluginPropertyUtils.pluginProp("schema"),
                                                           PluginPropertyUtils.pluginProp("sourceTable"),
                                                           PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("Number of records with different data: " + validateValues);
    Assert.assertEquals("Value of records transferred to the target table should be equal to the value " +
                          "of the records in the source table", validateValues, 0);
  }

  //---------------------- Testing in here

  @Then("capture the generated Output Schema.")
  public void getOutputSchema() {
    propertiesSchemaColumnList = CdfPluginPropertiesActions.getListOfFieldsFromOutputSchema();
    sourcePropertiesOutputSchema = CdfPluginPropertiesActions.getOutputSchema();
  }
  @Then("verify preview output schema matches the outputSchema captured in properties")
  public void verifyPreviewOutputSchemaMatchesTheOutputSchemaCapturedInProperties() {
    BeforeActions.scenario.write("This is the Output Schema we got after validating the plugin." +
                                   sourcePropertiesOutputSchema.keySet());

    CdfPluginPropertiesActions
      .verifyInputRecordsTableColumnsUnderPreviewTabMatchesInputSchemaFields(propertiesSchemaColumnList);
    CdfPluginPropertiesActions.clickOnTab(ConstantsUtil.PROPERTIES_TAB);
    verifyInputSchemaMatchesOutputSchemaInLocal(sourcePropertiesOutputSchema);
  }

  public static void verifyInputSchemaMatchesOutputSchemaInLocal(Map<String, String> outputSchema) {
    Map<String, String> actualInputSchema = new HashMap<>();
    int index = 0;
    for (WebElement element : CdfSchemaLocators.inputSchemaColumnNames) {
      actualInputSchema.put(element.getAttribute("value"),
                            CdfSchemaLocators.inputSchemaDataTypes.get(index).getAttribute("title"));
      index++;
    }

    BeforeActions.scenario.write("This is the Output Schema we got from preview." +
                                   actualInputSchema.keySet());

    BeforeActions.scenario.write("Actual Input Schema: ");
    for (String key : actualInputSchema.keySet()) {
      BeforeActions.scenario.write(key + " " + actualInputSchema.get(key));
    }

    BeforeActions.scenario.write("Expected Output schema: ");
    for (String key : outputSchema.keySet()) {
      BeforeActions.scenario.write(key + " " + outputSchema.get(key));
    }

    Assert.assertTrue("Schema should match", actualInputSchema.equals(outputSchema));
  }
}
