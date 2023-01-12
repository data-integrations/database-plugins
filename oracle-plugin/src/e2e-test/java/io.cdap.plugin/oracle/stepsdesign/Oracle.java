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

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.OracleClient;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *  Oracle Plugin related step design.
 */
public class Oracle implements CdfHelper {

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

}
