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

package io.cdap.plugin.mysql.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.MysqlClient;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.sql.SQLException;

/**
 *  MYSQL Plugin related step design.
 */
public class Mysql implements CdfHelper {

  @Then("Click on preview data for MySQL sink")
  public void clickOnPreviewDataForMysqlSink() {
    openSinkPluginPreviewData("Mysql");
  }


  @Then("Get count of no of records transferred to target MySQL Table")
  public void getCountOfNoOfRecordsTransferredToTargetMysqlTable() throws SQLException, ClassNotFoundException {
    int countRecords = MysqlClient.countRecord(PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertEquals("Number of records transferred should be equal to records out ",
                        countRecords, recordOut());
  }

  @Then("Validate records transferred to target table is equal to number of records from source table")
  public void validateRecordsTransferredToTargetTableIsEqualToNumberOfRecordsFromSourceTable()
    throws SQLException, ClassNotFoundException {
    int countRecordsTarget = MysqlClient.countRecord(PluginPropertyUtils.pluginProp("targetTable"));
    int countRecordsSource = MysqlClient.countRecord(PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("Number of records transferred:" + countRecordsSource);
    Assert.assertEquals(countRecordsSource, countRecordsTarget);
  }
}
