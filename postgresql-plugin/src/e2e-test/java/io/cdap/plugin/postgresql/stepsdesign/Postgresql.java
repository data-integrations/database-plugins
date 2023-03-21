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

package io.cdap.plugin.postgresql.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.PostgresqlClient;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.sql.SQLException;

/**
 *  POSTGRESQL Plugin related step design.
 */
public class Postgresql implements CdfHelper {

  @Then("Click on preview data for PostgreSQL sink")
  public void clickOnPreviewDataForPostgresqlSink() {
    openSinkPluginPreviewData("Postgres");
  }

  @Then("Validate the values of records transferred to target table is equal to the values from source table")
  public void validateTheValuesOfRecordsTransferredToTargetTableIsEqualToTheValuesFromSourceTable() throws
    SQLException, ClassNotFoundException {
    int countRecords = PostgresqlClient.countRecord(PluginPropertyUtils.pluginProp("targetTable"),
                                                    PluginPropertyUtils.pluginProp("schema"));
    Assert.assertEquals("Number of records transferred should be equal to records out ",
                        countRecords, recordOut());
    BeforeActions.scenario.write(" ******** Number of records transferred ********:" + countRecords);
    boolean recordsMatched = PostgresqlClient.validateRecordValues(
      PluginPropertyUtils.pluginProp("sourceTable"),
      PluginPropertyUtils.pluginProp("targetTable"),
      PluginPropertyUtils.pluginProp("schema"));
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }
}
