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
package io.cdap.plugin.cloudmysql.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.CloudMySqlClient;
import io.cdap.plugin.cloudmysql.BQValidation;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;

/**
 * CloudSqlMySql Plugin related step design.
 */
public class CloudMysql implements CdfHelper {

  private String targetTable = PluginPropertyUtils.pluginProp("targetTable");

  private String bqSourceTable = PluginPropertyUtils.pluginProp("bqSourceTable");

  private String sourceTable = PluginPropertyUtils.pluginProp("sourceTable");

  private String bqTargetTable = PluginPropertyUtils.pluginProp("bqTargetTable");

  @Then("Validate the values of records transferred to target table is equal to the values from source table")
  public void validateTheValuesOfRecordsTransferredToTargetTableIsEqualToTheValuesFromSourceTable()
    throws SQLException, ClassNotFoundException {
    int countRecords = CloudMySqlClient.countRecord(targetTable);
    Assert.assertEquals("Number of records transferred should be equal to records out ",
                        countRecords, recordOut());
    BeforeActions.scenario.write(" ******** Number of records transferred ********:" + countRecords);
    boolean recordsMatched = CloudMySqlClient.validateRecordValues(
      sourceTable, targetTable);
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }

  @Then("Validate the values of records transferred to target Big Query table is equal to the values from source table")
  public void validateTheValuesOfRecordsTransferredToTargetBigQueryTableIsEqualToTheValuesFromSourceTable()
    throws InterruptedException, IOException, SQLException, ClassNotFoundException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(bqTargetTable);
    BeforeActions.scenario.write("No of Records Transferred to BigQuery:" + targetBQRecordsCount);
    Assert.assertEquals("Out records should match with target BigQuery table records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), targetBQRecordsCount);

    boolean recordsMatched = BQValidation.validateDBAndBQRecordValues(
      sourceTable, bqTargetTable);
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }

  @Then("Validate the values of records transferred to target CloudSQLMySql table is equal to the values from source " +
    "BigQuery table")
  public void validateTheValuesOfRecordsTransferredToTargetCloudSQLMySqlTableIsEqualToTheValuesFromSourceBigQueryTable()
    throws InterruptedException, IOException, SQLException, ClassNotFoundException {
    int sourceBQRecordsCount = BigQueryClient.countBqQuery(bqSourceTable);
    BeforeActions.scenario.write("No of Records from source BigQuery table:" + sourceBQRecordsCount);
    Assert.assertEquals("Out records should match with target PostgreSQL table records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), sourceBQRecordsCount);

    boolean recordsMatched = BQValidation.validateBQAndDBRecordValues(
      bqSourceTable, targetTable);
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }
}
