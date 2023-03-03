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

package io.cdap.plugin.mssql.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.MssqlClient;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.sql.SQLException;

/**
 *  Mssql Plugin related step design.
 */
public class Mssql implements CdfHelper {

    @Then("Click on preview data for Mssql sink")
    public void clickOnPreviewDataForMssqlSink() {
        openSinkPluginPreviewData("SqlServer");
    }

    @Then("Validate records which are transferred to the target table are equal to number of records from the source table")
    public void validateRecordsWhichAreTransferredToTargetTableAreEqualToTheNumberOfRecordsTheFromSourceTable() throws
            SQLException, ClassNotFoundException {
        int countRecords = MssqlClient.countRecord(PluginPropertyUtils.pluginProp("targetTable"),
                PluginPropertyUtils.pluginProp("schema"));
        Assert.assertEquals("Number of records transferred should be equal to records out ",
                countRecords, recordOut());
        BeforeActions.scenario.write("******** Number of records transferred ********* : " + countRecords);
        boolean recordsMatched = MssqlClient.validateRecordValues(PluginPropertyUtils.pluginProp("schema"),
                PluginPropertyUtils.pluginProp("sourceTable"),
                PluginPropertyUtils.pluginProp("targetTable"));
        Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                "of the records in the source table", recordsMatched);
    }

}
