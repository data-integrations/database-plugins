/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.saphana;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class SapHanaSinkTestRun extends SapHanaPluginTestBase {

    public static final String SINK_TEST_TABLE_NAME = "SINK_TEST";


    private static final Schema SCHEMA = Schema.recordOf(
            "dbRecord",
            Schema.Field.of("ID", Schema.of(Schema.Type.INT))
    );

    @Before
    public void createTables() throws Exception {
        Connection conn = createConnection();
        Statement statement = conn.createStatement();
        statement.execute("CREATE TABLE " + SINK_TEST_TABLE_NAME + "(ID INT);");
    }


    private void createInputData(String inputDatasetName) throws Exception {
        // add some data to the input table
        DataSetManager<Table> inputManager = getDataset(inputDatasetName);
        List<StructuredRecord> inputRecords = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            inputRecords.add(StructuredRecord.builder(SCHEMA)
                    .set("ID", i)
                    .build());
        }
        MockSource.writeInput(inputManager, inputRecords);
    }

    @Test
    public void testDBSinkWithExplicitInputSchema() throws Exception {
        testDBSink("testDBSinkWithExplicitInputSchema", "input-dbsinktest-explicit", true);
    }

    public void testDBSink(String appName, String inputDatasetName, boolean setInputSchema) throws Exception {
        ETLPlugin sourceConfig = (setInputSchema)
                ? MockSource.getPlugin(inputDatasetName, SCHEMA)
                : MockSource.getPlugin(inputDatasetName);

        ETLPlugin sinkConfig = getSinkConfig();

        ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, appName);
        createInputData(inputDatasetName);
        runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(0)));
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement();
             ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + SINK_TEST_TABLE_NAME)) {
            Assert.assertTrue(resultSet.next());

        }

    }

    private ETLPlugin getSinkConfig() {
        return new ETLPlugin(
                SapHanaConstants.PLUGIN_NAME,
                BatchSink.PLUGIN_TYPE,
                ImmutableMap.<String, String>builder()
                        .putAll(BASE_PROPS)
                        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, SINK_TEST_TABLE_NAME)
                        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
                        .build(),
                null);
    }


    @After
    public void dropTables() throws Exception {
        Connection conn = createConnection();
        Statement statement = conn.createStatement();
        statement.execute("DROP TABLE " + SINK_TEST_TABLE_NAME + ";");
    }

}
