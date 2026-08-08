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
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class SapHanaSourceTestRun extends SapHanaPluginTestBase {

    public static final String SOURCE_TEST_TABLE_NAME = "SOURCE_TEST";

    @Before
    public void createTables() throws Exception {
        Connection conn = createConnection();
        Statement statement = conn.createStatement();
        statement.execute("CREATE TABLE " + SOURCE_TEST_TABLE_NAME + "(ID INT);");
        statement.execute("INSERT INTO " + SOURCE_TEST_TABLE_NAME + " VALUES(1)");
        statement.execute("INSERT INTO " + SOURCE_TEST_TABLE_NAME + " VALUES(2)");
        statement.execute("INSERT INTO " + SOURCE_TEST_TABLE_NAME + " VALUES(3)");
    }


    @Test
    public void testDBSource() throws Exception {
        String importQuery = "SELECT ID from " + SOURCE_TEST_TABLE_NAME + " where $CONDITIONS";
        String boundingQuery = "SELECT MIN(ID),MAX(ID) from " + SOURCE_TEST_TABLE_NAME + " where $CONDITIONS";
        String splitBy = "ID";

        ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
                .putAll(BASE_PROPS)
                .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
                .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
                .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
                .put(Constants.Reference.REFERENCE_NAME, "DBTestSource").build();

        ETLPlugin sourceConfig = new ETLPlugin(
                SapHanaConstants.PLUGIN_NAME,
                BatchSource.PLUGIN_TYPE,
                sourceProps
        );

        ETLPlugin sinkConfig = MockSink.getPlugin("macroOutputTable");

        ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                DATAPIPELINE_ARTIFACT, "testDBMacro");
        runETLOnce(appManager, ImmutableMap.of("logical.start.time", "0"));

        DataSetManager<Table> outputManager = getDataset("macroOutputTable");
        Assert.assertEquals(3, MockSink.readOutput(outputManager).size());
    }

    @After
    public void dropTables() throws Exception {
        Connection conn = createConnection();
        Statement statement = conn.createStatement();
        statement.execute("DROP TABLE " + SOURCE_TEST_TABLE_NAME + ";");
    }

}
