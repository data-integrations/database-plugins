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
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.plugin.db.batch.action.QueryConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class SapHanaActionTestRun extends SapHanaPluginTestBase {

    protected static final String ACTION_TEST_TABLE_NAME = "DB_ACTION_TEST";


    @Before
    public void createTables() throws Exception {
        Connection conn = createConnection();
        Statement statement = conn.createStatement();
        statement.execute("CREATE TABLE " + ACTION_TEST_TABLE_NAME + "(ID INT);");
        statement.execute("INSERT INTO " + ACTION_TEST_TABLE_NAME + " VALUES(1)");
        statement.execute("INSERT INTO " + ACTION_TEST_TABLE_NAME + " VALUES(2)");
    }


    @Test
    public void testDBAction() throws Exception {
        ETLStage source = new ETLStage("source", MockSource.getPlugin("actionInput"));
        ETLStage sink = new ETLStage("sink", MockSink.getPlugin("actionOutput"));
        ETLStage action = new ETLStage("action", new ETLPlugin(
                SapHanaConstants.PLUGIN_NAME,
                Action.PLUGIN_TYPE,
                ImmutableMap.<String, String>builder()
                        .putAll(BASE_PROPS)
                        .put(QueryConfig.QUERY, "DELETE from " + ACTION_TEST_TABLE_NAME + " where ID=1")
                        .build(),
                null));
        ETLBatchConfig config = ETLBatchConfig.builder()
                .addStage(source)
                .addStage(sink)
                .addStage(action)
                .addConnection(sink.getName(), action.getName())
                .addConnection(source.getName(), sink.getName())
                .build();
        AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
        ApplicationId appId = NamespaceId.DEFAULT.app("actionTest");
        ApplicationManager appManager = deployApplication(appId, appRequest);
        runETLOnce(appManager, ImmutableMap.of("logical.start.time", "0"));

        Connection connection = createConnection();
        Statement statement = connection.createStatement();

        ResultSet results = statement.executeQuery("select * from " + ACTION_TEST_TABLE_NAME);
        results.next();
        int id = results.getInt("ID");
        Assert.assertEquals(id, 2);
        Assert.assertNotEquals(id, 1);
        Assert.assertFalse(results.next());

    }

    @After
    public void dropTables() throws Exception {
        Connection conn = createConnection();
        Statement statement = conn.createStatement();
        statement.execute("DROP TABLE " + ACTION_TEST_TABLE_NAME + ";");
    }

}
