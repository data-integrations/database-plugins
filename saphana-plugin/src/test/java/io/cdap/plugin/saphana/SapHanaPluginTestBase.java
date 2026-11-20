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
import com.google.common.collect.Sets;
import com.sap.db.jdbc.Driver;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.batch.DatabasePluginTestBase;
import io.cdap.plugin.db.batch.sink.ETLDBOutputFormat;
import io.cdap.plugin.db.batch.source.DataDrivenETLDBInputFormat;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Map;


public abstract class SapHanaPluginTestBase extends DatabasePluginTestBase {


    protected static final String JDBC_DRIVER_NAME = "sap";
    protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
    protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
    protected static final Map<String, String> BASE_PROPS = ImmutableMap.<String, String>builder()
            .put(ConnectionConfig.HOST, System.getProperty("saphana.host", "localhost"))
            .put(ConnectionConfig.PORT, System.getProperty("saphana.port", "39017"))
            .put(ConnectionConfig.DATABASE, System.getProperty("sapahana.database", "SYSTEMDB"))
            .put(ConnectionConfig.USER, System.getProperty("sapahana.user", "SYSTEM"))
            .put(ConnectionConfig.PASSWORD, System.getProperty("sapahana.password", "SAPhxe123"))
            .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
            .build();
    private static String connectionUrl;
    private static Boolean setupCompleted = false;


    @BeforeClass
    public static void setupTest() throws Exception {
        if (setupCompleted) {
            return;
        }
        System.out.println("Setting up batch artifacts");
        setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
        System.out.println("Adding plugin artifact");
        addPluginArtifact(NamespaceId.DEFAULT.artifact(SapHanaConstants.PLUGIN_NAME, "1.0.0"),
                DATAPIPELINE_ARTIFACT_ID, SapHanaSource.class, DBRecord.class, ETLDBOutputFormat.class,
                DataDrivenETLDBInputFormat.class, SapHanaAction.class, SapHanaPostAction.class);

        PluginClass sapHanaDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME,
                "SapHana driver class", Driver.class.getName(), null, Collections.emptyMap());
        addPluginArtifact(NamespaceId.DEFAULT.artifact("saphana-jdbc-connector", "1.0.0"), DATAPIPELINE_ARTIFACT_ID,
                Sets.newHashSet(sapHanaDriver), Driver.class);

        connectionUrl = "jdbc:sap://" + BASE_PROPS.get(ConnectionConfig.HOST) + ":" +
                BASE_PROPS.get(ConnectionConfig.PORT) + "/" + BASE_PROPS.get(ConnectionConfig.DATABASE);
        setupCompleted = true;
    }


    protected static Connection createConnection() throws Exception {
        Class.forName(Driver.class.getCanonicalName());
        return DriverManager.getConnection(connectionUrl, BASE_PROPS.get(ConnectionConfig.USER),
                BASE_PROPS.get(ConnectionConfig.PASSWORD));
    }


}

