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

package io.cdap.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.config.AbstractDBSpecificSinkConfig;
import io.cdap.plugin.db.sink.AbstractDBSink;
import io.cdap.plugin.db.sink.FieldsValidator;
import io.cdap.plugin.util.DBUtils;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink support for Oracle database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(OracleConstants.PLUGIN_NAME)
@Description("Writes records to Oracle table. Each record will be written in a row in the table")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = OracleConnector.NAME)})
public class OracleSink extends AbstractDBSink<OracleSink.OracleSinkConfig> {

  private final OracleSinkConfig oracleSinkConfig;

  public OracleSink(OracleSinkConfig oracleSinkConfig) {
    super(oracleSinkConfig);
    this.oracleSinkConfig = oracleSinkConfig;
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new OracleSinkDBRecord(output, columnTypes);
  }

  @Override
  protected FieldsValidator getFieldsValidator() {
    return new OracleFieldsValidator();
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSinkSchemaReader();
  }
  @Override
  protected LineageRecorder getLineageRecorder(BatchSinkContext context) {
    String fqn = DBUtils.constructFQN("oracle",
                                      oracleSinkConfig.getConnection().getHost(),
                                      oracleSinkConfig.getConnection().getPort(),
                                      oracleSinkConfig.getConnection().getDatabase(),
                                      oracleSinkConfig.getReferenceName());
    Asset asset = Asset.builder(oracleSinkConfig.getReferenceName()).setFqn(fqn).build();
    return new LineageRecorder(context, asset);
  }


  /**
   * Oracle action configuration.
   */
  public static class OracleSinkConfig extends AbstractDBSpecificSinkConfig {

    private static final Character ESCAPE_CHAR = '"';

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private OracleConnectorConfig connection;

    @Name(OracleConstants.DEFAULT_BATCH_VALUE)
    @Description("The default batch value that triggers an execution request.")
    @Nullable
    public Integer defaultBatchValue;

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
    }

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(OracleConstants.DEFAULT_BATCH_VALUE, String.valueOf(defaultBatchValue));
    }

    @Override
    public String getTransactionIsolationLevel() {
      return connection.getTransactionIsolationLevel();
    }

    @Override
    public String getEscapedTableName() {
      return ESCAPE_CHAR + getTableName() + ESCAPE_CHAR;
    }

    @Override
    public String getEscapedDbSchemaName() {
      return ESCAPE_CHAR + getDBSchemaName() + ESCAPE_CHAR;
    }

    @Override
    protected OracleConnectorConfig getConnection() {
      return connection;
    }
  }
}
