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

package io.cdap.plugin.amazon.redshift;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.source.AbstractDBSource;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from an Amazon Redshift database.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(RedshiftConstants.PLUGIN_NAME)
@Description(
  "Reads from a Amazon Redshift database table(s) using a configurable SQL query."
    + " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = RedshiftConnector.NAME)})
public class RedshiftSource
  extends AbstractDBSource<RedshiftSource.RedshiftSourceConfig> {

  private final RedshiftSourceConfig redshiftSourceConfig;

  public RedshiftSource(RedshiftSourceConfig redshiftSourceConfig) {
    super(redshiftSourceConfig);
    this.redshiftSourceConfig = redshiftSourceConfig;
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new RedshiftSchemaReader();
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return RedshiftDBRecord.class;
  }

  @Override
  protected String createConnectionString() {
    return String.format(
      RedshiftConstants.REDSHIFT_CONNECTION_STRING_FORMAT,
      redshiftSourceConfig.connection.getHost(),
      redshiftSourceConfig.connection.getPort(),
      redshiftSourceConfig.connection.getDatabase());
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    String fqn = DBUtils.constructFQN("redshift", redshiftSourceConfig.getConnection().getHost(),
                                      redshiftSourceConfig.getConnection().getPort(),
                                      redshiftSourceConfig.getConnection().getDatabase(),
                                      redshiftSourceConfig.getReferenceName());
    Asset.Builder assetBuilder = Asset.builder(redshiftSourceConfig.getReferenceName()).setFqn(fqn);
    return new LineageRecorder(context, assetBuilder.build());
  }

  /**
   * Redshift source config.
   */
  public static class RedshiftSourceConfig extends AbstractDBSpecificSourceConfig {

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private RedshiftConnectorConfig connection;

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return Collections.emptyMap();
    }

    @VisibleForTesting
    public RedshiftSourceConfig(@Nullable Boolean useConnection,
                                @Nullable RedshiftConnectorConfig connection) {
      this.useConnection = useConnection;
      this.connection = connection;
    }

    @Override
    public Integer getFetchSize() {
      Integer fetchSize = super.getFetchSize();
      return fetchSize == null ? Integer.parseInt(DEFAULT_FETCH_SIZE) : fetchSize;
    }

    @Override
    protected RedshiftConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }
  }
}
