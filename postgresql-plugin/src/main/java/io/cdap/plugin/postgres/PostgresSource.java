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

package io.cdap.plugin.postgres;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from PostgreSQL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(PostgresConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = PostgresConnector.NAME)})
public class PostgresSource extends AbstractDBSource<PostgresSource.PostgresSourceConfig> {

  private final PostgresSourceConfig postgresSourceConfig;

  public PostgresSource(PostgresSourceConfig postgresSourceConfig) {
    super(postgresSourceConfig);
    this.postgresSourceConfig = postgresSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return postgresSourceConfig.getConnectionString();
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new PostgresSchemaReader();
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return PostgresDBRecord.class;
  }

  /**
   * PosgtreSQL source config.
   */
  public static class PostgresSourceConfig extends AbstractDBSpecificSourceConfig {

    public static final String NAME_USE_CONNECTION = "useConnection";
    public static final String NAME_CONNECTION = "connection";
    public static final String DEFAULT_CONNECTION_TIMEOUT_SECONDS = "100";

    @Name(NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private PostgresConnectorConfig connection;

    @Name(PostgresConstants.CONNECTION_TIMEOUT)
    @Description("The timeout value used for socket connect operations. If connecting to the server takes longer" +
      " than this value, the connection is broken. " +
      "The timeout is specified in seconds and a value of zero means that it is disabled")
    @Nullable
    public Integer connectionTimeout;

    @Override
    public String getConnectionString() {
      return String
        .format(PostgresConstants.POSTGRES_CONNECTION_STRING_WITH_DB_FORMAT, connection.getHost(), connection.getPort(),
                connection.getDatabase());
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(PostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }

    @Override
    public Integer getFetchSize() {
      Integer fetchSize = super.getFetchSize();
      return fetchSize == null ? Integer.parseInt(DEFAULT_FETCH_SIZE) : fetchSize;
    }

    @Override
    protected AbstractDBSpecificConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }
  }
}
