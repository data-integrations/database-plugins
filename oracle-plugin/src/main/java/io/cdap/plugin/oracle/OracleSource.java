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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.DBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from Oracle.
 */
@Plugin(type = "batchsource")
@Name(OracleConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class OracleSource extends AbstractDBSource {

  private final OracleSourceConfig oracleSourceConfig;

  public OracleSource(OracleSourceConfig oracleSourceConfig) {
    super(oracleSourceConfig);
    this.oracleSourceConfig = oracleSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(oracleSourceConfig.connectionType)) {
      return String.format(OracleConstants.ORACLE_CONNECTION_SERVICE_NAME_STRING_FORMAT,
                           oracleSourceConfig.host, oracleSourceConfig.port, oracleSourceConfig.database);
    }

    return String.format(OracleConstants.ORACLE_CONNECTION_STRING_FORMAT, oracleSourceConfig.host,
                         oracleSourceConfig.port, oracleSourceConfig.database);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSourceSchemaReader();
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return OracleSourceDBRecord.class;
  }

  /**
   * Oracle source config.
   */
  public static class OracleSourceConfig extends DBSpecificSourceConfig {
    @Name(OracleConstants.DEFAULT_BATCH_VALUE)
    @Description("The default batch value that triggers an execution request.")
    @Nullable
    public Integer defaultBatchValue;

    @Name(OracleConstants.DEFAULT_ROW_PREFETCH)
    @Description("The default number of rows to prefetch from the server.")
    @Nullable
    public Integer defaultRowPrefetch;

    @Name(OracleConstants.CONNECTION_TYPE)
    @Description("Whether to use an SID or Service Name when connecting to the database.")
    public String connectionType;

    @Override
    public String getConnectionString() {
      if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(this.connectionType)) {
        return String.format(OracleConstants.ORACLE_CONNECTION_SERVICE_NAME_STRING_FORMAT, host, port, database);
      }
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      builder.put(OracleConstants.DEFAULT_BATCH_VALUE, String.valueOf(defaultBatchValue));
      builder.put(OracleConstants.DEFAULT_ROW_PREFETCH, String.valueOf(defaultRowPrefetch));

      return builder.build();
    }
  }

  /**
   * Oracle specific schema request.
   */
  private static class OracleSchemaRequest extends GetSchemaRequest {
    @Nullable
    public String connectionType;
  }
}
