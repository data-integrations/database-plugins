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

package co.cask.oracle;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.db.batch.config.DBSpecificSourceConfig;
import co.cask.db.batch.source.AbstractDBSource;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

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
  protected String createConnectionString(String host, Integer port, String database) {
    return String.format(OracleConstants.ORACLE_CONNECTION_STRING_FORMAT, host, port, database);
  }

  /**
   * Oracle source config.
   */
  public static class OracleSourceConfig extends DBSpecificSourceConfig {
    @Name(OracleConstants.DEFAULT_BATCH_VALUE)
    @Description("The default batch value that triggers an execution request.")
    public Integer defaultBatchValue;

    @Name(OracleConstants.DEFAULT_ROW_PREFETCH)
    @Description("The default number of rows to prefetch from the server.")
    public Integer defaultRowPrefetch;

    @Override
    public String getConnectionString() {
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
}
