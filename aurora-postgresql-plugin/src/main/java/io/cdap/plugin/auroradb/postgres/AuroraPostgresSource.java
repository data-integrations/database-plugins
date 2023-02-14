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

package io.cdap.plugin.auroradb.postgres;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.db.batch.config.DBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from Aurora DB PostgreSQL type cluster.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(AuroraPostgresConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class AuroraPostgresSource extends AbstractDBSource<AuroraPostgresSource.AuroraPostgresSourceConfig> {

  private final AuroraPostgresSourceConfig auroraPostgresSourceConfig;

  public AuroraPostgresSource(AuroraPostgresSourceConfig auroraPostgresSourceConfig) {
    super(auroraPostgresSourceConfig);
    this.auroraPostgresSourceConfig = auroraPostgresSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return String.format(AuroraPostgresConstants.AURORA_POSTGRES_CONNECTION_STRING_FORMAT,
                         auroraPostgresSourceConfig.host, auroraPostgresSourceConfig.port,
                         auroraPostgresSourceConfig.database);
  }

  @Override
  protected List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    return new AuroraPostgresSchemaReader().getSchemaFields(resultSet);
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return AuroraPostgresDBRecord.class;
  }

  /**
   * Aurora DB PostgreSQL source config.
   */
  public static class AuroraPostgresSourceConfig extends DBSpecificSourceConfig {

    @Name(AuroraPostgresConstants.CONNECTION_TIMEOUT)
    @Description(AuroraPostgresConstants.CONNECTION_TIMEOUT_DESCRIPTION)
    @Nullable
    public Integer connectionTimeout;

    @Override
    public String getConnectionString() {
      return String.format(AuroraPostgresConstants.AURORA_POSTGRES_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      if (connectionTimeout != null) {
        return ImmutableMap.of(AuroraPostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
      } else {
        return ImmutableMap.of();
      }
    }
  }
}
