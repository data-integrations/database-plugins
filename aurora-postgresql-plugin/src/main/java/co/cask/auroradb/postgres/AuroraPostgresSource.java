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

package co.cask.auroradb.postgres;

import co.cask.SchemaReader;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.db.batch.config.DBSpecificSourceConfig;
import co.cask.db.batch.source.AbstractDBSource;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from Aurora DB PostgreSQL type cluster.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(AuroraPostgresConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class AuroraPostgresSource extends AbstractDBSource {

  private final AuroraPostgresSourceConfig auroraPostgresSourceConfig;

  public AuroraPostgresSource(AuroraPostgresSourceConfig auroraPostgresSourceConfig) {
    super(auroraPostgresSourceConfig);
    this.auroraPostgresSourceConfig = auroraPostgresSourceConfig;
  }

  @Override
  protected String createConnectionString(String host, Integer port, String database) {
    return String.format(AuroraPostgresConstants.AURORA_POSTGRES_CONNECTION_STRING_FORMAT, host, port, database);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new AuroraPostgresSchemaReader();
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
