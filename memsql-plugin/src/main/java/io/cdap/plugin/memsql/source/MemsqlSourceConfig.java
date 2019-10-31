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

package io.cdap.plugin.memsql.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.db.batch.config.DBSpecificSourceConfig;
import io.cdap.plugin.memsql.MemsqlConstants;
import io.cdap.plugin.memsql.MemsqlUtil;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Config class for {@link MemsqlSource}.
 */
public class MemsqlSourceConfig extends DBSpecificSourceConfig {

  @Name(MemsqlConstants.AUTO_RECONNECT)
  @Description("Should the driver try to re-establish stale and/or dead connections")
  @Nullable
  public Boolean autoReconnect;

  @Name(MemsqlConstants.USE_COMPRESSION)
  @Description("Select this option for WAN connections")
  @Nullable
  public Boolean useCompression;

  @Name(MemsqlConstants.USE_SSL)
  @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
  @Nullable
  public String useSSL;

  @Name(MemsqlConstants.USE_ANSI_QUOTES)
  @Description("Treats \" as an identifier quote character and not as a string quote character")
  @Nullable
  public Boolean useAnsiQuotes;

  @Name(MemsqlConstants.KEY_STORE)
  @Description("URL to the client certificate KeyStore (if not specified, use defaults)")
  @Nullable
  public String keyStore;

  @Name(MemsqlConstants.KEY_STORE_PASSWORD)
  @Description("Password for the client certificates KeyStore")
  @Nullable
  public String keyStorePassword;

  @Name(MemsqlConstants.TRUST_STORE)
  @Description("URL to the trusted root certificate KeyStore (if not specified, use defaults)")
  @Nullable
  public String trustStore;

  @Name(MemsqlConstants.TRUST_STORE_PASSWORD)
  @Description("Password for the trusted root certificates KeyStore")
  @Nullable
  public String trustStorePassword;

  @Override
  public String getConnectionString() {
    return MemsqlUtil.getConnectionString(host, port, database);
  }

  @Override
  public Map<String, String> getDBSpecificArguments() {
    return MemsqlUtil.composeDbSpecificArgumentsMap(autoReconnect, useCompression, useSSL,
                                                    keyStore,
                                                    keyStorePassword,
                                                    trustStore,
                                                    trustStorePassword);
  }

  @Override
  public List<String> getInitQueries() {
    return MemsqlUtil.composeDbInitQueries(useAnsiQuotes);
  }

  @Override
  protected void validateSchema(Schema actualSchema, FailureCollector collector) {
    Schema configSchema = getSchema();

    if (configSchema == null) {
      collector.addFailure("Schema should not be null or empty.", null)
        .withConfigProperty(SCHEMA);
      return;
    }

    for (Schema.Field field : configSchema.getFields()) {
      Schema.Field actualField = actualSchema.getField(field.getName());
      if (actualField == null) {
        collector.addFailure(
          String.format("Schema field '%s' is not present in actual record", field.getName()), null)
          .withOutputSchemaField(field.getName());
        continue;
      }

      Schema actualFieldSchema = actualField.getSchema().isNullable() ?
        actualField.getSchema().getNonNullable() : actualField.getSchema();
      Schema expectedFieldSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();

      // In MemqSQL bool stores as tinyint
      if (actualFieldSchema.getType().equals(Schema.Type.INT) &&
        expectedFieldSchema.getType().equals(Schema.Type.BOOLEAN)) {
        continue;
      }

      if (!actualFieldSchema.equals(expectedFieldSchema)) {
        collector.addFailure(
          String.format("Schema field '%s' has type '%s' but found '%s' in input record",
                        field.getName(), expectedFieldSchema.getType(), actualFieldSchema.getType()), null)
          .withOutputSchemaField(field.getName());
      }
    }
  }
}
