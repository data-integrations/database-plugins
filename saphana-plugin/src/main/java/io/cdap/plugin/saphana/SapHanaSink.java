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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.sink.AbstractDBSink;
import io.cdap.plugin.db.sink.FieldsValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * SAP HANA batch sink
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(SapHanaConstants.PLUGIN_NAME)
@Description("Writes records to a SAP HANA table. Each record will be written in a row in the table")
public class SapHanaSink extends AbstractDBSink<SapHanaSink.SapHanaSinkConfig> {

  private final SapHanaSinkConfig sapHanaSinkConfig;
  private static final Character ESCAPE_CHAR = '"';

  public SapHanaSink(SapHanaSinkConfig sapHanaSinkConfig) {
    super(sapHanaSinkConfig);
    this.sapHanaSinkConfig = sapHanaSinkConfig;
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SapHanaSchemaReader();
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new SapHanaDBRecord(output, columnTypes);
  }

  @Override
  protected void setColumnsInfo(List<Schema.Field> fields) {
    List<String> columnsList = new ArrayList<>();
    StringJoiner columnsJoiner = new StringJoiner(",");
    for (Schema.Field field : fields) {
      columnsList.add(field.getName());
      columnsJoiner.add(ESCAPE_CHAR + field.getName() + ESCAPE_CHAR);
    }

    super.columns = Collections.unmodifiableList(columnsList);
    super.dbColumns = columnsJoiner.toString();
  }

  @Override
  protected FieldsValidator getFieldsValidator() {
    return new SapHanaFieldValidator();
  }

  /**
   * SAP HANA sink configuration.
   */
  public static class SapHanaSinkConfig extends DBSpecificSinkConfig {

    @Override
    public String getConnectionString() {
      return String.format(SapHanaConstants.SAPHANA_CONNECTION_STRING_FORMAT, host, port);
    }

    @Override
    public String getEscapedTableName() {
      return ESCAPE_CHAR + tableName + ESCAPE_CHAR;
    }

  }
}
