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


import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.sink.CommonFieldsValidator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Objects;

/**
 * SAP HANA validator for DB fields.
 */
public class SapHanaFieldValidator extends CommonFieldsValidator {

  @Override
  public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema.Type fieldType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType()
      : field.getSchema().getType();

    String colTypeName = metadata.getColumnTypeName(index);
    int columnType = metadata.getColumnType(index);
    if (SapHanaSchemaReader.STRING_MAPPED_SAPHANA_TYPES_NAMES.contains(colTypeName) ||
      SapHanaSchemaReader.STRING_MAPPED_SAPHANA_TYPES.contains(columnType)) {
      if (Objects.equals(fieldType, Schema.Type.STRING)) {
        return true;
      } else {
        LOG.error("Field '{}' was given as type '{}' but must be of type 'string' for the SAP HANA column of " +
                    "{} type.", field.getName(), fieldType, colTypeName);
        return false;
      }
    }
    return super.isFieldCompatible(field, metadata, index);
  }
}
