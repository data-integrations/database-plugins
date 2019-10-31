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

package io.cdap.plugin.memsql.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.batch.sink.CommonFieldsValidator;

import java.sql.Types;

/**
 * Memsql validator for DB fields.
 */
public class MemsqlFieldsValidator extends CommonFieldsValidator {

  @Override
  public boolean isFieldCompatible(Schema.Type fieldType, Schema.LogicalType fieldLogicalType, int sqlType) {
    // In MemqSQL bool stores as tinyint
    if (fieldType == Schema.Type.BOOLEAN && sqlType == Types.TINYINT) {
      return true;
    }

    return super.isFieldCompatible(fieldType, fieldLogicalType, sqlType);
  }
}
