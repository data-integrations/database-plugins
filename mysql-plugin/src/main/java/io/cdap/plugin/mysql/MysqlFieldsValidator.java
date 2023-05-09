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


package io.cdap.plugin.mysql;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.sink.CommonFieldsValidator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Mysql Fields Validator class
 */
public class MysqlFieldsValidator extends CommonFieldsValidator {

    @Override
    public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {

        Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
        if (Schema.Type.INT == fieldSchema.getType()
            && Types.DATE == metadata.getColumnType(index)
            && MysqlSchemaReader.YEAR_TYPE_NAME.equalsIgnoreCase(metadata.getColumnTypeName(index))) {
            // If the column type is YEAR type then
            return true;
        }
        return super.isFieldCompatible(field, metadata, index);
    }
}
