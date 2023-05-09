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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class MysqlSchemaReaderUnitTest {

    @Test
    public void validateYearTypeToStringTypeConversion() throws SQLException {
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(metadata.getColumnType(Mockito.eq(1))).thenReturn(Types.DATE);
        Mockito.when(metadata.getColumnTypeName(Mockito.eq(1))).thenReturn(MysqlSchemaReader.YEAR_TYPE_NAME);

        MysqlSchemaReader schemaReader = new MysqlSchemaReader(null);
        Schema schema = schemaReader.getSchema(metadata, 1);
        Assert.assertTrue(Schema.of(Schema.Type.INT).equals(schema));
    }
}
