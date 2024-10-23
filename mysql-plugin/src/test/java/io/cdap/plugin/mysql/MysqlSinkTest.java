/*
 * Copyright © 2024 Cask Data, Inc.
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

public class MysqlSinkTest {
  @Test
  public void testSetColumnsInfo() {
    Schema outputSchema = Schema.recordOf("output",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("insert", Schema.of(Schema.Type.STRING)));
    MysqlSink mySQLSink = new MysqlSink(new MysqlSink.MysqlSinkConfig());
    Assert.assertNotNull(outputSchema.getFields());
    mySQLSink.setColumnsInfo(outputSchema.getFields());
    Assert.assertEquals("`id`,`name`,`insert`", mySQLSink.getDbColumns());
  }
}
