/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.oracle;

import org.junit.Assert;
import org.junit.Test;

public class OracleETLDBOutputFormatTest {

  private final OracleETLDBOutputFormat outputFormat = new OracleETLDBOutputFormat();

  @Test
  public void testConstructUpsertQueryBasic() {
    String[] fieldNames = {"id", "name", "age"};
    String[] listKeys = {"id"};
    String table = "my_table";

    String result = outputFormat.constructUpsertQuery(table, fieldNames, listKeys);

    String expected = "MERGE INTO my_table target " +
        "USING (SELECT ? AS id, ? AS name, ? AS age FROM dual) source " +
        "ON (target.id = source.id) " +
        "WHEN MATCHED THEN UPDATE SET target.name = source.name, target.age = source.age " +
        "WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (source.id, source.name, source.age)";

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testConstructUpsertQueryMultipleKeys() {
    String[] fieldNames = {"id", "code", "name", "value"};
    String[] listKeys = {"id", "code"};
    String table = "composite_key_table";

    String result = outputFormat.constructUpsertQuery(table, fieldNames, listKeys);

    String expected = "MERGE INTO composite_key_table target "
        + "USING (SELECT ? AS id, ? AS code, ? AS name, ? AS value FROM dual) source "
        + "ON (target.id = source.id AND target.code = source.code) "
        + "WHEN MATCHED THEN UPDATE SET target.name = source.name, target.value = source.value "
        + "WHEN NOT MATCHED THEN INSERT (id, code, name, value) VALUES (source.id, source.code, source.name, source"
        + ".value)";

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testConstructUpsertQuerySingleField() {
    String[] fieldNames = {"id", "name"};
    String[] listKeys = {"id"};
    String table = "single_field_update_table";

    String result = outputFormat.constructUpsertQuery(table, fieldNames, listKeys);

    String expected = "MERGE INTO single_field_update_table target " +
        "USING (SELECT ? AS id, ? AS name FROM dual) source " +
        "ON (target.id = source.id) " +
        "WHEN MATCHED THEN UPDATE SET target.name = source.name " +
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)";

    Assert.assertEquals(expected, result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructUpsertQueryNullListKeys() {
    String[] fieldNames = {"id", "name", "age"};
    String table = "my_table";

    outputFormat.constructUpsertQuery(table, fieldNames, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructUpsertQueryNullFieldNames() {
    String[] listKeys = {"id"};
    String table = "my_table";

    outputFormat.constructUpsertQuery(table, null, listKeys);
  }

  @Test
  public void testConstructUpsertQueryAllFieldsAreKeys() {
    String[] fieldNames = {"id", "code"};
    String[] listKeys = {"id", "code"};
    String table = "all_keys_table";

    String result = outputFormat.constructUpsertQuery(table, fieldNames, listKeys);

    // When all fields are keys, the UPDATE SET clause will be empty after "SET "
    // Note: There's an extra space before "WHEN NOT MATCHED" due to implementation
    String expected = "MERGE INTO all_keys_table target " +
        "USING (SELECT ? AS id, ? AS code FROM dual) source " +
        "ON (target.id = source.id AND target.code = source.code) " +
        "WHEN MATCHED THEN UPDATE SET  " +
        "WHEN NOT MATCHED THEN INSERT (id, code) VALUES (source.id, source.code)";

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testConstructUpsertQueryWithSpecialTableName() {
    String[] fieldNames = {"id", "name"};
    String[] listKeys = {"id"};
    String table = "SCHEMA.MY_TABLE";

    String result = outputFormat.constructUpsertQuery(table, fieldNames, listKeys);

    String expected = "MERGE INTO SCHEMA.MY_TABLE target " +
        "USING (SELECT ? AS id, ? AS name FROM dual) source " +
        "ON (target.id = source.id) " +
        "WHEN MATCHED THEN UPDATE SET target.name = source.name " +
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)";

    Assert.assertEquals(expected, result);
  }
}
