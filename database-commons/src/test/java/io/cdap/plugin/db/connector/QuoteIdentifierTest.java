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

package io.cdap.plugin.db.connector;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Driver;

/**
 * Unit tests for {@link DBUtils#quoteIdentifier(String, char)},
 * {@link AbstractDBSpecificConnector#quoteIdentifier(String, char)}, and
 * {@link AbstractDBSpecificConnector#getTableName(String, String, String)}.
 */
public class QuoteIdentifierTest {

  @Test
  public void testPlainIdentifier() {
    Assert.assertEquals("\"table\"", AbstractDBSpecificConnector.quoteIdentifier("table", '"'));
    Assert.assertEquals("`table`", AbstractDBSpecificConnector.quoteIdentifier("table", '`'));
    Assert.assertEquals("\"table\"", DBUtils.quoteIdentifier("table", '"'));
  }

  @Test
  public void testEmptyNullAndEmbeddedNul() {
    Assert.assertEquals("\"\"", AbstractDBSpecificConnector.quoteIdentifier("", '"'));
    Assert.assertEquals("``", DBUtils.quoteIdentifier("", '`'));
    Assert.assertThrows(NullPointerException.class,
                        () -> AbstractDBSpecificConnector.quoteIdentifier(null, '"'));
    Assert.assertThrows(NullPointerException.class,
                        () -> DBUtils.quoteIdentifier(null, '"'));
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> AbstractDBSpecificConnector.quoteIdentifier("a\0b", '"'));
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> DBUtils.quoteIdentifier("a\0b", '`'));
  }

  @Test
  public void testEmbeddedQuotesAreDoubled() {
    Assert.assertEquals("\"my\"\"table\"", AbstractDBSpecificConnector.quoteIdentifier("my\"table", '"'));
    Assert.assertEquals("`my``table`", AbstractDBSpecificConnector.quoteIdentifier("my`table", '`'));
    Assert.assertEquals("\"\"\"\"\"\"", AbstractDBSpecificConnector.quoteIdentifier("\"\"", '"'));
  }

  @Test
  public void testOtherQuoteCharacterIsLeftAsIs() {
    Assert.assertEquals("\"my`table\"", AbstractDBSpecificConnector.quoteIdentifier("my`table", '"'));
    Assert.assertEquals("`my\"table`", AbstractDBSpecificConnector.quoteIdentifier("my\"table", '`'));
  }

  @Test
  public void testInjectionPayloadStaysInsideIdentifier() {
    String payload = "pg_class\" LIMIT 0; UPDATE t SET v='x' WHERE id=1; SELECT * FROM \"pg_class";
    Assert.assertEquals("\"pg_class\"\" LIMIT 0; UPDATE t SET v='x' WHERE id=1; SELECT * FROM \"\"pg_class\"",
                        AbstractDBSpecificConnector.quoteIdentifier(payload, '"'));
  }

  @Test
  public void testBaseGetTableNameTwoPartAndThreePart() {
    BaseTableNameConnector connector = new BaseTableNameConnector();
    Assert.assertEquals("\"my_db\".\"my_table\"",
                        connector.getTableName("my_db", null, "my_table"));
    Assert.assertEquals("\"db\"\"1\".\"tbl\"\"; DROP TABLE x; --\"",
                        connector.getTableName("db\"1", null, "tbl\"; DROP TABLE x; --"));
    Assert.assertEquals("\"my_db\".\"my_schema\".\"my_table\"",
                        connector.getTableName("my_db", "my_schema", "my_table"));
    Assert.assertEquals("\"db\"\"1\".\"sch\"\"2\".\"tbl\"\"3\"",
                        connector.getTableName("db\"1", "sch\"2", "tbl\"3"));
    Assert.assertThrows(NullPointerException.class,
                        () -> connector.getTableName(null, "my_schema", "my_table"));
    Assert.assertThrows(NullPointerException.class,
                        () -> connector.getTableName("my_db", "my_schema", null));
  }

  private static class BaseTableNameConnector extends AbstractDBSpecificConnector<DBRecord> {
    BaseTableNameConnector() {
      super(new AbstractDBConnectorConfig() {
        @Override
        public String getConnectionString() {
          return "jdbc:test://localhost:5432/db";
        }
      });
      this.driverClass = Driver.class;
    }

    @Override
    public boolean supportSchema() {
      return true;
    }

    @Override
    protected Class<? extends DBWritable> getDBRecordType() {
      return DBRecord.class;
    }

    @Override
    public StructuredRecord transform(LongWritable key, DBRecord val) {
      return null;
    }
  }
}
