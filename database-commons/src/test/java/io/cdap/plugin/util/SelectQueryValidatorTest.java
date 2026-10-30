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

package io.cdap.plugin.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SelectQueryValidator}
 */
public class SelectQueryValidatorTest {

  @Test
  public void testValidSelectQueries() {
    String[] queries = {
      "SELECT * FROM my_table",
      "select id, name from my_table where id > 10 order by name",
      "SELECT * FROM my_table WHERE $CONDITIONS",
      "SELECT * FROM my_table WHERE id > 10 AND $CONDITIONS",
      "SELECT MIN(id), MAX(id) FROM my_table",
      "SELECT * FROM \"my schema\".\"my;table\"",
      "SELECT * FROM \"pg_class\"\"; DELETE FROM t; --\"",
      "SELECT * FROM `db`.`my;table`",
      "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id",
      "SELECT * FROM (SELECT id FROM my_table) t",
      "SELECT id FROM a UNION ALL SELECT id FROM b",
      "WITH t AS (SELECT id FROM my_table) SELECT * FROM t",
      "SELECT * FROM my_table WHERE name = 'a;b' -- trailing ; comment",
      "SELECT * FROM my_table /* ; DELETE FROM t */",
      "SELECT * FROM my_table;",
      "SELECT TOP 10 * FROM dbo.my_table",
      "SELECT * FROM my_table WHERE ROWNUM <= 10",
      "SELECT * FROM my_table LIMIT 10",
    };
    for (String query : queries) {
      try {
        SelectQueryValidator.validate(query, false);
      } catch (IllegalArgumentException e) {
        Assert.fail(String.format("Query '%s' should be valid but failed with: %s", query, e.getMessage()));
      }
    }
  }

  @Test
  public void testSquareBracketIdentifiers() {
    SelectQueryValidator.validate("SELECT * FROM [dbo].[my;table] WHERE [id] > 1", true);
    try {
      SelectQueryValidator.validate("SELECT * FROM [dbo].[t]; DELETE FROM [dbo].[t]", true);
      Assert.fail("Stacked statements with square bracket identifiers should have been rejected");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testInvalidQueries() {
    String[] queries = {
      // stacked statements, as in the reported vulnerability
      "SELECT * FROM \"pg_catalog\".\"pg_class\" LIMIT 0; UPDATE t SET v = 'x' WHERE id = 1; SELECT 1",
      "SELECT * FROM my_table; DELETE FROM my_table",
      "SELECT 1; SELECT 2",
      // data modification and definition
      "UPDATE my_table SET v = 1",
      "DELETE FROM my_table",
      "INSERT INTO my_table VALUES (1)",
      "DROP TABLE my_table",
      "TRUNCATE TABLE my_table",
      "CREATE TABLE t (id INT)",
      "ALTER TABLE my_table ADD COLUMN c INT",
      "MERGE INTO t USING s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.v = s.v",
      "GRANT SELECT ON my_table TO someone",
      // session and database commands
      "SET search_path TO other",
      "USE other_db",
      "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
      // stored procedures
      "EXEC my_procedure",
      "CALL my_procedure()",
      // reads that write or lock
      "SELECT * INTO new_table FROM my_table",
      "SELECT * FROM my_table FOR UPDATE",
      "WITH d AS (DELETE FROM my_table RETURNING *) SELECT * FROM d",
      "SELECT * FROM my_table INTO OUTFILE '/tmp/out.csv'",
      "SELECT * INTO OUTFILE '/tmp/out.csv' FROM my_table",
      // a dollar quoted string (PostgreSQL) that hides a second statement
      "SELECT $$ ' $$; DELETE FROM my_table; -- '",
      // a string that hides a second statement when backslash escapes quotes (MySQL) but not otherwise
      "SELECT 'a\\''; DELETE FROM my_table; -- '",
      // a string that hides a second statement when backslash does not escape quotes (PostgreSQL)
      "SELECT 'a\\'; DELETE FROM my_table; --'",
      // empty and unparsable
      "",
      ";",
      "SELECT FROM WHERE",
    };
    for (String query : queries) {
      try {
        SelectQueryValidator.validate(query, false);
        Assert.fail(String.format("Query '%s' should have been rejected", query));
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  @Test
  public void testErrorMessages() {
    assertError("SELECT 1; DELETE FROM t", "Only a single SELECT statement is allowed, but found 2 statements.");
    assertError("DELETE FROM t", "Only a SELECT statement is allowed, but found a Delete statement.");
    assertError("SELECT * INTO t2 FROM t", "SELECT ... INTO is not allowed, the query must only read data.");
    assertError("SELECT * FROM t FOR UPDATE", "SELECT ... FOR UPDATE is not allowed, the query must only read data.");
  }

  private static void assertError(String query, String expectedMessage) {
    try {
      SelectQueryValidator.validate(query, false);
      Assert.fail(String.format("Query '%s' should have been rejected", query));
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(expectedMessage, e.getMessage());
    }
  }
}
