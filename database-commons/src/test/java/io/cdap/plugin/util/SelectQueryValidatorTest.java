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
  public void validate_singleReadOnlySelect_isAllowed() {
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
      "SELECT * FROM my_table WHERE name = 'O;Brien -- not a comment'",
      "SELECT * FROM my_table;",
      "SELECT TOP 10 * FROM dbo.my_table",
      "SELECT * FROM my_table WHERE ROWNUM <= 10",
      "SELECT * FROM my_table LIMIT 10",
      // functions whose names collide with write keywords must still be allowed
      "SELECT TRUNCATE(col, 2) FROM my_table",
      "SELECT INSERT(name, 1, 2, 'x') FROM my_table",
      "SELECT REPLACE(name, 'a', 'b') FROM my_table",
      // valid but dialect-specific syntax JSqlParser 4.9 cannot parse must still be allowed (lexical fallback)
      "SELECT * FROM t TABLESAMPLE BERNOULLI (100.0 * 10 / (SELECT COUNT(*) FROM t))",
      "SELECT * FROM t FOR READ ONLY",
      "SELECT * FROM t FOR FETCH ONLY",
      "LOCKING ROW FOR ACCESS SELECT * FROM t",
      "SELECT * FROM `my``db`.`my``table`",
      // a macro placeholder uses $ but is not a dollar-quote opener; must not be falsely rejected
      "SELECT * FROM my_table WHERE ts <= '${logicalStartTime(yyyy-MM-dd,1d)}' AND $CONDITIONS",
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
  public void validate_squareBracketIdentifiers_allowedButStackedRejected() {
    SelectQueryValidator.validate("SELECT * FROM [dbo].[my;table] WHERE [id] > 1", true);
    assertRejected("SELECT * FROM [dbo].[t]; DELETE FROM [dbo].[t]", true);
  }

  @Test
  public void validate_backslashEscapedQuote_handledPerDialect() {
    // MySQL/Databricks: a backslash escapes the quote, so this is a valid single query
    SelectQueryValidator.validate("SELECT * FROM users WHERE last_name = 'O\\'Brien' AND id > 1", false, Boolean.TRUE);
    // unknown dialect: the same text is ambiguous and must be rejected (it could hide a second statement)
    assertRejected("SELECT 'a\\'; DELETE FROM my_table; --'", false);
  }

  @Test
  public void validate_lexerMismatchConstructs_areRejected() {
    // PostgreSQL dollar-quoted strings (tagged and untagged) that hide a second statement
    assertRejected("SELECT $$ ' $$; DELETE FROM my_table; -- '", false);
    assertRejected("SELECT $tag$ ' $tag$; UPDATE my_table SET v=$$x$$ WHERE id=1; SELECT * FROM t WHERE 1=1 -- '",
                   false);
    assertRejected("SELECT $$x$$ AS c FROM my_table", false);
    // Oracle alternative-quoted literal
    assertRejected("SELECT q'['; DELETE FROM my_table; --]' FROM dual", false);
    // MySQL/MariaDB executable comments
    assertRejected("SELECT * FROM my_table /*!50000 INTO OUTFILE '/tmp/out.csv' */", false);
    assertRejected("SELECT * FROM my_table /*!50000 FOR UPDATE */", false);
    assertRejected("SELECT 1; /*! DELETE FROM my_table */", false);
    assertRejected("SELECT * FROM my_table /*M!50000 FOR UPDATE */", false);
  }

  @Test
  public void validate_stackedOrWriteStatements_areRejected() {
    assertRejected("SELECT 1; DELETE FROM t", false);
    assertRejected("SELECT * FROM t; UPDATE t SET v = 1", false);
    assertRejected("SELECT * FROM \"pg_catalog\".\"pg_class\" LIMIT 0; UPDATE t SET v = 'x'; SELECT 1", false);
    assertRejected("UPDATE my_table SET v = 1", false);
    assertRejected("DELETE FROM my_table", false);
    assertRejected("INSERT INTO my_table VALUES (1)", false);
    assertRejected("DROP TABLE my_table", false);
    assertRejected("TRUNCATE TABLE my_table", false);
    assertRejected("CREATE TABLE t (id INT)", false);
    assertRejected("ALTER TABLE my_table ADD COLUMN c INT", false);
    assertRejected("MERGE INTO t USING s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.v = s.v", false);
    assertRejected("GRANT SELECT ON my_table TO someone", false);
    assertRejected("EXEC my_procedure", false);
    assertRejected("CALL my_procedure()", false);
    assertRejected("VALUES (1)", false);
    assertRejected("TABLE my_table", false);
    assertRejected("", false);
    assertRejected(";", false);
  }

  @Test
  public void validate_intoOrForClauseIncludingSubqueries_areRejected() {
    assertRejected("SELECT * INTO new_table FROM my_table", false);
    assertRejected("SELECT * FROM my_table INTO OUTFILE '/tmp/out.csv'", false);
    assertRejected("SELECT * FROM my_table FOR UPDATE", false);
    assertRejected("SELECT * FROM my_table FOR SHARE", false);
    // nested in subqueries (regression for subquery traversal gap)
    assertRejected("SELECT * FROM (SELECT * INTO new_table FROM my_table) t", false);
    assertRejected("SELECT * FROM (SELECT * FROM my_table FOR UPDATE) t", false);
    assertRejected("SELECT * FROM my_table WHERE id IN (SELECT id FROM other_table FOR UPDATE)", false);
    assertRejected("SELECT id FROM a UNION ALL (SELECT id INTO t2 FROM b)", false);
    assertRejected("WITH d AS (DELETE FROM my_table RETURNING *) SELECT * FROM d", false);
    // FOR READ ONLY / FOR FETCH ONLY are read-only and must be allowed
    SelectQueryValidator.validate("SELECT * FROM my_table FOR READ ONLY", false);
    SelectQueryValidator.validate("SELECT * FROM my_table FOR FETCH ONLY", false);
  }

  @Test
  public void validate_keywordsHiddenByCommentsOrQuotes_areRejected() {
    // comments and quoted tokens must not glue adjacent words into one token and hide a keyword
    assertRejected("SELECT * INTO/**/new_table FROM my_table", false);
    assertRejected("SELECT * FROM my_table INTO/**/OUTFILE '/tmp/out.csv'", false);
    assertRejected("SELECT * FROM my_table FOR/**/UPDATE", false);
    assertRejected("SELECT * FROM \"my_table\"FOR\"x\"UPDATE", false);
    assertRejected("SELECT a\"alias\"INTO new_table FROM my_table", false);
    assertRejected("WITH d AS (DELETE/**/FROM my_table RETURNING *) SELECT * FROM d", false);
    // a stripped identifier before '(' must not make INTO look like a function call
    assertRejected("SELECT * INTO \"new_table\"(col1) FROM my_table", false);
    // parenthesized write / dynamic SQL that JSqlParser cannot parse must not slip through the function exemption
    assertRejected("UPDATE (SELECT * FROM my_table) SET v = 1", false);
    assertRejected("EXEC (SELECT 'DROP TABLE x')", false);
    assertRejected("EXECUTE (SELECT 'DROP TABLE x')", false);
  }

  @Test
  public void validate_doubleQuoteBackslashBreakout_isRejected() {
    // In MySQL/Databricks a double quote is a string where a backslash can escape it; the query must be rejected
    // whether or not backslash escaping is in effect, so it cannot break out under either interpretation.
    String q = "SELECT \"a\\\"\"; DELETE FROM my_table; --\" FROM my_table";
    assertRejected(q, false);
    Assert.assertThrows(IllegalArgumentException.class, () -> SelectQueryValidator.validate(q, false, Boolean.TRUE));
  }

  @Test
  public void validate_nullQuery_isRejected() {
    assertRejected(null, false);
  }

  @Test
  public void validate_invalidQueries_reportExpectedErrorMessages() {
    assertError("SELECT 1; DELETE FROM t", "Only a single SELECT statement is allowed. Found 2 statements.");
    assertError("DELETE FROM t", "The 'DELETE' keyword is not allowed. The query must only read data.");
    assertError("SELECT * INTO t2 FROM t", "The 'INTO' keyword is not allowed. The query must only read data.");
    assertError("SELECT * FROM t FOR UPDATE",
                "FOR UPDATE/SHARE locking clauses are not allowed. The query must only read data.");
    assertError("TABLE my_table", "Only a SELECT statement is allowed. No SELECT keyword was found.");
  }

  @Test
  public void validateColumnReference_singleColumnName_isAllowed() {
    String[] columns = {"id", "my_col", "t.id", "\"My Col\"", "`id`", "schema.t.id"};
    for (String col : columns) {
      SelectQueryValidator.validateColumnReference(col, "Split-By Field Name", false);
    }
    SelectQueryValidator.validateColumnReference("[my id]", "Split-By Field Name", true);
  }

  @Test
  public void validateColumnReference_nonColumnOrInjection_isRejected() {
    String[] columns = {
      "1 = 1); UPDATE audit_vault.write_marker SET value='INJECTED' WHERE id=1; SELECT * FROM \"my_table\" WHERE (1",
      "id; DROP TABLE t",
      "id)",
      "f(id)",
      "id, name",
      "id OR 1=1",
      "id /*! FOR UPDATE */",
      "\"a\\\"\"; DROP TABLE t; --\"",
      "id -- comment",
      "id /* comment */",
      "id # comment",
      "",
      null,
    };
    for (String col : columns) {
      try {
        SelectQueryValidator.validateColumnReference(col, "Split-By Field Name", false);
        Assert.fail(String.format("Column '%s' should have been rejected", col));
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  private static void assertRejected(String query, boolean squareBracketQuotes) {
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> SelectQueryValidator.validate(query, squareBracketQuotes));
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
