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

import net.sf.jsqlparser.parser.TokenMgrException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

/**
 * Unit tests for {@link SelectQueryValidator}.
 */
public class SelectQueryValidatorTest {

  @Test
  public void testSingleReadOnlySelectAllowed() {
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
      "SELECT TRUNCATE(col, 2) FROM my_table",
      "SELECT INSERT(name, 1, 2, 'x') FROM my_table",
      "SELECT REPLACE(name, 'a', 'b') FROM my_table",
      "SELECT CAST(name AS CHAR CHARACTER SET utf8mb4) FROM my_table",
      "SELECT * FROM t TABLESAMPLE BERNOULLI (100.0 * 10 / (SELECT COUNT(*) FROM t))",
      "SELECT * FROM t FOR READ ONLY",
      "SELECT * FROM t FOR FETCH ONLY",
      "LOCKING ROW FOR ACCESS SELECT * FROM t",
      "LOCKING TABLE my_table FOR READ SELECT * FROM my_table",
      "SELECT * FROM `my``db`.`my``table`",
      "SELECT * FROM my_table WHERE ts <= '${logicalStartTime(yyyy-MM-dd,1d)}' AND $CONDITIONS",
    };
    for (String query : queries) {
      for (SelectQueryValidator.Dialect dialect : SelectQueryValidator.Dialect.values()) {
        try {
          SelectQueryValidator.validate(query, dialect);
        } catch (IllegalArgumentException e) {
          Assert.fail(String.format("Query '%s' should be valid under %s but failed with: %s",
                                    query, dialect, e.getMessage()));
        }
      }
    }
  }

  @Test
  public void testOrdinaryLiteralsAndDollarIdentifiersAllowed() {
    String[] queries = {
      "SELECT * FROM t WHERE grade = 'Q'",
      "SELECT * FROM t WHERE code IN ('p','q')",
      "SELECT * FROM t WHERE name LIKE '%q'",
      "SELECT TO_CHAR(d, 'Q') FROM t",
      "SELECT * FROM t WHERE label = 'nq''test'",
      "SELECT * FROM t WHERE code = '$A$'",
      "SELECT * FROM t WHERE code = '$$not_dollar_quoted$$'",
      "SELECT * FROM t WHERE note = '/*!50000 NOT EXECUTABLE IN STRING */'",
      "SELECT * FROM v$session",
      "SELECT * FROM sys.dbms_aq$aq$_jms",
    };
    for (String query : queries) {
      for (SelectQueryValidator.Dialect dialect : SelectQueryValidator.Dialect.values()) {
        SelectQueryValidator.validate(query, dialect);
      }
    }
  }

  @Test
  public void testUnquotedCallDotQualifiedColumnsAndMergeJoinAllowed() {
    SelectQueryValidator.validate("SELECT call FROM calls", SelectQueryValidator.Dialect.UNKNOWN);
    SelectQueryValidator.validate(
      "SELECT c.call, c.grant, c.update, c.delete, c.set, c.into, c.lock FROM calls c",
      SelectQueryValidator.Dialect.UNKNOWN);
    SelectQueryValidator.validate(
      "SELECT \"c\".grant, \"c\".update FROM calls \"c\"",
      SelectQueryValidator.Dialect.POSTGRESQL);
    SelectQueryValidator.validate(
      "SELECT [c].grant, [c].merge FROM [calls] [c]",
      SelectQueryValidator.Dialect.TSQL);
    SelectQueryValidator.validate(
      "SELECT * FROM a INNER MERGE JOIN b ON a.id = b.id",
      SelectQueryValidator.Dialect.TSQL);
    SelectQueryValidator.validate(
      "SELECT * FROM a JOIN b ON a.id = b.id OPTION (MERGE JOIN)",
      SelectQueryValidator.Dialect.TSQL);

    // Top-level CALL, JDBC {call ...} escapes, and non-identifier dot prefixes must still be rejected.
    assertRejected("CALL my_procedure()", SelectQueryValidator.Dialect.UNKNOWN);
    assertRejected("{call my_procedure()}", SelectQueryValidator.Dialect.UNKNOWN);
    assertRejected("{?= call my_procedure()}", SelectQueryValidator.Dialect.UNKNOWN);
    assertRejected("SELECT 1. DELETE FROM t", SelectQueryValidator.Dialect.TSQL);
    assertRejected("SELECT 'a'. DELETE FROM t", SelectQueryValidator.Dialect.TSQL);
    assertRejected("SELECT 1 AS character SET IDENTITY_INSERT t ON", SelectQueryValidator.Dialect.TSQL);
  }

  @Test
  public void testSquareBracketIdentifiersHandledPerDialect() {
    SelectQueryValidator.validate("SELECT * FROM [dbo].[my;table] WHERE [id] > 1", true);
    SelectQueryValidator.validate("SELECT * FROM [dbo].[my;table] WHERE [id] > 1",
                                  SelectQueryValidator.Dialect.TSQL);
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> SelectQueryValidator.validate("SELECT * FROM [dbo].[t]; DELETE FROM [dbo].[t]", true));
    assertRejected("SELECT * FROM [dbo].[t]; DELETE FROM [dbo].[t]", SelectQueryValidator.Dialect.TSQL);
    assertRejected("SELECT * FROM [dbo].[my;table]", SelectQueryValidator.Dialect.ANSI);
  }

  @Test
  public void testBackslashLiteralsAcrossDialects() {
    String likeEscape = "SELECT * FROM t WHERE name LIKE '%\\_%' ESCAPE '\\'";
    String windowsPath = "SELECT * FROM t WHERE dir = 'C:\\data\\'";
    for (SelectQueryValidator.Dialect dialect : new SelectQueryValidator.Dialect[]{
      SelectQueryValidator.Dialect.ANSI,
      SelectQueryValidator.Dialect.TSQL,
      SelectQueryValidator.Dialect.POSTGRESQL
    }) {
      SelectQueryValidator.validate(likeEscape, dialect);
      SelectQueryValidator.validate(windowsPath, dialect);
    }

    // MySQL and Databricks use backslash escapes inside string literals.
    SelectQueryValidator.validate("SELECT * FROM users WHERE last_name = 'O\\'Brien' AND id > 1",
                                  SelectQueryValidator.Dialect.MYSQL);
    SelectQueryValidator.validate("SELECT * FROM users WHERE last_name = 'O\\'Brien' AND id > 1",
                                  SelectQueryValidator.Dialect.DATABRICKS);

    // Under UNKNOWN or MYSQL_AMBIGUOUS_BACKSLASH (when sqlMode may enable NO_BACKSLASH_ESCAPES), a backslash before a
    // single quote is ambiguous and must be rejected so it cannot break out of the literal.
    assertRejected("SELECT 'a\\'; DELETE FROM my_table; -- '", SelectQueryValidator.Dialect.UNKNOWN);
    assertRejected("SELECT 'a\\'; DELETE FROM my_table; -- '", SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH);
  }

  @Test
  public void testPostgresqlPerLiteralExtendedStringsAndNestedComments() {
    SelectQueryValidator.validate("SELECT E'O\\'Brien', 'C:\\data\\' FROM t",
                                  SelectQueryValidator.Dialect.POSTGRESQL);
    SelectQueryValidator.validate("SELECT e'line1\\nline2', 'C:\\', '; SELECT 1 -- ' FROM t",
                                  SelectQueryValidator.Dialect.POSTGRESQL);
    SelectQueryValidator.validate("SELECT 'a' /* outer /* inner ; DELETE */ still comment */ FROM t",
                                  SelectQueryValidator.Dialect.POSTGRESQL);
    SelectQueryValidator.validate("SELECT 'a' /* outer /* inner ; DELETE */ still comment */ FROM t",
                                  SelectQueryValidator.Dialect.TSQL);

    // Second literal has no E prefix, so '\' does not escape the quote in PostgreSQL.
    assertRejected("SELECT E'a\\'b', 'c\\'; DELETE FROM t; --'", SelectQueryValidator.Dialect.POSTGRESQL);
    assertRejected("SELECT E'a\\'b', 'c\\'; DELETE FROM t; --'", SelectQueryValidator.Dialect.UNKNOWN);

    // PostgreSQL quotecontinue concatenates adjacent newline-separated string segments in E'...' mode.
    assertRejected("SELECT E'a'\n'b\\'', 'x'; DELETE FROM t; --'", SelectQueryValidator.Dialect.POSTGRESQL);
    assertRejected("SELECT E'a'\n'b\\'', 'x'; DELETE FROM t; --'", SelectQueryValidator.Dialect.UNKNOWN);

    // Nested block comment breakout must be rejected in POSTGRESQL, TSQL, and UNKNOWN.
    String nestedCommentBreakout = "SELECT 'a' /* /* */ , ' */ ; DELETE FROM t; --'";
    assertRejected(nestedCommentBreakout, SelectQueryValidator.Dialect.POSTGRESQL);
    assertRejected(nestedCommentBreakout, SelectQueryValidator.Dialect.TSQL);
    assertRejected(nestedCommentBreakout, SelectQueryValidator.Dialect.UNKNOWN);

    assertRejected("SELECT * FROM t /* unterminated comment", SelectQueryValidator.Dialect.ANSI);
    assertRejected("SELECT * FROM t /* outer /* inner */", SelectQueryValidator.Dialect.POSTGRESQL);
  }

  @Test
  public void testMysqlDashDashAndHashComments() {
    // MySQL requires whitespace, a control character, or EOF after '--' to start a comment.
    String[] nonCommentsInMysql = {
      "SELECT name FROM t WHERE id > 0 --1 INTO OUTFILE '/tmp/x'",
      "SELECT name FROM t WHERE id > 0 --1 FOR UPDATE",
      "SELECT 1 --1; DELETE FROM t",
    };
    for (String query : nonCommentsInMysql) {
      assertRejected(query, SelectQueryValidator.Dialect.MYSQL);
      assertRejected(query, SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH);
      assertRejected(query, SelectQueryValidator.Dialect.UNKNOWN);
    }

    SelectQueryValidator.validate("SELECT name FROM t WHERE id > 0 -- 1trailing comment",
                                  SelectQueryValidator.Dialect.MYSQL);
    SelectQueryValidator.validate("SELECT name FROM t WHERE id > 0 --",
                                  SelectQueryValidator.Dialect.MYSQL);

    // '#' starts a line comment in MySQL, but ends at \n or \r and is not a comment in ANSI/PostgreSQL.
    SelectQueryValidator.validate("SELECT * FROM t WHERE id = 1 # comment ; DELETE FROM t",
                                  SelectQueryValidator.Dialect.MYSQL);
    assertRejected("SELECT * FROM t # comment\n; DELETE FROM t", SelectQueryValidator.Dialect.MYSQL);
    assertRejected("SELECT * FROM t # comment\r; DELETE FROM t", SelectQueryValidator.Dialect.MYSQL);
    assertRejected("SELECT * FROM t WHERE id = 1 # comment ; DELETE FROM t", SelectQueryValidator.Dialect.ANSI);
  }

  @Test
  public void testDoubleQuoteBackslashBreakoutUnderAnsiQuotesRejected() {
    // Valid single string in backslash mode, but breaks out as an identifier if ANSI_QUOTES is active.
    String ansiQuotesBreakout = "SELECT \"a\\\"; DELETE FROM t; -- \" FROM t";
    assertRejected(ansiQuotesBreakout, SelectQueryValidator.Dialect.MYSQL);
    assertRejected(ansiQuotesBreakout, SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH);
    assertRejected(ansiQuotesBreakout, SelectQueryValidator.Dialect.DATABRICKS);
    assertRejected(ansiQuotesBreakout, SelectQueryValidator.Dialect.UNKNOWN);

    String backslashModeBreakout = "SELECT \"a\\\"\"; DELETE FROM my_table; --\" FROM my_table";
    assertRejected(backslashModeBreakout, SelectQueryValidator.Dialect.MYSQL);
    assertRejected(backslashModeBreakout, SelectQueryValidator.Dialect.UNKNOWN);
  }

  @Test
  public void testCarriageReturnLineCommentTerminationRejected() {
    for (SelectQueryValidator.Dialect dialect : SelectQueryValidator.Dialect.values()) {
      assertRejected("SELECT 1 -- comment\r; DELETE FROM t", dialect);
      assertRejected("SELECT * FROM t -- comment\rINTO OUTFILE '/tmp/x'", dialect);
    }
  }

  @Test
  public void testTurkishDefaultLocaleDoesNotBypassKeywordOrUrlChecks() {
    Locale previous = Locale.getDefault();
    Locale.setDefault(new Locale("tr", "TR"));
    try {
      assertRejected("select * into backup_t from my_table", SelectQueryValidator.Dialect.UNKNOWN);
      assertRejected("select * from my_table into outfile '/tmp/x'", SelectQueryValidator.Dialect.MYSQL);
      assertRejected("select * from my_table kill 52", SelectQueryValidator.Dialect.TSQL);
      assertRejected("with d as (insert into my_table values (1)) select * from d",
                     SelectQueryValidator.Dialect.POSTGRESQL);
      Assert.assertEquals(SelectQueryValidator.Dialect.TSQL,
                          SelectQueryValidator.Dialect.fromJdbcUrl("JDBC:SQLSERVER://HOST:1433", null));
      Assert.assertEquals(
        SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH,
        SelectQueryValidator.Dialect.fromJdbcUrl("JDBC:MYSQL://HOST:3306/DB",
                                                 "sessionVariables=sql_mode='NO_BACKSLASH_ESCAPES'"));
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  public void testNonAsciiWhitespaceDoesNotGlueKeywords() {
    assertRejected("DELETE\u00A0FROM t WHERE id IN (SELECT 1)", SelectQueryValidator.Dialect.UNKNOWN);
    assertRejected("SELECT * INTO\u00A0x FROM t", SelectQueryValidator.Dialect.UNKNOWN);
    assertRejected("SELECT * FROM t FOR\u00A0UPDATE", SelectQueryValidator.Dialect.UNKNOWN);
    assertRejected("SELECT * FROM t FOR\u3000KEY\u2000SHARE", SelectQueryValidator.Dialect.POSTGRESQL);
  }

  @Test
  public void testLexerMismatchConstructsOutsideLiteralsRejected() {
    assertRejected("SELECT $$ ' $$; DELETE FROM my_table; -- '", SelectQueryValidator.Dialect.POSTGRESQL);
    assertRejected("SELECT $tag$ ' $tag$; UPDATE my_table SET v=$$x$$ WHERE id=1; SELECT * FROM t WHERE 1=1 -- '",
                   SelectQueryValidator.Dialect.UNKNOWN);
    assertRejected("SELECT $$x$$ AS c FROM my_table", SelectQueryValidator.Dialect.POSTGRESQL);
    assertRejected("SELECT 123$tag$x$tag$ FROM my_table", SelectQueryValidator.Dialect.POSTGRESQL);
    assertRejected("SELECT q'['; DELETE FROM my_table; --]' FROM dual", SelectQueryValidator.Dialect.ANSI);
    assertRejected("SELECT nq'['; DELETE FROM my_table; --]' FROM dual", SelectQueryValidator.Dialect.ANSI);
    assertRejected("SELECT * FROM my_table /*!50000 INTO OUTFILE '/tmp/out.csv' */",
                   SelectQueryValidator.Dialect.MYSQL);
    assertRejected("SELECT * FROM my_table /*!50000 FOR UPDATE */", SelectQueryValidator.Dialect.MYSQL);
    assertRejected("SELECT 1; /*! DELETE FROM my_table */", SelectQueryValidator.Dialect.MYSQL);
    assertRejected("SELECT * FROM my_table /*M!50000 FOR UPDATE */", SelectQueryValidator.Dialect.MYSQL);
    assertRejected("SELECT * FROM my_table /* outer /*!50000 INTO OUTFILE '/tmp/x' */ */",
                   SelectQueryValidator.Dialect.POSTGRESQL);
  }

  @Test
  public void testStackedWriteLockingAndTsqlBatchStatementsRejected() {
    String[] rejected = {
      "SELECT 1; DELETE FROM t",
      "SELECT * FROM t; UPDATE t SET v = 1",
      "SELECT * FROM \"pg_catalog\".\"pg_class\" LIMIT 0; UPDATE t SET v = 'x'; SELECT 1",
      "UPDATE my_table SET v = 1",
      "DELETE FROM my_table",
      "INSERT INTO my_table VALUES (1)",
      "DROP TABLE my_table",
      "TRUNCATE TABLE my_table",
      "CREATE TABLE t (id INT)",
      "ALTER TABLE my_table ADD COLUMN c INT",
      "MERGE INTO t USING s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.v = s.v",
      "GRANT SELECT ON my_table TO someone",
      "EXEC my_procedure",
      "CALL my_procedure()",
      "VALUES (1)",
      "TABLE my_table",
      "COPY (SELECT * FROM t) TO PROGRAM 'id'",
      "EXPLAIN SELECT * FROM t",
      "EXPLAIN (ANALYZE) SELECT * FROM t",
      "REPLACE t (a) SELECT 1",
      "LOCKING TABLE t FOR WRITE SELECT * FROM t",
      "SELECT * INTO new_table FROM my_table",
      "SELECT * FROM my_table INTO OUTFILE '/tmp/out.csv'",
      "SELECT * FROM my_table FOR UPDATE",
      "SELECT * FROM my_table FOR SHARE",
      "SELECT * FROM my_table FOR NO KEY UPDATE",
      "SELECT * FROM my_table FOR KEY SHARE",
      "SELECT * FROM my_table LOCK IN SHARE MODE",
      "SELECT * FROM (SELECT * INTO new_table FROM my_table) t",
      "SELECT * FROM (SELECT * FROM my_table FOR UPDATE) t",
      "SELECT * FROM my_table WHERE id IN (SELECT id FROM other_table FOR KEY SHARE)",
      "SELECT id FROM a UNION ALL (SELECT id INTO t2 FROM b)",
      "WITH d AS (DELETE FROM my_table RETURNING *) SELECT * FROM d",
      "SELECT 1 DENY SELECT ON t TO public",
      "SELECT * FROM t DBCC CHECKIDENT('t', RESEED, 0)",
      "SELECT 1 SET IDENTITY_INSERT t ON",
      "SELECT * FROM t SHUTDOWN WITH NOWAIT",
      "SELECT 1 BACKUP DATABASE db TO DISK = 'x'",
      "SELECT 1 WAITFOR DELAY '00:00:10'",
      "SELECT * INTO/**/new_table FROM my_table",
      "SELECT * FROM my_table INTO/**/OUTFILE '/tmp/out.csv'",
      "SELECT * FROM my_table FOR/**/UPDATE",
      "SELECT * FROM \"my_table\"FOR\"x\"UPDATE",
      "SELECT a\"alias\"INTO new_table FROM my_table",
      "WITH d AS (DELETE/**/FROM my_table RETURNING *) SELECT * FROM d",
      "SELECT * INTO \"new_table\"(col1) FROM my_table",
      "UPDATE (SELECT * FROM my_table) SET v = 1",
      "EXEC (SELECT 'DROP TABLE x')",
      "EXECUTE (SELECT 'DROP TABLE x')",
      "",
      ";",
      null,
    };
    for (String query : rejected) {
      assertRejected(query, SelectQueryValidator.Dialect.UNKNOWN);
    }
  }

  @Test
  public void testConfirmWithParserRejectsNonSelectStatement() {
    SelectQueryValidator.confirmWithParser("SELECT * FROM my_table", false, false);

    IllegalArgumentException explainErr = Assert.assertThrows(
      IllegalArgumentException.class,
      () -> SelectQueryValidator.confirmWithParser("EXPLAIN SELECT * FROM t", false, false));
    Assert.assertTrue(explainErr.getMessage().startsWith("Only a SELECT statement is allowed. Found a "));

    IllegalArgumentException replaceErr = Assert.assertThrows(
      IllegalArgumentException.class,
      () -> SelectQueryValidator.confirmWithParser("REPLACE t (a) SELECT 1", false, false));
    Assert.assertEquals("Only a SELECT statement is allowed. Found a Upsert statement.", replaceErr.getMessage());
  }

  @Test
  public void testInvalidQueriesReportExpectedErrorMessages() {
    assertError("SELECT 1; DELETE FROM t", "Only a single SELECT statement is allowed. Found 2 statements.");
    assertError("DELETE FROM t", "The 'DELETE' keyword is not allowed. The query must only read data.");
    assertError("SELECT * INTO t2 FROM t", "The 'INTO' keyword is not allowed. The query must only read data.");
    assertError("SELECT * FROM t FOR UPDATE",
                "FOR UPDATE/SHARE locking clauses are not allowed. The query must only read data.");
    assertError("SELECT * FROM t FOR KEY SHARE",
                "FOR UPDATE/SHARE locking clauses are not allowed. The query must only read data.");
    assertError("SELECT * FROM t LOCK IN SHARE MODE",
                "LOCK IN SHARE MODE clauses are not allowed. The query must only read data.");
    assertError("TABLE my_table", "Only a SELECT statement is allowed. No SELECT keyword was found.");
    assertError("EXPLAIN SELECT * FROM t", "Only a SELECT statement is allowed. Query cannot start with 'EXPLAIN'.");
  }

  @Test
  public void testDialectFromJdbcUrl() {
    Assert.assertEquals(SelectQueryValidator.Dialect.TSQL,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:sqlserver://localhost:1433", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.TSQL,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:jtds:sqlserver://localhost:1433/db", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.POSTGRESQL,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:postgresql://localhost:5432/db", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.UNKNOWN,
                        SelectQueryValidator.Dialect.fromJdbcUrl(
                          "jdbc:postgresql://localhost:5432/db?options=-c%20standard_conforming_strings=off", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.MYSQL,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:mysql://localhost:3306/db", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.MYSQL,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:mariadb://localhost:3306/db", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH,
                        SelectQueryValidator.Dialect.fromJdbcUrl(
                          "jdbc:mysql://localhost:3306/db", "sessionVariables=sql_mode='NO_BACKSLASH_ESCAPES'"));
    Assert.assertEquals(SelectQueryValidator.Dialect.DATABRICKS,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:databricks://host:443/default", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.ANSI,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:oracle:thin:@localhost:1521:xe", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.ANSI,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:hsqldb:hsql://localhost/testdb", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.UNKNOWN,
                        SelectQueryValidator.Dialect.fromJdbcUrl("jdbc:redshift://cluster:5439/dev", null));
    Assert.assertEquals(SelectQueryValidator.Dialect.UNKNOWN,
                        SelectQueryValidator.Dialect.fromJdbcUrl(null, null));
  }

  @Test
  public void testValidateColumnReferenceAllowed() {
    String[] columns = {"id", "my_col", "t.id", "\"My Col\"", "`id`", "schema.t.id", "v$col"};
    for (String col : columns) {
      SelectQueryValidator.validateColumnReference(col, SelectQueryValidator.Dialect.UNKNOWN);
    }
    SelectQueryValidator.validateColumnReference("[my id]", SelectQueryValidator.Dialect.TSQL);
  }

  @Test
  public void testValidateColumnReferenceWrapsTokenMgrExceptionCleanly() {
    for (String malformedQuote : new String[]{"id'", "\"id", "id`"}) {
      IllegalArgumentException ex = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> SelectQueryValidator.validateColumnReference(malformedQuote, SelectQueryValidator.Dialect.UNKNOWN));
      Assert.assertEquals("Must be a single column name.", ex.getMessage());
      Assert.assertTrue("Expected TokenMgrException cause for input: " + malformedQuote,
                        ex.getCause() instanceof TokenMgrException);
    }
  }

  @Test
  public void testValidateColumnReferenceRejectsEmptyAndInjections() {
    for (String blank : new String[]{"", "   ", null}) {
      IllegalArgumentException ex = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> SelectQueryValidator.validateColumnReference(blank, SelectQueryValidator.Dialect.UNKNOWN));
      Assert.assertEquals("Must not be empty.", ex.getMessage());
    }

    String[] injections = {
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
      "id\nOR 1=1",
      "'id'",
      "*",
      "t.*",
      "$$id$$",
    };
    for (String col : injections) {
      IllegalArgumentException ex = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> SelectQueryValidator.validateColumnReference(col, SelectQueryValidator.Dialect.UNKNOWN));
      Assert.assertEquals("Must be a single column name.", ex.getMessage());
    }
  }

  private static void assertRejected(String query, SelectQueryValidator.Dialect dialect) {
    Assert.assertThrows("Expected query to be rejected under " + dialect + ": " + query,
                        IllegalArgumentException.class,
                        () -> SelectQueryValidator.validate(query, dialect));
  }

  private static void assertError(String query, String expectedMessage) {
    try {
      SelectQueryValidator.validate(query, SelectQueryValidator.Dialect.UNKNOWN);
      Assert.fail(String.format("Query '%s' should have been rejected", query));
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(expectedMessage, e.getMessage());
    }
  }
}
