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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * Validates that a user provided query is a single, read-only SELECT statement before it is executed with the
 * credential of a database connection.
 */
public final class SelectQueryValidator {

  private static final long PARSE_TIMEOUT_MS = 10_000L;
  // Placeholder replaced with the split condition at runtime, it is not valid SQL on its own
  private static final String CONDITIONS_PLACEHOLDER = "$CONDITIONS";
  private static final ThreadFactory PARSER_THREAD_FACTORY = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("select-query-validator-%d").build();

  private SelectQueryValidator() {
  }

  /**
   * Validates that the given query is a single read-only SELECT statement.
   *
   * @param query the query to validate
   * @param squareBracketQuotes whether square brackets quote identifiers, as in SQL Server
   * @throws IllegalArgumentException if the query cannot be parsed or is not a single read-only SELECT statement
   */
  public static void validate(String query, boolean squareBracketQuotes) {
    String parsableQuery = query.replace(CONDITIONS_PLACEHOLDER, "1 = 1");
    // Databases disagree on whether a backslash escapes a quote inside a string literal. Requiring a single SELECT
    // under both interpretations makes sure a crafted string literal cannot hide a second statement.
    validate(parsableQuery, squareBracketQuotes, false);
    validate(parsableQuery, squareBracketQuotes, true);
  }

  private static void validate(String query, boolean squareBracketQuotes, boolean backslashEscapes) {
    Statements statements;
    // Use our own executor so that its thread is always released, the parser does not shut down its own executor
    // when parsing fails.
    ExecutorService executor = Executors.newSingleThreadExecutor(PARSER_THREAD_FACTORY);
    try {
      statements = CCJSqlParserUtil.parseStatements(query, executor, parser -> parser
        .withSquareBracketQuotation(squareBracketQuotes)
        .withBackslashEscapeCharacter(backslashEscapes)
        .withTimeOut(PARSE_TIMEOUT_MS));
    } catch (JSQLParserException e) {
      throw new IllegalArgumentException(
        "Query could not be parsed. Only a single SELECT statement is allowed. Error: " + firstLine(e), e);
    } finally {
      executor.shutdownNow();
    }

    // the parser returns null for an empty query
    List<Statement> nonEmptyStatements = statements == null ? Collections.emptyList()
      : statements.stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (nonEmptyStatements.size() != 1) {
      throw new IllegalArgumentException(String.format(
        "Only a single SELECT statement is allowed, but found %d statements.", nonEmptyStatements.size()));
    }
    Statement statement = nonEmptyStatements.get(0);
    if (!(statement instanceof Select)) {
      throw new IllegalArgumentException(String.format(
        "Only a SELECT statement is allowed, but found a %s statement.", statement.getClass().getSimpleName()));
    }
    validateSelect((Select) statement);
  }

  private static void validateSelect(Select select) {
    if (select.getWithItemsList() != null) {
      for (WithItem withItem : select.getWithItemsList()) {
        validateSelect(withItem.getSelect());
      }
    }
    if (select instanceof PlainSelect) {
      PlainSelect plainSelect = (PlainSelect) select;
      if ((plainSelect.getIntoTables() != null && !plainSelect.getIntoTables().isEmpty())
        || plainSelect.getIntoTempTable() != null) {
        throw new IllegalArgumentException("SELECT ... INTO is not allowed, the query must only read data.");
      }
      if (plainSelect.getForMode() != null) {
        throw new IllegalArgumentException(String.format(
          "SELECT ... FOR %s is not allowed, the query must only read data.", plainSelect.getForMode().getValue()));
      }
    } else if (select instanceof SetOperationList) {
      for (Select operand : ((SetOperationList) select).getSelects()) {
        validateSelect(operand);
      }
    } else if (select instanceof ParenthesedSelect) {
      validateSelect(((ParenthesedSelect) select).getSelect());
    } else {
      throw new IllegalArgumentException(String.format(
        "Unsupported SELECT statement of type %s.", select.getClass().getSimpleName()));
    }
  }

  private static String firstLine(Exception e) {
    String message = String.valueOf(e.getMessage());
    int newLine = message.indexOf('\n');
    return newLine < 0 ? message : message.substring(0, newLine);
  }
}
