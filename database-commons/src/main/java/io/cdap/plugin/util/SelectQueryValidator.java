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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.Select;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Validates that a user provided query is a single, read-only SELECT statement before it is executed with the
 * credential of a database connection.
 *
 * <p>The check is primarily lexical: the query is scanned with awareness of string literals, comments and quoted
 * identifiers, and is required to be a single statement that reads (contains {@code SELECT}) and carries no write or
 * data-definition keyword, no {@code INTO}, and no {@code FOR UPDATE}/{@code SHARE} locking clause outside of quoted
 * text. A lexical scan is used because no single SQL parser understands every dialect the connectors support, and a
 * parser that mis-lexes a construct (dollar quotes, executable comments, dialect-specific clauses) either lets an
 * injection through or rejects a valid query. A few constructs that parsers are known to lex differently from the
 * database driver are rejected outright up front. JSqlParser is still run as a best-effort secondary check, but only
 * to add confidence when it succeeds; a query it cannot parse is not rejected on that basis alone.
 */
public final class SelectQueryValidator {

  private static final long PARSE_TIMEOUT_MS = 10_000L;
  // Placeholder replaced with the split condition at runtime, it is not valid SQL on its own
  private static final String CONDITIONS_PLACEHOLDER = "$CONDITIONS";

  private static final ThreadFactory PARSER_THREAD_FACTORY = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("select-query-validator-%d").build();
  // A single shared, bounded pool of daemon threads for the parser, instead of two new executors per call. The
  // unbounded queue makes concurrent bursts wait for a thread rather than being rejected, and idle threads still
  // terminate after the keep-alive.
  private static final ExecutorService PARSER_EXECUTOR = createParserExecutor();

  private static ExecutorService createParserExecutor() {
    int maxThreads = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
      maxThreads, maxThreads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), PARSER_THREAD_FACTORY);
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  // Constructs that JSqlParser and the database drivers lex differently, so a crafted literal can look like one
  // SELECT to the parser while the driver splits it into several statements. They never occur in a sampling query.
  private static final Pattern DOLLAR_QUOTE =
    Pattern.compile("\\$([A-Za-z_\\u0080-\\uFFFF][A-Za-z0-9_\\u0080-\\uFFFF]*)?\\$");
  private static final Pattern ORACLE_QUOTE = Pattern.compile("(^|[^A-Za-z0-9_])[qQ]'", Pattern.MULTILINE);
  private static final Pattern EXECUTABLE_COMMENT = Pattern.compile("/\\*(M)?!");

  // Keywords that, used as a statement (not as a function call), write or change schema. Matched only when the
  // keyword is not immediately followed by '(' so legitimate functions such as MySQL TRUNCATE(x, d) are allowed.
  private static final Set<String> WRITE_KEYWORDS = new HashSet<>(Arrays.asList(
    "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
    "GRANT", "REVOKE", "CALL", "EXEC", "EXECUTE", "INTO"));
  // Of the write keywords, only these also name scalar functions (e.g. MySQL TRUNCATE(x, d), INSERT(s, p, l, n)).
  // A trailing '(' exempts a write keyword only for these, and only when it is not the first word of the statement,
  // so statements such as UPDATE (SELECT ...) SET ... or EXEC (...) are never allowed through.
  private static final Set<String> FUNCTION_COLLIDING_WRITE_KEYWORDS =
    new HashSet<>(Arrays.asList("TRUNCATE", "INSERT"));
  // Tokens that, following FOR, acquire locks (FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE). FOR READ ONLY and
  // FOR FETCH ONLY are read-only and allowed.
  private static final Set<String> LOCKING_AFTER_FOR = new HashSet<>(Arrays.asList("UPDATE", "SHARE", "NO"));

  private SelectQueryValidator() {
  }

  /**
   * Validates that the given query is a single read-only SELECT statement, trying both backslash-escape
   * interpretations since the dialect is unknown.
   *
   * @param query the query to validate
   * @param squareBracketQuotes whether square brackets quote identifiers, as in SQL Server
   * @throws IllegalArgumentException if the query is not a single read-only SELECT statement
   */
  public static void validate(String query, boolean squareBracketQuotes) {
    validate(query, squareBracketQuotes, null);
  }

  /**
   * Validates that the given query is a single read-only SELECT statement.
   *
   * @param query the query to validate
   * @param squareBracketQuotes whether square brackets quote identifiers, as in SQL Server
   * @param backslashEscapes whether a backslash escapes a quote inside a string literal in the database's dialect;
   *     {@code null} when the dialect is unknown, in which case the query must pass under both interpretations
   * @throws IllegalArgumentException if the query is not a single read-only SELECT statement
   */
  public static void validate(String query, boolean squareBracketQuotes, @Nullable Boolean backslashEscapes) {
    if (query == null) {
      throw new IllegalArgumentException("Query must not be null.");
    }
    String parsableQuery = query.replace(CONDITIONS_PLACEHOLDER, "1 = 1");
    rejectLexerMismatchConstructs(parsableQuery);
    // A double quote is a string literal in MySQL/MariaDB/SingleStore/Databricks by default (where a backslash can
    // escape it) but an identifier when ANSI_QUOTES is enabled (where it cannot). When the dialect is unknown, or it
    // is a backslash dialect and the query contains a double quote, require the query to be a single read-only SELECT
    // under both backslash interpretations so a crafted literal cannot break out either way.
    boolean bothModes = backslashEscapes == null
      || (Boolean.TRUE.equals(backslashEscapes) && parsableQuery.indexOf('"') >= 0);
    if (bothModes) {
      validateOneMode(parsableQuery, squareBracketQuotes, false);
      validateOneMode(parsableQuery, squareBracketQuotes, true);
    } else {
      validateOneMode(parsableQuery, squareBracketQuotes, backslashEscapes);
    }
  }

  private static void rejectLexerMismatchConstructs(String query) {
    if (DOLLAR_QUOTE.matcher(query).find()) {
      throw new IllegalArgumentException(
        "Dollar-quoted string literals ($$...$$ or $tag$...$tag$) are not allowed in the query.");
    }
    if (ORACLE_QUOTE.matcher(query).find()) {
      throw new IllegalArgumentException("Alternative-quoted string literals (q'...') are not allowed in the query.");
    }
    if (EXECUTABLE_COMMENT.matcher(query).find()) {
      throw new IllegalArgumentException("Executable SQL comments ('/*!' or '/*M!') are not allowed in the query.");
    }
  }

  private static void validateOneMode(String query, boolean squareBracketQuotes, boolean backslashEscapes) {
    lexicalCheck(query, squareBracketQuotes, backslashEscapes);
    // Best-effort parser confirmation: if JSqlParser can parse the query, make sure it is a single SELECT. If it
    // cannot parse it (a valid but dialect-specific construct), the lexical check above is authoritative.
    Statements statements;
    try {
      statements = CCJSqlParserUtil.parseStatements(query, PARSER_EXECUTOR, parser -> parser
        .withSquareBracketQuotation(squareBracketQuotes)
        .withBackslashEscapeCharacter(backslashEscapes)
        .withTimeOut(PARSE_TIMEOUT_MS));
    } catch (JSQLParserException e) {
      return;
    }
    // Statement separation is authoritative in the lexical scan above (it honors dialect comments such as MySQL
    // '#', which JSqlParser does not), so the parser is only used here to confirm the statement type.
    List<Statement> nonEmpty = statements == null ? java.util.Collections.emptyList()
      : statements.stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (nonEmpty.size() == 1 && !(nonEmpty.get(0) instanceof Select)) {
      throw new IllegalArgumentException(String.format(
        "Only a SELECT statement is allowed. Found a %s statement.", nonEmpty.get(0).getClass().getSimpleName()));
    }
  }

  /**
   * Scans the query, ignoring the contents of string literals, comments and quoted identifiers, and enforces that it
   * is a single statement that reads and performs no write, schema change or locking.
   */
  private static void lexicalCheck(String query, boolean squareBracketQuotes, boolean backslashEscapes) {
    StringBuilder bareText = new StringBuilder(query.length());
    int statementsWithContent = 0;
    boolean currentStatementHasContent = false;
    int i = 0;
    int n = query.length();
    while (i < n) {
      char c = query.charAt(i);
      if (c == '-' && i + 1 < n && query.charAt(i + 1) == '-') {
        i = skipToEndOfLine(query, i + 2);
        // A comment is a token boundary: emit a space so adjacent words do not glue into one token.
        bareText.append(' ');
      } else if (backslashEscapes && c == '#') {
        // '#' starts a line comment in MySQL/MariaDB. Gated to backslash dialects so that in standard dialects,
        // where '#' can be an operator, a '#' never hides a following statement from the scan.
        i = skipToEndOfLine(query, i + 1);
        bareText.append(' ');
      } else if (c == '/' && i + 1 < n && query.charAt(i + 1) == '*') {
        i = skipBlockComment(query, i);
        bareText.append(' ');
      } else if (c == '\'') {
        i = skipString(query, i, '\'', backslashEscapes);
        currentStatementHasContent = true;
        // Replace the literal/identifier with a placeholder token surrounded by spaces, so neighbouring keywords
        // stay separate tokens and a stripped identifier before '(' cannot make a preceding keyword look like a call.
        bareText.append(" _ ");
      } else if (c == '"') {
        // A double quote may be a string (backslash can escape it) or an identifier; honor the dialect's backslash
        // rule so a backslash-escaped quote cannot hide the real end of the token.
        i = skipString(query, i, '"', backslashEscapes);
        currentStatementHasContent = true;
        bareText.append(" _ ");
      } else if (c == '`') {
        i = skipQuotedIdentifier(query, i, '`');
        currentStatementHasContent = true;
        bareText.append(" _ ");
      } else if (squareBracketQuotes && c == '[') {
        i = skipBracketIdentifier(query, i);
        currentStatementHasContent = true;
        bareText.append(" _ ");
      } else if (c == ';') {
        if (currentStatementHasContent) {
          statementsWithContent++;
        }
        currentStatementHasContent = false;
        bareText.append(' ');
        i++;
      } else {
        if (!Character.isWhitespace(c)) {
          currentStatementHasContent = true;
        }
        bareText.append(c);
        i++;
      }
    }
    if (currentStatementHasContent) {
      statementsWithContent++;
    }
    if (statementsWithContent > 1) {
      throw new IllegalArgumentException(String.format(
        "Only a single SELECT statement is allowed. Found %d statements.", statementsWithContent));
    }
    checkKeywords(bareText.toString());
  }

  private static void checkKeywords(String bareText) {
    List<String> words = new java.util.ArrayList<>();
    List<Integer> endOffsets = new java.util.ArrayList<>();
    int i = 0;
    int n = bareText.length();
    while (i < n) {
      char c = bareText.charAt(i);
      if (isIdentifierChar(c)) {
        int start = i;
        while (i < n && isIdentifierChar(bareText.charAt(i))) {
          i++;
        }
        words.add(bareText.substring(start, i).toUpperCase());
        endOffsets.add(i);
      } else {
        i++;
      }
    }
    for (int w = 0; w < words.size(); w++) {
      String word = words.get(w);
      if (WRITE_KEYWORDS.contains(word)) {
        boolean isAllowedFunctionCall = FUNCTION_COLLIDING_WRITE_KEYWORDS.contains(word)
          && w > 0
          && isFollowedByOpenParen(bareText, endOffsets.get(w));
        if (!isAllowedFunctionCall) {
          throw new IllegalArgumentException(String.format(
            "The '%s' keyword is not allowed. The query must only read data.", word));
        }
      }
      if ("FOR".equals(word) && w + 1 < words.size() && LOCKING_AFTER_FOR.contains(words.get(w + 1))) {
        throw new IllegalArgumentException(
          "FOR UPDATE/SHARE locking clauses are not allowed. The query must only read data.");
      }
    }
    if (!words.contains("SELECT")) {
      throw new IllegalArgumentException("Only a SELECT statement is allowed. No SELECT keyword was found.");
    }
  }

  private static boolean isFollowedByOpenParen(String text, int fromOffset) {
    int i = fromOffset;
    while (i < text.length() && Character.isWhitespace(text.charAt(i))) {
      i++;
    }
    return i < text.length() && text.charAt(i) == '(';
  }

  private static boolean isIdentifierChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c >= 0x80;
  }

  private static int skipToEndOfLine(String q, int i) {
    while (i < q.length() && q.charAt(i) != '\n') {
      i++;
    }
    return i;
  }

  private static int skipBlockComment(String q, int i) {
    i += 2;
    while (i + 1 < q.length() && !(q.charAt(i) == '*' && q.charAt(i + 1) == '/')) {
      i++;
    }
    return Math.min(q.length(), i + 2);
  }

  private static int skipString(String q, int i, char quote, boolean backslashEscapes) {
    i++;
    while (i < q.length()) {
      char c = q.charAt(i);
      if (backslashEscapes && c == '\\' && i + 1 < q.length()) {
        i += 2;
        continue;
      }
      if (c == quote) {
        if (i + 1 < q.length() && q.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    throw new IllegalArgumentException("Unterminated string literal in the query.");
  }

  private static int skipQuotedIdentifier(String q, int i, char quote) {
    i++;
    while (i < q.length()) {
      char c = q.charAt(i);
      if (c == quote) {
        if (i + 1 < q.length() && q.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    throw new IllegalArgumentException("Unterminated quoted identifier in the query.");
  }

  private static int skipBracketIdentifier(String q, int i) {
    i++;
    while (i < q.length()) {
      char c = q.charAt(i);
      if (c == ']') {
        if (i + 1 < q.length() && q.charAt(i + 1) == ']') {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    throw new IllegalArgumentException("Unterminated quoted identifier in the query.");
  }

  /**
   * Validates that the given value is a single, bare column reference (optionally qualified and/or quoted) and
   * nothing more. Used for inputs such as the split-by field, which are concatenated into generated SQL at runtime
   * and so must not be able to carry operators, function calls, comments or additional statements.
   *
   * @param value the value to validate
   * @param label a human readable name for the value, used in error messages
   * @param squareBracketQuotes whether square brackets quote identifiers, as in SQL Server
   * @throws IllegalArgumentException if the value is not a single column reference
   */
  public static void validateColumnReference(String value, String label, boolean squareBracketQuotes) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(label + " must not be empty.");
    }
    // A column reference is concatenated into generated SQL, so reject anything that could end the identifier or the
    // clause: statement separators, comments, backslashes and the lexer-mismatch quote constructs.
    if (value.indexOf(';') >= 0 || value.indexOf('\\') >= 0 || value.indexOf('#') >= 0
      || value.contains("--") || value.contains("/*")
      || DOLLAR_QUOTE.matcher(value).find() || ORACLE_QUOTE.matcher(value).find()
      || EXECUTABLE_COMMENT.matcher(value).find()) {
      throw new IllegalArgumentException(label + " must be a single column name.");
    }
    Expression expression;
    try {
      // allowPartialParse=false so trailing content (operators, ';', extra tokens) is rejected, not ignored
      expression = CCJSqlParserUtil.parseExpression(value, false,
        parser -> parser.withSquareBracketQuotation(squareBracketQuotes).withTimeOut(PARSE_TIMEOUT_MS));
    } catch (JSQLParserException e) {
      throw new IllegalArgumentException(label + " must be a single column name. Error: " + firstLine(e), e);
    }
    if (!(expression instanceof Column)) {
      throw new IllegalArgumentException(label + " must be a single column name.");
    }
  }

  private static String firstLine(Exception e) {
    String message = String.valueOf(e.getMessage());
    int newLine = message.indexOf('\n');
    return newLine < 0 ? message : message.substring(0, newLine);
  }
}
