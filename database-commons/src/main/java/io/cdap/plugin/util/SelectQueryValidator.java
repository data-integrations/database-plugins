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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.TokenMgrException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.Select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
 * Validates that a user-provided query is a single, read-only {@code SELECT} statement before it is executed with the
 * credentials of a database connection.
 *
 * <p>The check is primarily lexical: the query is scanned with dialect-aware rules for string literals, comments, and
 * quoted identifiers, and is required to be a single statement starting with {@code SELECT} or {@code WITH} (or a
 * Teradata {@code LOCKING ... FOR ACCESS} modifier) that carries no write, DDL, administrative, {@code INTO}, or
 * row/table locking clause outside quoted text. Constructs that parsers and database drivers lex differently when
 * encountered outside literals and comments (PostgreSQL dollar-quoted strings, Oracle {@code q'...'} literals, and
 * MySQL executable comments) are rejected outright. JSqlParser is run as a best-effort secondary check to confirm
 * statement count and statement type when the query is within its grammar.
 */
public final class SelectQueryValidator {

  private static final long PARSE_TIMEOUT_MS = 10_000L;
  // Placeholder replaced with the split condition at runtime; it is not valid SQL on its own.
  private static final String CONDITIONS_PLACEHOLDER = "$CONDITIONS";
  private static final String STRING_PLACEHOLDER = "_S_";
  private static final String QUOTED_IDENT_PLACEHOLDER = "_Q_";

  private static final ThreadFactory PARSER_THREAD_FACTORY = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("select-query-validator-%d").build();
  // Bounded pool of daemon threads for JSqlParser timeout enforcement; idle threads terminate after keep-alive.
  private static final ExecutorService PARSER_EXECUTOR = createParserExecutor();

  private static ExecutorService createParserExecutor() {
    int maxThreads = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
      maxThreads, maxThreads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), PARSER_THREAD_FACTORY);
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  // Matches a PostgreSQL dollar-quote delimiter ($$ or $tag$) anchored at the current scanner position.
  private static final Pattern DOLLAR_QUOTE_AT_START =
    Pattern.compile("^\\$([A-Za-z_\\u0080-\\uFFFF][A-Za-z0-9_\\u0080-\\uFFFF]*)?\\$");

  // Keywords that perform writes, DDL, privilege changes, procedural execution, or server administration.
  // A keyword in this set is rejected unless:
  //   1. It is dot-qualified by an identifier (e.g. c.grant, t.update), so unquoted column names remain usable;
  //   2. It is in FUNCTION_COLLIDING_WRITE_KEYWORDS (TRUNCATE, INSERT), not the first word, and followed by '(';
  //   3. It is MERGE immediately followed by JOIN or UNION (SQL Server MERGE JOIN / OPTION (MERGE JOIN)); or
  //   4. It is SET immediately preceded by CHARACTER (e.g. CAST(x AS CHAR CHARACTER SET utf8mb4)), not 'AS CHARACTER'.
  private static final Set<String> WRITE_KEYWORDS = new HashSet<>(Arrays.asList(
    "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
    "GRANT", "REVOKE", "DENY", "EXEC", "EXECUTE", "INTO", "SET", "DBCC", "SHUTDOWN", "BACKUP", "RESTORE",
    "KILL", "RECONFIGURE", "BULK", "LOCK", "WAITFOR"));

  // Of WRITE_KEYWORDS, only TRUNCATE and INSERT also name scalar functions (e.g. TRUNCATE(x, d), INSERT(s, p, l, n)).
  private static final Set<String> FUNCTION_COLLIDING_WRITE_KEYWORDS =
    new HashSet<>(Arrays.asList("TRUNCATE", "INSERT"));

  // Tokens following FOR that acquire row/table locks (FOR UPDATE, FOR SHARE, FOR NO KEY UPDATE, FOR KEY SHARE).
  // Read-only clauses such as FOR READ ONLY and FOR FETCH ONLY are allowed.
  private static final Set<String> LOCKING_AFTER_FOR =
    new HashSet<>(Arrays.asList("UPDATE", "SHARE", "NO", "KEY"));

  // Allowed leading keywords for a read-only query.
  private static final Set<String> ALLOWED_START_KEYWORDS =
    new HashSet<>(Arrays.asList("SELECT", "WITH", "LOCKING"));

  /**
   * SQL dialect lexical profile used when scanning source queries and column references.
   */
  public enum Dialect {
    /**
     * Standard ANSI SQL (e.g. Oracle, DB2, SAP HANA, Teradata, HSQLDB): single-quoted strings escape quotes only by
     * doubling ({@code ''}), {@code "--"} starts a line comment unconditionally, and double quotes quote identifiers.
     */
    ANSI(false),

    /**
     * Microsoft SQL Server (T-SQL): single-quoted strings escape quotes only via {@code ''}, square brackets
     * ({@code [...]}) and double quotes quote identifiers, and {@code /* ... *}{@code /} comments nest.
     */
    TSQL(true),

    /**
     * PostgreSQL: standard {@code '...'} literals do not treat {@code '\'} as an escape, whereas {@code E'...'}
     * literals honor backslash escapes per literal, and {@code /* ... *}{@code /} comments nest.
     */
    POSTGRESQL(false),

    /**
     * MySQL, MariaDB, and SingleStore with default backslash escaping: {@code '\'} escapes quotes in literals,
     * {@code '#'} starts a line comment, {@code "--"} starts a line comment only before whitespace/control/EOF, and
     * queries containing {@code '"'} are checked under both string and {@code ANSI_QUOTES} identifier rules.
     */
    MYSQL(false),

    /**
     * MySQL family when {@code sql_mode} may be overridden (e.g. {@code NO_BACKSLASH_ESCAPES}): applies MySQL comment
     * rules and requires the query to pass under both backslash-escape interpretations.
     */
    MYSQL_AMBIGUOUS_BACKSLASH(false),

    /**
     * Databricks / Spark SQL: {@code '\'} escapes quotes in string literals, {@code "--"} starts a line comment
     * unconditionally, and queries containing {@code '"'} are checked under both backslash interpretations.
     */
    DATABRICKS(false),

    /**
     * Unknown or ambiguous dialect: requires the query to pass conservatively under ANSI, PostgreSQL {@code E'...'},
     * and MySQL (with and without backslash escapes) lexical rules.
     */
    UNKNOWN(false);

    private final boolean squareBracketQuotes;

    Dialect(boolean squareBracketQuotes) {
      this.squareBracketQuotes = squareBracketQuotes;
    }

    /**
     * Returns whether square brackets ({@code [...]}) quote identifiers in this dialect.
     */
    public boolean usesSquareBracketQuotes() {
      return squareBracketQuotes;
    }

    /**
     * Infers the {@link Dialect} from a JDBC URL and optional connection arguments, returning {@link #UNKNOWN} when
     * the URL is null or does not match a known driver prefix.
     */
    public static Dialect fromJdbcUrl(@Nullable String connectionString, @Nullable String connectionArguments) {
      if (connectionString == null) {
        return UNKNOWN;
      }
      String lower = connectionString.trim().toLowerCase(Locale.ROOT);
      if (lower.startsWith("jdbc:sqlserver:") || lower.startsWith("jdbc:jtds:sqlserver:")) {
        return TSQL;
      }
      if (lower.startsWith("jdbc:postgresql:") || lower.startsWith("jdbc:edb:")) {
        if (mayOverrideStandardConformingStrings(lower)
          || mayOverrideStandardConformingStrings(connectionArguments)) {
          return UNKNOWN;
        }
        return POSTGRESQL;
      }
      if (lower.startsWith("jdbc:mysql:") || lower.startsWith("jdbc:mariadb:")) {
        if (mayOverrideMysqlSqlMode(lower) || mayOverrideMysqlSqlMode(connectionArguments)) {
          return MYSQL_AMBIGUOUS_BACKSLASH;
        }
        return MYSQL;
      }
      if (lower.startsWith("jdbc:databricks:")) {
        return DATABRICKS;
      }
      if (lower.startsWith("jdbc:oracle:") || lower.startsWith("jdbc:db2:")
        || lower.startsWith("jdbc:hsqldb:") || lower.startsWith("jdbc:h2:")
        || lower.startsWith("jdbc:derby:") || lower.startsWith("jdbc:sap:")
        || lower.startsWith("jdbc:teradata:")) {
        return ANSI;
      }
      return UNKNOWN;
    }

    /**
     * Returns true if connection arguments may customize MySQL {@code sql_mode} (such as {@code NO_BACKSLASH_ESCAPES}).
     */
    public static boolean mayOverrideMysqlSqlMode(@Nullable String text) {
      if (text == null || text.isEmpty()) {
        return false;
      }
      String lower = text.toLowerCase(Locale.ROOT);
      return lower.contains("sql_mode") || lower.contains("no_backslash_escapes")
        || lower.contains("sessionvariables");
    }

    private static boolean mayOverrideStandardConformingStrings(@Nullable String text) {
      return text != null && text.toLowerCase(Locale.ROOT).contains("standard_conforming_strings");
    }
  }

  private static final class ScanConfig {
    final boolean squareBracketQuotes;
    final boolean backslashEscapesInSingleQuotes;
    final boolean backslashEscapesInDoubleQuotes;
    final boolean postgresExtendedStrings;
    final boolean hashComments;
    final boolean mysqlDashDashComments;
    final boolean nestedBlockComments;

    ScanConfig(boolean squareBracketQuotes,
               boolean backslashEscapesInSingleQuotes,
               boolean backslashEscapesInDoubleQuotes,
               boolean postgresExtendedStrings,
               boolean hashComments,
               boolean mysqlDashDashComments,
               boolean nestedBlockComments) {
      this.squareBracketQuotes = squareBracketQuotes;
      this.backslashEscapesInSingleQuotes = backslashEscapesInSingleQuotes;
      this.backslashEscapesInDoubleQuotes = backslashEscapesInDoubleQuotes;
      this.postgresExtendedStrings = postgresExtendedStrings;
      this.hashComments = hashComments;
      this.mysqlDashDashComments = mysqlDashDashComments;
      this.nestedBlockComments = nestedBlockComments;
    }
  }

  private static final class WordToken {
    final String word;
    final int start;
    final int end;
    final boolean canQualifyColumn;

    WordToken(String word, int start, int end, boolean canQualifyColumn) {
      this.word = word;
      this.start = start;
      this.end = end;
      this.canQualifyColumn = canQualifyColumn;
    }
  }

  private SelectQueryValidator() {
  }

  /**
   * Validates that the given query is a single read-only {@code SELECT} statement under the {@link Dialect#UNKNOWN}
   * conservative rules (or {@link Dialect#TSQL} if {@code squareBracketQuotes} is true).
   *
   * @param query the query to validate
   * @param squareBracketQuotes whether square brackets quote identifiers, as in SQL Server
   * @throws IllegalArgumentException if the query is not a single read-only SELECT statement
   */
  public static void validate(String query, boolean squareBracketQuotes) {
    validate(query, squareBracketQuotes ? Dialect.TSQL : Dialect.UNKNOWN);
  }

  /**
   * Validates that the given query is a single read-only {@code SELECT} statement for the specified {@link Dialect}.
   *
   * @param query the query to validate
   * @param dialect the database SQL dialect, or {@link Dialect#UNKNOWN} when unknown
   * @throws IllegalArgumentException if the query is not a single read-only SELECT statement
   */
  public static void validate(String query, Dialect dialect) {
    if (query == null) {
      throw new IllegalArgumentException("Query must not be null.");
    }
    Objects.requireNonNull(dialect, "Dialect must not be null.");
    String parsableQuery = query.replace(CONDITIONS_PLACEHOLDER, "1 = 1");
    boolean hasDoubleQuote = parsableQuery.indexOf('"') >= 0;

    switch (dialect) {
      case ANSI:
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, false, false, false, false, false, false));
        break;
      case TSQL:
        validateWithConfig(parsableQuery,
                           new ScanConfig(true, false, false, false, false, false, true));
        break;
      case POSTGRESQL:
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, false, false, true, false, false, true));
        break;
      case MYSQL:
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, true, true, false, true, true, false));
        if (hasDoubleQuote) {
          // Under ANSI_QUOTES a double quote is an identifier where '\' does not escape '"'.
          validateWithConfig(parsableQuery,
                             new ScanConfig(false, true, false, false, true, true, false));
        }
        break;
      case MYSQL_AMBIGUOUS_BACKSLASH:
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, false, false, false, true, true, false));
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, true, true, false, true, true, false));
        if (hasDoubleQuote) {
          validateWithConfig(parsableQuery,
                             new ScanConfig(false, true, false, false, true, true, false));
        }
        break;
      case DATABRICKS:
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, true, true, false, false, false, false));
        if (hasDoubleQuote) {
          validateWithConfig(parsableQuery,
                             new ScanConfig(false, true, false, false, false, false, false));
        }
        break;
      case UNKNOWN:
      default:
        // Must pass under ANSI, PostgreSQL (per-literal E'...' and nested comments), and MySQL (both backslash modes).
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, false, false, false, false, false, false));
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, false, false, true, false, false, true));
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, false, false, false, true, true, false));
        validateWithConfig(parsableQuery,
                           new ScanConfig(false, true, true, false, true, true, false));
        if (hasDoubleQuote) {
          validateWithConfig(parsableQuery,
                             new ScanConfig(false, true, false, false, true, true, false));
        }
        break;
    }
  }

  private static void validateWithConfig(String query, ScanConfig config) {
    boolean usedExtendedStrings = lexicalCheck(query, config);
    boolean parserBackslash = config.backslashEscapesInSingleQuotes || usedExtendedStrings;
    confirmWithParser(query, config.squareBracketQuotes, parserBackslash);
  }

  /**
   * Best-effort secondary check via JSqlParser: if JSqlParser can parse the query, verify that it produces no more
   * than one statement and that the statement is a {@link Select}. Dialect-specific syntax that JSqlParser cannot
   * parse is left to the authoritative lexical scan.
   */
  @VisibleForTesting
  static void confirmWithParser(String query, boolean squareBracketQuotes, boolean backslashEscapes) {
    Statements statements;
    try {
      statements = CCJSqlParserUtil.parseStatements(query, PARSER_EXECUTOR, parser -> parser
        .withSquareBracketQuotation(squareBracketQuotes)
        .withBackslashEscapeCharacter(backslashEscapes)
        .withTimeOut(PARSE_TIMEOUT_MS));
    } catch (JSQLParserException | TokenMgrException e) {
      return;
    }
    List<Statement> nonEmpty = statements == null ? Collections.emptyList()
      : statements.stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (nonEmpty.size() == 1 && !(nonEmpty.get(0) instanceof Select)) {
      throw new IllegalArgumentException(String.format(
        "Only a SELECT statement is allowed. Found a %s statement.", nonEmpty.get(0).getClass().getSimpleName()));
    }
  }

  /**
   * Scans the query, ignoring the contents of string literals, comments, and quoted identifiers, and enforces that it
   * is a single read-only SELECT statement. Returns true if any PostgreSQL {@code E'...'} literal was scanned.
   */
  private static boolean lexicalCheck(String query, ScanConfig config) {
    StringBuilder bareText = new StringBuilder(query.length() + 16);
    int statementsWithContent = 0;
    boolean currentStatementHasContent = false;
    boolean sawExtendedString = false;
    int i = 0;
    int n = query.length();

    while (i < n) {
      char c = query.charAt(i);
      if (c == '-' && i + 1 < n && query.charAt(i + 1) == '-' && isDashDashCommentStart(query, i, config)) {
        i = skipToEndOfLine(query, i + 2);
        bareText.append(' ');
      } else if (config.hashComments && c == '#') {
        i = skipToEndOfLine(query, i + 1);
        bareText.append(' ');
      } else if (c == '/' && i + 1 < n && query.charAt(i + 1) == '*') {
        if (isExecutableCommentStart(query, i)) {
          throw new IllegalArgumentException(
            "Executable SQL comments ('/*!' or '/*M!') are not allowed in the query.");
        }
        i = skipBlockComment(query, i, config.nestedBlockComments);
        bareText.append(' ');
      } else if (isOracleAlternativeQuoteStart(query, i)) {
        throw new IllegalArgumentException("Alternative-quoted string literals (q'...') are not allowed in the query.");
      } else if (c == '$' && isDollarQuoteStart(query, i)) {
        throw new IllegalArgumentException(
          "Dollar-quoted string literals ($$...$$ or $tag$...$tag$) are not allowed in the query.");
      } else if (config.postgresExtendedStrings && isPostgresExtendedStringStart(query, i)) {
        i = skipPostgresExtendedString(query, i + 1, config.nestedBlockComments);
        sawExtendedString = true;
        currentStatementHasContent = true;
        bareText.append(' ').append(STRING_PLACEHOLDER).append(' ');
      } else if (c == '\'') {
        i = skipString(query, i, '\'', config.backslashEscapesInSingleQuotes);
        currentStatementHasContent = true;
        bareText.append(' ').append(STRING_PLACEHOLDER).append(' ');
      } else if (c == '"') {
        if (config.backslashEscapesInDoubleQuotes) {
          i = skipString(query, i, '"', true);
        } else {
          i = skipQuotedIdentifier(query, i, '"');
        }
        currentStatementHasContent = true;
        bareText.append(' ').append(QUOTED_IDENT_PLACEHOLDER).append(' ');
      } else if (c == '`') {
        i = skipQuotedIdentifier(query, i, '`');
        currentStatementHasContent = true;
        bareText.append(' ').append(QUOTED_IDENT_PLACEHOLDER).append(' ');
      } else if (config.squareBracketQuotes && c == '[') {
        i = skipBracketIdentifier(query, i);
        currentStatementHasContent = true;
        bareText.append(' ').append(QUOTED_IDENT_PLACEHOLDER).append(' ');
      } else if (c == ';') {
        if (currentStatementHasContent) {
          statementsWithContent++;
        }
        currentStatementHasContent = false;
        bareText.append(' ');
        i++;
      } else {
        if (!isSqlWhitespace(c)) {
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
    return sawExtendedString;
  }

  private static boolean isDashDashCommentStart(String query, int i, ScanConfig config) {
    if (!config.mysqlDashDashComments) {
      return true;
    }
    // MySQL and MariaDB treat '--' as a comment only before whitespace, a control character, or end of input.
    if (i + 2 >= query.length()) {
      return true;
    }
    char after = query.charAt(i + 2);
    return isSqlWhitespace(after) || Character.isISOControl(after);
  }

  private static boolean isExecutableCommentStart(String query, int i) {
    int n = query.length();
    if (i + 2 < n && query.charAt(i + 2) == '!') {
      return true;
    }
    return i + 3 < n && (query.charAt(i + 2) == 'M' || query.charAt(i + 2) == 'm') && query.charAt(i + 3) == '!';
  }

  private static boolean isOracleAlternativeQuoteStart(String query, int i) {
    if (i > 0 && isIdentifierChar(query.charAt(i - 1))) {
      return false;
    }
    char c = query.charAt(i);
    int n = query.length();
    if ((c == 'q' || c == 'Q') && i + 1 < n && query.charAt(i + 1) == '\'') {
      return true;
    }
    return (c == 'n' || c == 'N') && i + 2 < n
      && (query.charAt(i + 1) == 'q' || query.charAt(i + 1) == 'Q')
      && query.charAt(i + 2) == '\'';
  }

  private static boolean isPostgresExtendedStringStart(String query, int i) {
    if (i > 0 && isIdentifierChar(query.charAt(i - 1))) {
      return false;
    }
    char c = query.charAt(i);
    return (c == 'e' || c == 'E') && i + 1 < query.length() && query.charAt(i + 1) == '\'';
  }

  private static boolean isDollarQuoteStart(String query, int i) {
    // In PostgreSQL, '$' is a valid continuation character inside an identifier (e.g. v$session, dbms_aq$aq$_jms).
    // Walk back across any preceding identifier characters: if the token began with a letter or '_', it is an
    // ordinary identifier rather than a dollar-quote opener.
    if (i > 0 && isIdentifierChar(query.charAt(i - 1))) {
      int k = i - 1;
      while (k > 0 && isIdentifierChar(query.charAt(k - 1))) {
        k--;
      }
      char tokenFirst = query.charAt(k);
      if (!Character.isDigit(tokenFirst) && tokenFirst != '$') {
        return false;
      }
    }
    if (!DOLLAR_QUOTE_AT_START.matcher(query.substring(i)).find()) {
      return false;
    }
    int closingDollar = query.indexOf('$', i + 1);
    for (int p = i + 1; p < closingDollar; p++) {
      if (!isIdentifierChar(query.charAt(p)) || query.charAt(p) == '$') {
        return false;
      }
    }
    return true;
  }

  private static void checkKeywords(String bareText) {
    List<WordToken> tokens = new ArrayList<>();
    int i = 0;
    int n = bareText.length();
    while (i < n) {
      char c = bareText.charAt(i);
      if (isIdentifierChar(c)) {
        int start = i;
        while (i < n && isIdentifierChar(bareText.charAt(i))) {
          i++;
        }
        String raw = bareText.substring(start, i);
        String upper = raw.toUpperCase(Locale.ROOT);
        boolean canQualify = QUOTED_IDENT_PLACEHOLDER.equals(upper)
          || (!STRING_PLACEHOLDER.equals(upper) && !Character.isDigit(c));
        tokens.add(new WordToken(upper, start, i, canQualify));
      } else {
        i++;
      }
    }

    checkJdbcCallEscape(bareText, tokens);

    for (int w = 0; w < tokens.size(); w++) {
      WordToken token = tokens.get(w);
      String word = token.word;
      if (STRING_PLACEHOLDER.equals(word) || QUOTED_IDENT_PLACEHOLDER.equals(word)) {
        continue;
      }
      if (isDotQualifiedIdentifier(bareText, tokens, w)) {
        continue;
      }
      if ("CALL".equals(word) && w == 0) {
        throw new IllegalArgumentException("The 'CALL' keyword is not allowed. The query must only read data.");
      }
      if ("FOR".equals(word) && w + 1 < tokens.size() && LOCKING_AFTER_FOR.contains(tokens.get(w + 1).word)) {
        throw new IllegalArgumentException(
          "FOR UPDATE/SHARE locking clauses are not allowed. The query must only read data.");
      }
      if ("LOCK".equals(word) && w + 2 < tokens.size()
        && "IN".equals(tokens.get(w + 1).word) && "SHARE".equals(tokens.get(w + 2).word)) {
        throw new IllegalArgumentException(
          "LOCK IN SHARE MODE clauses are not allowed. The query must only read data.");
      }
      if (WRITE_KEYWORDS.contains(word) && !isExemptWriteKeyword(bareText, tokens, w)) {
        throw new IllegalArgumentException(String.format(
          "The '%s' keyword is not allowed. The query must only read data.", word));
      }
    }

    boolean hasSelect = tokens.stream().anyMatch(t -> "SELECT".equals(t.word));
    if (!hasSelect) {
      throw new IllegalArgumentException("Only a SELECT statement is allowed. No SELECT keyword was found.");
    }

    WordToken first = tokens.get(0);
    if (!ALLOWED_START_KEYWORDS.contains(first.word)) {
      throw new IllegalArgumentException(String.format(
        "Only a SELECT statement is allowed. Query cannot start with '%s'.", first.word));
    }
    if ("LOCKING".equals(first.word) && !hasReadOnlyTeradataLockingPrefix(tokens)) {
      throw new IllegalArgumentException(
        "Only LOCKING ... FOR ACCESS or FOR READ modifiers are allowed before SELECT.");
    }
  }

  private static void checkJdbcCallEscape(String bareText, List<WordToken> tokens) {
    // Reject JDBC callable escape syntax: {call proc(...)} or {?= call proc(...)}.
    for (WordToken token : tokens) {
      if (!"CALL".equals(token.word)) {
        continue;
      }
      int p = skipWhitespaceBackward(bareText, token.start - 1);
      if (p >= 0 && bareText.charAt(p) == '=') {
        p = skipWhitespaceBackward(bareText, p - 1);
        if (p >= 0 && bareText.charAt(p) == '?') {
          p = skipWhitespaceBackward(bareText, p - 1);
        }
      }
      if (p >= 0 && bareText.charAt(p) == '{') {
        throw new IllegalArgumentException("The 'CALL' keyword is not allowed. The query must only read data.");
      }
    }
  }

  private static boolean isDotQualifiedIdentifier(String bareText, List<WordToken> tokens, int w) {
    if (w == 0) {
      return false;
    }
    WordToken prev = tokens.get(w - 1);
    if (!prev.canQualifyColumn) {
      return false;
    }
    int dotPos = skipWhitespaceBackward(bareText, tokens.get(w).start - 1);
    if (dotPos < 0 || bareText.charAt(dotPos) != '.') {
      return false;
    }
    int beforeDot = skipWhitespaceBackward(bareText, dotPos - 1);
    return beforeDot == prev.end - 1;
  }

  private static boolean isExemptWriteKeyword(String bareText, List<WordToken> tokens, int w) {
    String word = tokens.get(w).word;
    if (FUNCTION_COLLIDING_WRITE_KEYWORDS.contains(word)
      && w > 0
      && isFollowedByOpenParen(bareText, tokens.get(w).end)) {
      return true;
    }
    if ("MERGE".equals(word) && w > 0 && w + 1 < tokens.size()) {
      String next = tokens.get(w + 1).word;
      if ("JOIN".equals(next) || "UNION".equals(next)) {
        return true;
      }
    }
    if ("SET".equals(word) && w > 0 && "CHARACTER".equals(tokens.get(w - 1).word)) {
      // Allow SQL standard / MySQL 'CAST(x AS CHAR CHARACTER SET utf8mb4)', while rejecting 'SELECT 1 AS character SET'
      boolean precededByAs = w >= 2 && "AS".equals(tokens.get(w - 2).word);
      return !precededByAs;
    }
    return false;
  }

  private static boolean hasReadOnlyTeradataLockingPrefix(List<WordToken> tokens) {
    for (int w = 1; w < tokens.size(); w++) {
      String word = tokens.get(w).word;
      if ("SELECT".equals(word) || "WITH".equals(word)) {
        return false;
      }
      if ("FOR".equals(word) && w + 1 < tokens.size()) {
        String mode = tokens.get(w + 1).word;
        return "ACCESS".equals(mode) || "READ".equals(mode);
      }
    }
    return false;
  }

  private static int skipWhitespaceBackward(String text, int fromIndex) {
    int i = fromIndex;
    while (i >= 0 && isSqlWhitespace(text.charAt(i))) {
      i--;
    }
    return i;
  }

  private static boolean isFollowedByOpenParen(String text, int fromOffset) {
    int i = fromOffset;
    while (i < text.length() && isSqlWhitespace(text.charAt(i))) {
      i++;
    }
    return i < text.length() && text.charAt(i) == '(';
  }

  private static boolean isIdentifierChar(char c) {
    if (isSqlWhitespace(c) || Character.isISOControl(c)) {
      return false;
    }
    return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c >= 0x80;
  }

  private static boolean isSqlWhitespace(char c) {
    return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\uFEFF';
  }

  private static int skipToEndOfLine(String q, int i) {
    while (i < q.length() && q.charAt(i) != '\n' && q.charAt(i) != '\r') {
      i++;
    }
    return i;
  }

  private static int skipBlockComment(String q, int i, boolean nested) {
    i += 2;
    int depth = 1;
    while (i + 1 < q.length()) {
      if (nested && q.charAt(i) == '/' && q.charAt(i + 1) == '*') {
        if (isExecutableCommentStart(q, i)) {
          throw new IllegalArgumentException(
            "Executable SQL comments ('/*!' or '/*M!') are not allowed in the query.");
        }
        depth++;
        i += 2;
        continue;
      }
      if (q.charAt(i) == '*' && q.charAt(i + 1) == '/') {
        depth--;
        i += 2;
        if (depth == 0) {
          return i;
        }
        continue;
      }
      i++;
    }
    throw new IllegalArgumentException("Unterminated block comment in the query.");
  }

  private static int skipPostgresExtendedString(String q, int quoteIdx, boolean nestedBlockComments) {
    int next = skipString(q, quoteIdx, '\'', true);
    // PostgreSQL's lexer concatenates adjacent string literals in <xe> mode when separated by whitespace/comments
    // containing at least one newline ('quotecontinue').
    while (next < q.length()) {
      int p = next;
      boolean sawNewline = false;
      while (p < q.length()) {
        char c = q.charAt(p);
        if (c == '\n' || c == '\r') {
          sawNewline = true;
          p++;
        } else if (isSqlWhitespace(c)) {
          p++;
        } else if (c == '-' && p + 1 < q.length() && q.charAt(p + 1) == '-') {
          p = skipToEndOfLine(q, p + 2);
        } else if (c == '/' && p + 1 < q.length() && q.charAt(p + 1) == '*') {
          int commentEnd = skipBlockComment(q, p, nestedBlockComments);
          for (int k = p + 2; k < commentEnd - 2; k++) {
            if (q.charAt(k) == '\n' || q.charAt(k) == '\r') {
              sawNewline = true;
              break;
            }
          }
          p = commentEnd;
        } else {
          break;
        }
      }
      if (sawNewline && p < q.length() && q.charAt(p) == '\'') {
        next = skipString(q, p, '\'', true);
      } else {
        break;
      }
    }
    return next;
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
   * and so must not carry operators, function calls, comments, or additional statements.
   *
   * @param value the value to validate
   * @param dialect the database SQL dialect
   * @throws IllegalArgumentException if the value is not a single column reference
   */
  public static void validateColumnReference(String value, Dialect dialect) {
    Objects.requireNonNull(dialect, "Dialect must not be null.");
    validateColumnReference(value, dialect.usesSquareBracketQuotes());
  }

  /**
   * Validates that the given value is a single, bare column reference (optionally qualified and/or quoted).
   *
   * @param value the value to validate
   * @param squareBracketQuotes whether square brackets quote identifiers, as in SQL Server
   * @throws IllegalArgumentException if the value is not a single column reference
   */
  public static void validateColumnReference(String value, boolean squareBracketQuotes) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Must not be empty.");
    }
    if (value.indexOf(';') >= 0 || value.indexOf('\\') >= 0 || value.indexOf('#') >= 0
      || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0
      || value.contains("--") || value.contains("/*")
      || containsUnquotedDollarQuote(value, squareBracketQuotes)) {
      throw new IllegalArgumentException("Must be a single column name.");
    }
    Expression expression;
    try {
      // allowPartialParse=false so trailing content (operators, extra tokens) is rejected rather than ignored.
      expression = CCJSqlParserUtil.parseExpression(value, false,
        parser -> parser.withSquareBracketQuotation(squareBracketQuotes).withTimeOut(PARSE_TIMEOUT_MS));
    } catch (JSQLParserException | TokenMgrException e) {
      throw new IllegalArgumentException("Must be a single column name.", e);
    }
    if (!(expression instanceof Column) || "*".equals(((Column) expression).getColumnName())) {
      throw new IllegalArgumentException("Must be a single column name.");
    }
  }

  private static boolean containsUnquotedDollarQuote(String value, boolean squareBracketQuotes) {
    if (value.indexOf('$') < 0) {
      return false;
    }
    int i = 0;
    int n = value.length();
    try {
      while (i < n) {
        char c = value.charAt(i);
        if (c == '"' || c == '`') {
          i = skipQuotedIdentifier(value, i, c);
        } else if (squareBracketQuotes && c == '[') {
          i = skipBracketIdentifier(value, i);
        } else if (c == '$' && isDollarQuoteStart(value, i)) {
          return true;
        } else {
          i++;
        }
      }
    } catch (IllegalArgumentException e) {
      return false;
    }
    return false;
  }
}
