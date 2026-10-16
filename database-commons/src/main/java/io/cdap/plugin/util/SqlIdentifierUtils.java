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

import javax.annotation.Nullable;

/**
 * Utilities for safely embedding SQL identifiers (database, schema, table and column names) into SQL text.
 *
 * <p>Identifiers cannot be bound as JDBC parameters, so the only way to use an untrusted identifier in a query is to
 * quote it and escape any embedded quote characters. Quoting makes the identifier case-sensitive and allows any
 * character, which is the desired behavior since the names handled by the connectors come straight from the database
 * metadata.
 */
public final class SqlIdentifierUtils {

  /** Identifier quote character used by PostgreSQL, Oracle, SQL Server (QUOTED_IDENTIFIER ON), Redshift, etc. */
  public static final char ANSI_QUOTE = '"';
  /** Identifier quote character used by MySQL / MariaDB. */
  public static final char MYSQL_QUOTE = '`';

  private SqlIdentifierUtils() {
    // utility class
  }

  /**
   * Quotes the given identifier with the ANSI double quote character, escaping any embedded double quotes.
   *
   * @param identifier the raw identifier
   * @return the quoted identifier, e.g. {@code "my""table"} for the input {@code my"table}
   * @throws IllegalArgumentException if the identifier is null, empty or contains a NUL character
   */
  public static String quoteAnsiIdentifier(@Nullable String identifier) {
    return quoteIdentifier(identifier, ANSI_QUOTE);
  }

  /**
   * Quotes the given identifier with the MySQL backtick character, escaping any embedded backticks.
   *
   * @param identifier the raw identifier
   * @return the quoted identifier, e.g. {@code `my``table`} for the input {@code my`table}
   * @throws IllegalArgumentException if the identifier is null, empty or contains a NUL character
   */
  public static String quoteMySqlIdentifier(@Nullable String identifier) {
    return quoteIdentifier(identifier, MYSQL_QUOTE);
  }

  /**
   * Quotes the given identifier with the given quote character. Any occurrence of the quote character inside the
   * identifier is escaped by doubling it, which is the escaping rule shared by all SQL dialects for quoted
   * identifiers. NUL characters are rejected because they are never valid in an identifier and can be used to
   * truncate strings in some drivers / servers.
   *
   * @param identifier the raw identifier
   * @param quoteChar the quote character of the SQL dialect
   * @return the quoted and escaped identifier
   * @throws IllegalArgumentException if the identifier is null, empty or contains a NUL character
   */
  public static String quoteIdentifier(@Nullable String identifier, char quoteChar) {
    if (identifier == null || identifier.isEmpty()) {
      throw new IllegalArgumentException("SQL identifier must not be null or empty.");
    }
    if (identifier.indexOf('\0') >= 0) {
      throw new IllegalArgumentException("SQL identifier must not contain NUL characters.");
    }
    StringBuilder quoted = new StringBuilder(identifier.length() + 2);
    quoted.append(quoteChar);
    for (int i = 0; i < identifier.length(); i++) {
      char c = identifier.charAt(i);
      if (c == quoteChar) {
        quoted.append(quoteChar);
      }
      quoted.append(c);
    }
    quoted.append(quoteChar);
    return quoted.toString();
  }
}
