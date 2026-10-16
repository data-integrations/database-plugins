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
 * Unit tests for {@link SqlIdentifierUtils}.
 */
public class SqlIdentifierUtilsTest {

  /** Table component of the sample request path used in the original vulnerability report. */
  private static final String INJECTION_PAYLOAD =
    "pg_class\" LIMIT 0; UPDATE pg_catalog.pg_class SET relname = 'x'; SELECT * FROM \"pg_catalog\".\"pg_class";

  @Test
  public void testPlainIdentifierIsOnlyQuoted() {
    Assert.assertEquals("\"my_table\"", SqlIdentifierUtils.quoteAnsiIdentifier("my_table"));
    Assert.assertEquals("`my_table`", SqlIdentifierUtils.quoteMySqlIdentifier("my_table"));
    Assert.assertEquals("\"MixedCase\"", SqlIdentifierUtils.quoteIdentifier("MixedCase", '"'));
  }

  @Test
  public void testEmbeddedQuoteCharactersAreDoubled() {
    Assert.assertEquals("\"my\"\"table\"", SqlIdentifierUtils.quoteAnsiIdentifier("my\"table"));
    Assert.assertEquals("\"\"\"\"", SqlIdentifierUtils.quoteAnsiIdentifier("\""));
    Assert.assertEquals("\"a\"\"\"\"b\"", SqlIdentifierUtils.quoteAnsiIdentifier("a\"\"b"));
    Assert.assertEquals("`my``table`", SqlIdentifierUtils.quoteMySqlIdentifier("my`table"));
  }

  @Test
  public void testOtherDialectQuoteCharactersAreLeftUntouched() {
    // a backtick is a regular character inside an ANSI quoted identifier and vice versa
    Assert.assertEquals("\"my`table\"", SqlIdentifierUtils.quoteAnsiIdentifier("my`table"));
    Assert.assertEquals("`my\"table`", SqlIdentifierUtils.quoteMySqlIdentifier("my\"table"));
  }

  @Test
  public void testSpecialCharactersStayInsideTheIdentifier() {
    Assert.assertEquals("\"a;b -- c /* d */ 'e' \n f\"",
                        SqlIdentifierUtils.quoteAnsiIdentifier("a;b -- c /* d */ 'e' \n f"));
  }

  @Test
  public void testInjectionPayloadCannotBreakOutOfTheIdentifier() {
    String quoted = SqlIdentifierUtils.quoteAnsiIdentifier(INJECTION_PAYLOAD);
    Assert.assertEquals("\"pg_class\"\" LIMIT 0; UPDATE pg_catalog.pg_class SET relname = 'x'; "
                          + "SELECT * FROM \"\"pg_catalog\"\".\"\"pg_class\"", quoted);
    // Every quote inside the identifier must be doubled: the number of quotes between the delimiters is even.
    String inner = quoted.substring(1, quoted.length() - 1);
    Assert.assertEquals(0, inner.replace("\"\"", "").chars().filter(c -> c == '"').count());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullIdentifierIsRejected() {
    SqlIdentifierUtils.quoteAnsiIdentifier(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyIdentifierIsRejected() {
    SqlIdentifierUtils.quoteAnsiIdentifier("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNulCharacterIsRejected() {
    SqlIdentifierUtils.quoteAnsiIdentifier("table\0name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNulCharacterIsRejectedForMySql() {
    SqlIdentifierUtils.quoteMySqlIdentifier("table\0name");
  }
}
