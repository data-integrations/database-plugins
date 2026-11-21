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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractDBSpecificConnector#quoteIdentifier(String, char)}
 */
public class QuoteIdentifierTest {

  @Test
  public void testPlainIdentifier() {
    Assert.assertEquals("\"table\"", AbstractDBSpecificConnector.quoteIdentifier("table", '"'));
    Assert.assertEquals("`table`", AbstractDBSpecificConnector.quoteIdentifier("table", '`'));
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
}
