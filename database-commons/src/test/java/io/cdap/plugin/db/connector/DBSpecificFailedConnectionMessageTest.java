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

public class DBSpecificFailedConnectionMessageTest {

  @Test
  public void testNormalizeConnectionArgumentsReordersProperties() {
    String jdk8Order = "Failed to create connection to database via connection string: " +
      "jdbc:mysql://localhost:3306 and arguments: {user=username, rewriteBatchedStatements=true, " +
      "connectTimeout=20000, tinyInt1isBit=false, socketTimeout=20000}. Error: " +
      "ConnectException: Connection refused (Connection refused).";
    String jdk11Order = "Failed to create connection to database via connection string: " +
      "jdbc:mysql://localhost:3306 and arguments: {rewriteBatchedStatements=true, tinyInt1isBit=false, " +
      "connectTimeout=20000, socketTimeout=20000, user=username}. Error: " +
      "ConnectException: Connection refused (Connection refused).";

    Assert.assertEquals(DBSpecificFailedConnectionTest.normalizeConnectionArguments(jdk8Order),
                        DBSpecificFailedConnectionTest.normalizeConnectionArguments(jdk11Order));
  }

  @Test
  public void testNormalizeConnectionArgumentsDetectsMismatchedValues() {
    String expected = "Failed to create connection to database via connection string: " +
      "jdbc:mysql://localhost:3306 and arguments: {user=username, connectTimeout=20000}. Error: " +
      "ConnectException: Connection refused.";
    String mismatchedValue = "Failed to create connection to database via connection string: " +
      "jdbc:mysql://localhost:3306 and arguments: {connectTimeout=30000, user=username}. Error: " +
      "ConnectException: Connection refused.";

    Assert.assertNotEquals(DBSpecificFailedConnectionTest.normalizeConnectionArguments(expected),
                           DBSpecificFailedConnectionTest.normalizeConnectionArguments(mismatchedValue));
  }

  @Test
  public void testNormalizeConnectionArgumentsHandlesEmptyAndUnformattedMessages() {
    String emptyArgs = "Failed to create connection to database via connection string: " +
      "jdbc:h2:mem:test and arguments: {}. Error: SQLException.";
    Assert.assertEquals(emptyArgs, DBSpecificFailedConnectionTest.normalizeConnectionArguments(emptyArgs));
    Assert.assertEquals("Plain error", DBSpecificFailedConnectionTest.normalizeConnectionArguments("Plain error"));
    Assert.assertNull(DBSpecificFailedConnectionTest.normalizeConnectionArguments(null));
  }
}
