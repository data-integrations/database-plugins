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

package io.cdap.plugin.db;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

public class ConnectionConfigTest {

  @Test
  public void testConnectionArgumentsWithUserAndPassword() {
    Map<String, String> arguments = ConnectionConfig.getConnectionArguments("key=value", "user", "secret");
    Assert.assertEquals("value", arguments.get("key"));
    Assert.assertEquals("user", arguments.get("user"));
    Assert.assertEquals("secret", arguments.get("password"));
  }

  @Test
  public void testConnectionArgumentsWithUserAndNoPassword() {
    Map<String, String> arguments = ConnectionConfig.getConnectionArguments(null, "user", null);
    Assert.assertEquals("user", arguments.get("user"));
    Assert.assertFalse(arguments.containsKey("password"));
    // Used to throw a NullPointerException because of a null password value.
    Properties properties = new Properties();
    properties.putAll(arguments);
    Assert.assertEquals("user", properties.getProperty("user"));
  }

  @Test
  public void testConnectionArgumentsWithoutUser() {
    Map<String, String> arguments = ConnectionConfig.getConnectionArguments(null, null, "secret");
    Assert.assertFalse(arguments.containsKey("user"));
    Assert.assertFalse(arguments.containsKey("password"));
  }
}
