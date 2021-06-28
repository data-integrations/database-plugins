/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.postgres;

import io.cdap.plugin.db.connector.DBSpecificConnectorBaseTest;
import org.junit.Test;

import java.io.IOException;


public class PostgresConnectorTest extends DBSpecificConnectorBaseTest {

  private static final String JDBC_DRIVER_CLASS_NAME = "org.postgresql.Driver";

  @Test
  public void test() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    test(new PostgresConnector(
           new PostgresConnectorConfig(host, port, username, password, JDBC_PLUGIN_NAME, connectionArguments)),
         JDBC_DRIVER_CLASS_NAME, PostgresConstants.PLUGIN_NAME);
  }
}

