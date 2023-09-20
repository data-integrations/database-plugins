/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.amazon.redshift;

import io.cdap.plugin.db.connector.DBSpecificFailedConnectionTest;
import org.junit.Test;

import java.io.IOException;

public class RedshiftFailedConnectionTest extends DBSpecificFailedConnectionTest {
  private static final String JDBC_DRIVER_CLASS_NAME = "com.amazon.redshift.Driver";

  @Test
  public void test() throws ClassNotFoundException, IOException {

    RedshiftConnector connector = new RedshiftConnector(
      new RedshiftConnectorConfig("username", "password", "jdbc", "", "localhost", "db", 5432));

    super.test(JDBC_DRIVER_CLASS_NAME, connector, "Failed to create connection to database via connection string: " +
      "jdbc:redshift://localhost:5432/db and arguments: " +
      "{user=username}. Error: ConnectException: Connection refused " +
      "(Connection refused).");
  }
}
