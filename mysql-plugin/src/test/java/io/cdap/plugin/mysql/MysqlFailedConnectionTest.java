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

package io.cdap.plugin.mysql;

import io.cdap.plugin.db.connector.DBSpecificFailedConnectionTest;
import org.junit.Test;

import java.io.IOException;

public class MysqlFailedConnectionTest extends DBSpecificFailedConnectionTest {
  private static final String JDBC_DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  @Test
  public void test() throws ClassNotFoundException, IOException {

    MysqlConnector connector = new MysqlConnector(
      new MysqlConnectorConfig("localhost", 3306, "username", "password", "jdbc", ""));

    super.test(JDBC_DRIVER_CLASS_NAME, connector, "Failed to create connection to database via connection string: " +
                                                    "jdbc:mysql://localhost:3306 and arguments: {user=username, " +
                                                    "rewriteBatchedStatements=true, "  +
                                                    "connectTimeout=20000, socketTimeout=20000}. Error: " +
                                                    "ConnectException: Connection refused (Connection refused).");
  }
}
