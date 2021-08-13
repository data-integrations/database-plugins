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

package io.cdap.plugin.mssql;

import io.cdap.plugin.db.connector.DBSpecificFailedConnectionTest;
import org.junit.Test;

import java.io.IOException;

public class SqlServerFailedConnectionTest extends DBSpecificFailedConnectionTest {
  private static final String JDBC_DRIVER_CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  @Test
  public void test() throws ClassNotFoundException, IOException {

    SqlServerConnector connector = new SqlServerConnector(
      new SqlServerConnectorConfig("localhost", 1433, "username", "password", "jdbc", ""));

    super.test(JDBC_DRIVER_CLASS_NAME, connector, "Failed to create connection to database via connection string: " +
                                                    "jdbc:sqlserver://localhost:1433 and arguments: {user=username}. " +
                                                    "Error: SQLServerException: The TCP/IP connection to the host " +
                                                    "localhost, port 1433 has failed. Error: \"Connection refused " +
                                                    "(Connection refused). Verify the connection properties. Make " +
                                                    "sure that an instance of SQL Server is running on the host and " +
                                                    "accepting TCP/IP connections at the port. Make sure that TCP " +
                                                    "connections to the port are not blocked by a firewall.\"..");
  }
}
