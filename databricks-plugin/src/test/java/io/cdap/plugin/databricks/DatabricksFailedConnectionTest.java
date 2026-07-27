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

package io.cdap.plugin.databricks;

import io.cdap.plugin.db.connector.DBSpecificFailedConnectionTest;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;

/**
 * Test failed connection handling for {@link DatabricksConnector}.
 */
public class DatabricksFailedConnectionTest extends DBSpecificFailedConnectionTest {
  private static final String JDBC_DRIVER_CLASS_NAME = "com.databricks.client.jdbc.Driver";

  @Test
  public void test() throws ClassNotFoundException, IOException {
    DatabricksConnector connector = new DatabricksConnector(
      new DatabricksConnectorConfig("token", "password", "jdbc", "", "localhost",
                                    "sql/1.0/warehouses/test", "db", 443));

    try {
      super.test(JDBC_DRIVER_CLASS_NAME, connector,
                 "Failed to create connection to database via connection string: " +
                   "jdbc:databricks://localhost:443;ConnCatalog=db;HttpPath=sql/1.0/warehouses/test; " +
                   "and arguments: {user=token}. Error: DatabricksHttpException: " +
                   "Caught error while executing http request: [https://localhost:443/sql/1.0/warehouses/test]. " +
                   "Error Message: [com.databricks.internal.apache.http.conn.HttpHostConnectException: " +
                   "Connect to localhost:443 [localhost/127.0.0.1] failed: Connection refused (Connection refused)].");
    } catch (UnsupportedClassVersionError e) {
      Assume.assumeNoException(e);
    }
  }
}
