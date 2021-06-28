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

import io.cdap.plugin.db.connector.DBSpecificConnectorBaseTest;
import org.junit.Test;

import java.io.IOException;


public class SqlServerConnectorTest extends DBSpecificConnectorBaseTest {

  private static final String JDBC_DRIVER_CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  @Test
  public void test() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    test(new SqlServerConnector(
           new SqlServerConnectorConfig(host, port, username, password, JDBC_PLUGIN_NAME, connectionArguments)),
         JDBC_DRIVER_CLASS_NAME, SqlServerConstants.PLUGIN_NAME);
  }
}

