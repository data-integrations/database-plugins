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

package io.cdap.plugin.cloudsql.postgres;

import io.cdap.plugin.db.connector.DBSpecificConnectorBaseTest;
import org.apache.parquet.Strings;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for CloudSQL PostgreSQL Connector, it will only be run when below properties are provided:
 * -DinstanceType -- Whether the CloudSQL instance to connect to is private or public. If not provided, assumed public
 * -DconnectionName -- The CloudSQL instance to connect to. For a public instance, the connection string should be in
 * the format <PROJECT_ID>:<REGION>:<INSTANCE_NAME> which can be found in the instance overview page.
 * For a private instance, enter the internal IP address of the Compute Engine VM cloudsql proxy is running on.
 */
public class CloudSQLPostgreSQLConnectorTest extends DBSpecificConnectorBaseTest {

  private static final String JDBC_DRIVER_CLASS_NAME = "com.google.cloud.sql.postgres.SocketFactory";
  private static String instanceType;
  private static String connectionName;

  @BeforeClass
  public static void doSetup() {
    // Certain properties need to be configured otherwise the whole tests will be skipped.
    String messageTemplate = "%s is not configured, please refer to javadoc of this class for details.";
    instanceType = System.getProperty("instanceType");
    if (Strings.isNullOrEmpty(instanceType)) {
      instanceType = "public";
    }
    connectionName = System.getProperty("connectionName");
    Assume.assumeFalse(String.format(messageTemplate, "connectionName"), connectionName == null);
  }

  @Test
  public void test() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    test(new CloudSQLPostgreSQLConnector(
           new CloudSQLPostgreSQLConnectorConfig(username, password, JDBC_PLUGIN_NAME, connectionArguments,
                                                 instanceType, connectionName, database)), JDBC_DRIVER_CLASS_NAME,
         CloudSQLPostgreSQLConstants.PLUGIN_NAME);
  }
}
