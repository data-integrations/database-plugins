/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.util;

import com.google.common.net.InetAddresses;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for CloudSQL .
 */
public class CloudSQLUtil {
  public static final String CONNECTION_NAME = "connectionName";
  public static final String CONNECTION_NAME_PATTERN = "[a-z0-9-]+:[a-z0-9-]+:[a-z0-9-]+";
  public static final String INSTANCE_TYPE = "instanceType";
  public static final String PUBLIC_INSTANCE = "public";
  public static final String PRIVATE_INSTANCE = "private";
  public static final String CLOUDSQL_POSTGRESQL = "CloudSQL PostgreSQL";
  public static final String CLOUDSQL_MYSQL = "CloudSQL MySQL";


  /**
   * Utility method to check the Connection Name format of a CloudSQL instance.
   *
   * @param failureCollector {@link FailureCollector} for the pipeline
   * @param instanceType CloudSQL instance type
   * @param connectionName Connection Name for the CloudSQL instance
   * @param databaseType Type of CloudSQL instance- CloudSQL PostgreSQL, CLoudSQL MySQL
   */
  public static void checkConnectionName(
    FailureCollector failureCollector, String instanceType, String connectionName, String databaseType) {

    if (PUBLIC_INSTANCE.equalsIgnoreCase(instanceType)) {
      Pattern connectionNamePattern =
        Pattern.compile(CONNECTION_NAME_PATTERN);
      Matcher matcher = connectionNamePattern.matcher(connectionName);

      if (!matcher.matches()) {
        failureCollector
          .addFailure(
            String.format("Connection Name must be in the format <PROJECT_ID>:<REGION>:<INSTANCE_NAME> to connect to "
              + "a public %s instance.", databaseType), null)
          .withConfigProperty(CONNECTION_NAME);
      }
    } else {
      if (!InetAddresses.isInetAddress(connectionName)) {
        failureCollector
          .addFailure(
            String.format("Enter the internal IP address of the Compute Engine VM cloudsql proxy "
              + "is running on, to connect to a private %s instance.", databaseType), null)
          .withConfigProperty(CONNECTION_NAME);
      }
    }
  }
}
