/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for CloudSQL PostgreSQL.
 */
public class CloudSQLPostgreSQLUtil {

  /**
   * Utility method to check the Connection Name format of a CloudSQL PostgreSQL instance.
   *
   * @param failureCollector {@link FailureCollector} for the pipeline
   * @param instanceType CloudSQL PostgreSQL instance type
   * @param connectionName Connection Name for the CloudSQL PostgreSQL instance
   */
  public static void checkConnectionName(
    FailureCollector failureCollector, String instanceType, String connectionName) {

    if (Strings.isNullOrEmpty(instanceType)) {
      failureCollector
        .addFailure(
          "Cloud SQL Instance Type cannot be null or empty", null)
        .withConfigProperty(CloudSQLPostgreSQLConstants.INSTANCE_TYPE);
    }

    if (Strings.isNullOrEmpty(connectionName)) {
      failureCollector
        .addFailure(
          "Cloud SQL Connection Name cannot be null or empty", null)
        .withConfigProperty(CloudSQLPostgreSQLConstants.CONNECTION_NAME);
    }

    if (!failureCollector.getValidationFailures().isEmpty()) {
      return;
    }

    if (CloudSQLPostgreSQLConstants.PUBLIC_INSTANCE.equalsIgnoreCase(instanceType)) {
      Pattern connectionNamePattern =
        Pattern.compile(CloudSQLPostgreSQLConstants.CONNECTION_NAME_PATTERN);
      Matcher matcher = connectionNamePattern.matcher(connectionName);

      if (!matcher.matches()) {
        failureCollector
          .addFailure(
            "Connection Name must be in the format <PROJECT_ID>:<REGION>:<INSTANCE_NAME> to connect to "
              + "a public CloudSQL PostgreSQL instance.", null)
          .withConfigProperty(CloudSQLPostgreSQLConstants.CONNECTION_NAME);
      }
    } else {
      if (!InetAddresses.isInetAddress(connectionName)) {
        failureCollector
          .addFailure(
            "Enter the internal IP address of the Compute Engine VM cloudsql proxy "
              + "is running on, to connect to a private CloudSQL PostgreSQL instance.", null)
          .withConfigProperty(CloudSQLPostgreSQLConstants.CONNECTION_NAME);
      }
    }
  }
}
