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

import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

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

  public static final String AUTHENTICATION_TYPE = "authenticationType";
  public static final String AUTHENTICATION_TYPE_BASIC = "basic";
  public static final String AUTHENTICATION_TYPE_IAM = "iam";
  public static final String SERVICE_ACCOUNT_EMAIL = "serviceAccountEmail";

  // Connection properties understood by the Cloud SQL Java connector (socket factory).
  public static final String ENABLE_IAM_AUTH_PROPERTY = "enableIamAuth";
  public static final String TARGET_PRINCIPAL_PROPERTY = "cloudSqlTargetPrincipal";
  // The Cloud SQL Java connector requires a non-empty password with IAM authentication, but ignores its value.
  public static final String IAM_AUTH_PASSWORD_PLACEHOLDER = "cloudsql-iam-auth";

  private static final String SERVICE_ACCOUNT_SUFFIX = ".gserviceaccount.com";
  private static final Pattern EMAIL_PATTERN = Pattern.compile("[^@\\s]+@[^@\\s]+\\.[^@\\s]+");

  /**
   * Returns whether IAM database authentication is selected.
   *
   * @param authenticationType the configured authentication type, null means basic authentication
   */
  public static boolean isIamAuthentication(@Nullable String authenticationType) {
    return AUTHENTICATION_TYPE_IAM.equalsIgnoreCase(authenticationType);
  }

  /**
   * Returns the Cloud SQL database user name of an IAM principal. PostgreSQL uses the email without the
   * {@code .gserviceaccount.com} suffix, MySQL uses the part of the email before the {@code @}.
   *
   * @param email IAM principal email, e.g. {@code my-sa@my-project.iam.gserviceaccount.com}
   * @param databaseType {@link #CLOUDSQL_POSTGRESQL} or {@link #CLOUDSQL_MYSQL}
   */
  @Nullable
  public static String getIamDatabaseUser(@Nullable String email, String databaseType) {
    if (Strings.isNullOrEmpty(email)) {
      return null;
    }
    String trimmedEmail = email.trim();
    if (CLOUDSQL_MYSQL.equals(databaseType)) {
      int atIndex = trimmedEmail.indexOf('@');
      return atIndex < 0 ? trimmedEmail : trimmedEmail.substring(0, atIndex);
    }
    return trimmedEmail.endsWith(SERVICE_ACCOUNT_SUFFIX)
      ? trimmedEmail.substring(0, trimmedEmail.length() - SERVICE_ACCOUNT_SUFFIX.length())
      : trimmedEmail;
  }

  /**
   * Returns the password to send with IAM authentication. Public instances connect through the Cloud SQL Java
   * connector, which needs a non-empty placeholder. Private instances connect through the Cloud SQL Auth Proxy,
   * which performs the IAM login itself, so no password is sent.
   */
  @Nullable
  public static String getIamPassword(@Nullable String instanceType) {
    return PRIVATE_INSTANCE.equalsIgnoreCase(instanceType) ? null : IAM_AUTH_PASSWORD_PLACEHOLDER;
  }

  /**
   * Returns the JDBC connection properties that enable IAM authentication in the Cloud SQL Java connector.
   * The connector impersonates the given service account, so the database login is the same at design time and
   * at runtime. Private instances connect through the Cloud SQL Auth Proxy and need no extra properties.
   */
  public static Map<String, String> getIamConnectionArguments(@Nullable String instanceType,
                                                              @Nullable String serviceAccountEmail) {
    if (PRIVATE_INSTANCE.equalsIgnoreCase(instanceType) || Strings.isNullOrEmpty(serviceAccountEmail)) {
      return Collections.emptyMap();
    }
    Map<String, String> arguments = new HashMap<>();
    arguments.put(ENABLE_IAM_AUTH_PROPERTY, Boolean.TRUE.toString());
    arguments.put(TARGET_PRINCIPAL_PROPERTY, serviceAccountEmail.trim());
    return arguments;
  }

  /**
   * Validates the authentication properties of a CloudSQL plugin.
   *
   * @param collector {@link FailureCollector} for the pipeline
   * @param authenticationType configured authentication type, null means basic authentication
   * @param instanceType CloudSQL instance type
   * @param serviceAccountEmail configured service account email
   * @param serviceAccountEmailIsMacro whether the service account email is a macro
   */
  public static void validateAuthentication(FailureCollector collector, @Nullable String authenticationType,
                                            @Nullable String instanceType, @Nullable String serviceAccountEmail,
                                            boolean serviceAccountEmailIsMacro) {
    if (authenticationType != null && !AUTHENTICATION_TYPE_BASIC.equalsIgnoreCase(authenticationType)
      && !isIamAuthentication(authenticationType)) {
      collector.addFailure(String.format("Invalid authentication type '%s'.", authenticationType),
                           String.format("Use '%s' or '%s'.", AUTHENTICATION_TYPE_BASIC, AUTHENTICATION_TYPE_IAM))
        .withConfigProperty(AUTHENTICATION_TYPE);
      return;
    }
    if (!isIamAuthentication(authenticationType) || serviceAccountEmailIsMacro) {
      return;
    }
    if (Strings.isNullOrEmpty(serviceAccountEmail)) {
      collector.addFailure("Service Account Email is required for IAM authentication.", null)
        .withConfigProperty(SERVICE_ACCOUNT_EMAIL);
      return;
    }
    String email = serviceAccountEmail.trim();
    if (!EMAIL_PATTERN.matcher(email).matches()) {
      collector.addFailure(String.format("Service Account Email '%s' is not a valid email address.", email), null)
        .withConfigProperty(SERVICE_ACCOUNT_EMAIL);
      return;
    }
    if (!PRIVATE_INSTANCE.equalsIgnoreCase(instanceType) && !email.endsWith(SERVICE_ACCOUNT_SUFFIX)) {
      collector.addFailure(
          String.format("'%s' is not a service account. IAM authentication to a public instance impersonates a "
                          + "service account.", email),
          "Enter a service account email ending with '" + SERVICE_ACCOUNT_SUFFIX + "'.")
        .withConfigProperty(SERVICE_ACCOUNT_EMAIL);
    }
  }


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
