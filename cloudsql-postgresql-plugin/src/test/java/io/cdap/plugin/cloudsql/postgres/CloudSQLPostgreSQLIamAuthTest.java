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

package io.cdap.plugin.cloudsql.postgres;

import io.cdap.plugin.cloudsql.postgres.CloudSQLPostgreSQLAction.CloudSQLPostgreSQLActionConfig;
import io.cdap.plugin.util.CloudSQLUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

/**
 * Tests for IAM database authentication in the CloudSQL PostgreSQL plugins.
 */
public class CloudSQLPostgreSQLIamAuthTest {

  private static final String SERVICE_ACCOUNT = "my-sa@my-project.iam.gserviceaccount.com";
  private static final String IAM_USER = "my-sa@my-project.iam";
  private static final String CONNECTION_NAME = "my-project:us-west1:my-instance";

  @Test
  public void testConnectorBasicAuthentication() {
    CloudSQLPostgreSQLConnectorConfig config =
      connectorConfig(CloudSQLUtil.PUBLIC_INSTANCE, CONNECTION_NAME, "basic");
    Properties properties = config.getConnectionArgumentsProperties();
    Assert.assertEquals("dbuser", properties.getProperty("user"));
    Assert.assertEquals("dbpassword", properties.getProperty("password"));
    Assert.assertNull(properties.getProperty(CloudSQLUtil.ENABLE_IAM_AUTH_PROPERTY));
    Assert.assertNull(properties.getProperty(CloudSQLUtil.TARGET_PRINCIPAL_PROPERTY));
  }

  @Test
  public void testConnectorWithoutAuthenticationTypeUsesBasic() {
    CloudSQLPostgreSQLConnectorConfig config =
      connectorConfig(CloudSQLUtil.PUBLIC_INSTANCE, CONNECTION_NAME, null);
    Assert.assertFalse(config.isIamAuthentication());
    Assert.assertEquals("dbuser", config.getUser());
    Assert.assertEquals("dbpassword", config.getPassword());
  }

  @Test
  public void testConnectorIamAuthenticationPublicInstance() {
    CloudSQLPostgreSQLConnectorConfig config =
      connectorConfig(CloudSQLUtil.PUBLIC_INSTANCE, CONNECTION_NAME, "iam");
    Assert.assertEquals(IAM_USER, config.getUser());
    Assert.assertEquals(CloudSQLUtil.IAM_AUTH_PASSWORD_PLACEHOLDER, config.getPassword());
    Properties properties = config.getConnectionArgumentsProperties();
    Assert.assertEquals(IAM_USER, properties.getProperty("user"));
    Assert.assertEquals(CloudSQLUtil.IAM_AUTH_PASSWORD_PLACEHOLDER, properties.getProperty("password"));
    Assert.assertEquals("true", properties.getProperty(CloudSQLUtil.ENABLE_IAM_AUTH_PROPERTY));
    Assert.assertEquals(SERVICE_ACCOUNT, properties.getProperty(CloudSQLUtil.TARGET_PRINCIPAL_PROPERTY));
  }

  @Test
  public void testConnectorIamAuthenticationPrivateInstance() {
    CloudSQLPostgreSQLConnectorConfig config =
      connectorConfig(CloudSQLUtil.PRIVATE_INSTANCE, "10.0.0.2", "iam");
    Assert.assertEquals(IAM_USER, config.getUser());
    Assert.assertNull(config.getPassword());
    Properties properties = config.getConnectionArgumentsProperties();
    Assert.assertEquals(IAM_USER, properties.getProperty("user"));
    Assert.assertNull(properties.getProperty("password"));
    Assert.assertNull(properties.getProperty(CloudSQLUtil.ENABLE_IAM_AUTH_PROPERTY));
    Assert.assertNull(properties.getProperty(CloudSQLUtil.TARGET_PRINCIPAL_PROPERTY));
  }

  @Test
  public void testActionIamAuthenticationPublicInstance() {
    CloudSQLPostgreSQLActionConfig config = actionConfig(CloudSQLUtil.PUBLIC_INSTANCE, CONNECTION_NAME);
    Map<String, String> arguments = config.getConnectionArguments();
    Assert.assertEquals(IAM_USER, arguments.get("user"));
    Assert.assertEquals(CloudSQLUtil.IAM_AUTH_PASSWORD_PLACEHOLDER, arguments.get("password"));
    Assert.assertEquals("true", arguments.get(CloudSQLUtil.ENABLE_IAM_AUTH_PROPERTY));
    Assert.assertEquals(SERVICE_ACCOUNT, arguments.get(CloudSQLUtil.TARGET_PRINCIPAL_PROPERTY));
  }

  @Test
  public void testActionIamAuthenticationPrivateInstance() {
    CloudSQLPostgreSQLActionConfig config = actionConfig(CloudSQLUtil.PRIVATE_INSTANCE, "10.0.0.2");
    Map<String, String> arguments = config.getConnectionArguments();
    Assert.assertEquals(IAM_USER, arguments.get("user"));
    Assert.assertFalse(arguments.containsKey("password"));
    Assert.assertFalse(arguments.containsKey(CloudSQLUtil.ENABLE_IAM_AUTH_PROPERTY));
    // The action passes these arguments to the driver as Properties, which rejects null values.
    Properties properties = new Properties();
    properties.putAll(arguments);
  }

  private static CloudSQLPostgreSQLConnectorConfig connectorConfig(String instanceType, String connectionName,
      String authenticationType) {
    return new CloudSQLPostgreSQLConnectorConfig("dbuser", "dbpassword", "cloudsql-postgresql", null, instanceType,
      connectionName, "mydb", null, authenticationType, SERVICE_ACCOUNT);
  }

  private static CloudSQLPostgreSQLActionConfig actionConfig(String instanceType, String connectionName) {
    CloudSQLPostgreSQLActionConfig config = new CloudSQLPostgreSQLActionConfig();
    config.instanceType = instanceType;
    config.connectionName = connectionName;
    config.database = "mydb";
    config.authenticationType = CloudSQLUtil.AUTHENTICATION_TYPE_IAM;
    config.serviceAccountEmail = SERVICE_ACCOUNT;
    return config;
  }
}
