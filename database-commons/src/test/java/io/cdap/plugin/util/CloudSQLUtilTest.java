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

package io.cdap.plugin.util;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CloudSQLUtilTest {

  private static final String SERVICE_ACCOUNT = "my-sa@my-project.iam.gserviceaccount.com";

  @Test
  public void testIsIamAuthentication() {
    Assert.assertTrue(CloudSQLUtil.isIamAuthentication("iam"));
    Assert.assertTrue(CloudSQLUtil.isIamAuthentication("IAM"));
    Assert.assertFalse(CloudSQLUtil.isIamAuthentication("basic"));
    Assert.assertFalse(CloudSQLUtil.isIamAuthentication(null));
  }

  @Test
  public void testIamDatabaseUser() {
    Assert.assertEquals("my-sa@my-project.iam",
                        CloudSQLUtil.getIamDatabaseUser(SERVICE_ACCOUNT, CloudSQLUtil.CLOUDSQL_POSTGRESQL));
    Assert.assertEquals("my-sa", CloudSQLUtil.getIamDatabaseUser(SERVICE_ACCOUNT, CloudSQLUtil.CLOUDSQL_MYSQL));
    Assert.assertEquals("user@example.com",
                        CloudSQLUtil.getIamDatabaseUser(" user@example.com ", CloudSQLUtil.CLOUDSQL_POSTGRESQL));
    Assert.assertEquals("user", CloudSQLUtil.getIamDatabaseUser("user@example.com", CloudSQLUtil.CLOUDSQL_MYSQL));
    Assert.assertNull(CloudSQLUtil.getIamDatabaseUser(null, CloudSQLUtil.CLOUDSQL_MYSQL));
    Assert.assertNull(CloudSQLUtil.getIamDatabaseUser("", CloudSQLUtil.CLOUDSQL_POSTGRESQL));
  }

  @Test
  public void testIamPassword() {
    Assert.assertEquals(CloudSQLUtil.IAM_AUTH_PASSWORD_PLACEHOLDER,
                        CloudSQLUtil.getIamPassword(CloudSQLUtil.PUBLIC_INSTANCE));
    Assert.assertEquals(CloudSQLUtil.IAM_AUTH_PASSWORD_PLACEHOLDER, CloudSQLUtil.getIamPassword(null));
    Assert.assertNull(CloudSQLUtil.getIamPassword(CloudSQLUtil.PRIVATE_INSTANCE));
  }

  @Test
  public void testIamConnectionArgumentsForPublicInstance() {
    Map<String, String> arguments =
      CloudSQLUtil.getIamConnectionArguments(CloudSQLUtil.PUBLIC_INSTANCE, SERVICE_ACCOUNT);
    Assert.assertEquals(2, arguments.size());
    Assert.assertEquals("true", arguments.get(CloudSQLUtil.ENABLE_IAM_AUTH_PROPERTY));
    Assert.assertEquals(SERVICE_ACCOUNT, arguments.get(CloudSQLUtil.TARGET_PRINCIPAL_PROPERTY));
  }

  @Test
  public void testIamConnectionArgumentsForPrivateInstance() {
    Assert.assertTrue(CloudSQLUtil.getIamConnectionArguments(CloudSQLUtil.PRIVATE_INSTANCE, SERVICE_ACCOUNT)
                        .isEmpty());
  }

  @Test
  public void testValidateBasicAuthentication() {
    Assert.assertTrue(validate(null, CloudSQLUtil.PUBLIC_INSTANCE, null, false).isEmpty());
    Assert.assertTrue(validate("basic", CloudSQLUtil.PUBLIC_INSTANCE, null, false).isEmpty());
  }

  @Test
  public void testValidateInvalidAuthenticationType() {
    assertSingleFailure(validate("oauth", CloudSQLUtil.PUBLIC_INSTANCE, null, false),
                        CloudSQLUtil.AUTHENTICATION_TYPE);
  }

  @Test
  public void testValidateIamAuthentication() {
    Assert.assertTrue(validate("iam", CloudSQLUtil.PUBLIC_INSTANCE, SERVICE_ACCOUNT, false).isEmpty());
    Assert.assertTrue(validate("iam", CloudSQLUtil.PRIVATE_INSTANCE, SERVICE_ACCOUNT, false).isEmpty());
    Assert.assertTrue(validate("iam", CloudSQLUtil.PRIVATE_INSTANCE, "user@example.com", false).isEmpty());
    Assert.assertTrue(validate("iam", CloudSQLUtil.PUBLIC_INSTANCE, "${sa}", true).isEmpty());
  }

  @Test
  public void testValidateIamAuthenticationWithoutServiceAccount() {
    assertSingleFailure(validate("iam", CloudSQLUtil.PUBLIC_INSTANCE, null, false),
                        CloudSQLUtil.SERVICE_ACCOUNT_EMAIL);
  }

  @Test
  public void testValidateIamAuthenticationWithInvalidEmail() {
    assertSingleFailure(validate("iam", CloudSQLUtil.PRIVATE_INSTANCE, "not-an-email", false),
                        CloudSQLUtil.SERVICE_ACCOUNT_EMAIL);
  }

  @Test
  public void testValidateIamAuthenticationPublicInstanceRequiresServiceAccount() {
    assertSingleFailure(validate("iam", CloudSQLUtil.PUBLIC_INSTANCE, "user@example.com", false),
                        CloudSQLUtil.SERVICE_ACCOUNT_EMAIL);
  }

  private static List<ValidationFailure> validate(String authenticationType, String instanceType,
                                                  String serviceAccountEmail, boolean isMacro) {
    MockFailureCollector collector = new MockFailureCollector();
    CloudSQLUtil.validateAuthentication(collector, authenticationType, instanceType, serviceAccountEmail, isMacro);
    return collector.getValidationFailures();
  }

  private static void assertSingleFailure(List<ValidationFailure> failures, String property) {
    Assert.assertEquals(1, failures.size());
    Assert.assertEquals(property, failures.get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
