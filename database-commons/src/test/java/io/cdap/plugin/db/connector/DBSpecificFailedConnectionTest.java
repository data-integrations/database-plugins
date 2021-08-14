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

package io.cdap.plugin.db.connector;

import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import org.junit.Assert;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

public class DBSpecificFailedConnectionTest {
  private static final String JDBC_DRIVER_CLASS_NAME = "oracle.jdbc.OracleDriver";

  protected void test(String jdbcClassName, AbstractDBSpecificConnector connector, String expectedErrorMsg)
    throws ClassNotFoundException, IOException {

    ConnectorConfigurer configurer = Mockito.mock(ConnectorConfigurer.class);
    Mockito.when(configurer.usePluginClass(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                                           Mockito.any(PluginProperties.class)))
      .thenReturn((Class) DBSpecificFailedConnectionTest.class.getClassLoader().loadClass(jdbcClassName));


    connector.configure(configurer);
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    connector.test(context);
    List<ValidationFailure> validationFailures = context.getFailureCollector().getValidationFailures();
    Assert.assertEquals(1, validationFailures.size());
    Assert.assertEquals(expectedErrorMsg, validationFailures.get(0).getMessage());
  }
}
