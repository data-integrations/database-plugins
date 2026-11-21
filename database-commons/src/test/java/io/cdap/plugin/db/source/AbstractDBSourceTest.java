/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.db.source;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Test class for {@link AbstractDBSource} schema, query validation, and read-only schema fetch.
 */
public class AbstractDBSourceTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("string_column", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("bytes_column", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
    Schema.Field.of("int_column", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("long_column", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("float_column", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
    Schema.Field.of("double_column", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("boolean_column", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))
  );
  private static final AbstractDBSource.DBSourceConfig TEST_CONFIG = new AbstractDBSource.DBSourceConfig() {
    @Override
    public String getConnectionString() {
      return "";
    }
  };

  @Test
  public void testValidateSourceSchemaCorrectSchema() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    TEST_CONFIG.validateSchema(SCHEMA, SCHEMA, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateSourceSchemaMismatchFields() {
    Schema actualSchema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("string_column", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("bytes_column", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("int_column", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long_column", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float_column", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double_column", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))
    );

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    TEST_CONFIG.validateSchema(actualSchema, SCHEMA, collector);
    assertPropertyValidationFailed(collector, "boolean_column");
  }

  @Test
  public void testValidateSourceSchemaInvalidFieldType() {
    Schema actualSchema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("string_column", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("bytes_column", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("int_column", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long_column", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float_column", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double_column", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("boolean_column", Schema.nullableOf(Schema.of(Schema.Type.INT)))
    );

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    TEST_CONFIG.validateSchema(actualSchema, SCHEMA, collector);
    assertPropertyValidationFailed(collector, "boolean_column");
  }

  @Test
  public void testConfigurePipelineValidatesImportBoundingAndSplitByWithExplicitSchema() {
    TestDBSourceConfig config = new TestDBSourceConfig(
      "SELECT * FROM t WHERE $CONDITIONS; DELETE FROM t",
      "SELECT MIN(id), MAX(id) FROM t; DROP TABLE t",
      "id'",
      2,
      SCHEMA,
      null);
    TestDBSource source = new TestDBSource(config);
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    PipelineConfigurer configurer = mockPipelineConfigurer(collector);

    source.configurePipeline(configurer);

    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(3, failures.size());
    assertConfigFailure(failures.get(0), AbstractDBSource.DBSourceConfig.IMPORT_QUERY,
                        "Invalid Import Query: Only a single SELECT statement is allowed. Found 2 statements.");
    assertConfigFailure(failures.get(1), AbstractDBSource.DBSourceConfig.BOUNDING_QUERY,
                        "Invalid Bounding Query: Only a single SELECT statement is allowed. Found 2 statements.");
    assertConfigFailure(failures.get(2), AbstractDBSource.DBSourceConfig.SPLIT_BY,
                        "Invalid Split-By Field Name: Must be a single column name.");
  }

  @Test
  public void testExecuteSchemaQuerySetsReadOnlyDisablesAutoCommitAndRollsBack() throws SQLException {
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery("SELECT * FROM t")).thenReturn(resultSet);
    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getColumnCount()).thenReturn(1);
    Mockito.when(metaData.getColumnName(1)).thenReturn("id");
    Mockito.when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
    Mockito.when(metaData.getColumnTypeName(1)).thenReturn("INTEGER");
    Mockito.when(metaData.isSigned(1)).thenReturn(true);
    Mockito.when(metaData.isNullable(1)).thenReturn(ResultSetMetaData.columnNoNulls);

    TestDBSourceConfig config = new TestDBSourceConfig("SELECT * FROM t WHERE $CONDITIONS", null, null, 1, null, false);
    TestDBSource source = new TestDBSource(config);
    Schema fetched = source.executeSchemaQuery(connection);
    Assert.assertNotNull(fetched.getField("id"));

    InOrder inOrder = Mockito.inOrder(connection, statement, resultSet);
    inOrder.verify(connection).setAutoCommit(false);
    inOrder.verify(connection).setReadOnly(true);
    inOrder.verify(connection).createStatement();
    inOrder.verify(statement).setMaxRows(1);
    inOrder.verify(statement).executeQuery("SELECT * FROM t");
    inOrder.verify(resultSet).close();
    inOrder.verify(statement).close();
    inOrder.verify(connection).rollback();
    Mockito.verify(connection, Mockito.never()).commit();
  }

  @Test
  public void testDisableQueryValidationAndReadOnlyFlagOnLegacyAndConnectorConfigs() throws IOException {
    TestDBSourceConfig enabledConfig = new TestDBSourceConfig(
      "SELECT * FROM t WHERE $CONDITIONS",
      "SELECT MIN(id), MAX(id) FROM t",
      "id",
      2,
      SCHEMA,
      false);
    TestDBSource enabledSource = new TestDBSource(enabledConfig);
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    ConnectionConfigAccessor enabledAccessor =
      enabledSource.getConnectionConfigAccessor(Driver.class.getName(), SCHEMA, collector);
    Assert.assertTrue(enabledAccessor.isReadOnly());

    TestDBSourceConfig disabledConfig = new TestDBSourceConfig(
      "SELECT * FROM t WHERE $CONDITIONS; DELETE FROM t",
      "SELECT MIN(id), MAX(id) FROM t; DROP TABLE t",
      "id'",
      2,
      SCHEMA,
      true);
    TestDBSource disabledSource = new TestDBSource(disabledConfig);
    MockFailureCollector disabledCollector = new MockFailureCollector(MOCK_STAGE);
    disabledSource.configurePipeline(mockPipelineConfigurer(disabledCollector));
    Assert.assertTrue(disabledCollector.getValidationFailures().isEmpty());
    ConnectionConfigAccessor disabledAccessor =
      disabledSource.getConnectionConfigAccessor(Driver.class.getName(), SCHEMA, disabledCollector);
    Assert.assertFalse(disabledAccessor.isReadOnly());

    TestConnectorConfig connectorWithoutOptOut = new TestConnectorConfig(false);
    TestConnectorSourceConfig connectorSourceEnabled = new TestConnectorSourceConfig(connectorWithoutOptOut);
    Assert.assertFalse(connectorSourceEnabled.isQueryValidationDisabled());

    TestConnectorConfig connectorWithOptOut = new TestConnectorConfig(true);
    TestConnectorSourceConfig connectorSourceDisabled = new TestConnectorSourceConfig(connectorWithOptOut);
    Assert.assertTrue(connectorSourceDisabled.isQueryValidationDisabled());
  }

  private static PipelineConfigurer mockPipelineConfigurer(MockFailureCollector collector) {
    PipelineConfigurer pipelineConfigurer = Mockito.mock(PipelineConfigurer.class);
    StageConfigurer stageConfigurer = Mockito.mock(StageConfigurer.class);
    Mockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(stageConfigurer);
    Mockito.when(stageConfigurer.getFailureCollector()).thenReturn(collector);
    Mockito.doReturn(Driver.class).when(pipelineConfigurer).usePluginClass(
      ArgumentMatchers.anyString(),
      ArgumentMatchers.anyString(),
      ArgumentMatchers.anyString(),
      ArgumentMatchers.any(PluginProperties.class));
    return pipelineConfigurer;
  }

  private static void assertConfigFailure(ValidationFailure failure, String expectedProperty, String expectedMessage) {
    Assert.assertEquals(expectedMessage, failure.getMessage());
    List<ValidationFailure.Cause> causes = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals(expectedProperty, causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private static void assertPropertyValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.OUTPUT_SCHEMA_FIELD);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String attribute) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(attribute) != null)
      .collect(Collectors.toList());
  }

  private static final class TestDBSource extends AbstractDBSource<TestDBSourceConfig> {
    TestDBSource(TestDBSourceConfig sourceConfig) {
      super(sourceConfig);
    }

    @Override
    protected String createConnectionString() {
      return sourceConfig.getConnectionString();
    }
  }

  private static final class TestDBSourceConfig extends AbstractDBSource.DBSourceConfig {
    private final Schema explicitSchema;

    TestDBSourceConfig(String importQuery, String boundingQuery, String splitBy, Integer numSplits,
                       Schema explicitSchema, Boolean disableQueryValidation) {
      this.referenceName = "testRef";
      this.importQuery = importQuery;
      this.boundingQuery = boundingQuery;
      this.splitBy = splitBy;
      this.numSplits = numSplits;
      this.explicitSchema = explicitSchema;
      this.disableQueryValidation = disableQueryValidation;
    }

    @Override
    public String getJdbcPluginName() {
      return "testDriver";
    }

    @Override
    public String getConnectionString() {
      return "jdbc:hsqldb:mem:test";
    }

    @Override
    public Schema getSchema() {
      return explicitSchema;
    }

    @Override
    public Integer getMaxRetryCount() {
      return 0;
    }
  }

  private static final class TestConnectorConfig extends AbstractDBConnectorConfig {
    TestConnectorConfig(Boolean disableValidation) {
      this.disableQueryValidation = disableValidation;
    }

    @Override
    public String getConnectionString() {
      return "jdbc:postgresql://localhost:5432/db";
    }

    @Override
    public boolean canConnect() {
      return false;
    }
  }

  private static final class TestConnectorSourceConfig extends AbstractDBSpecificSourceConfig {
    private final AbstractDBConnectorConfig connection;

    TestConnectorSourceConfig(AbstractDBConnectorConfig connection) {
      this.connection = connection;
    }

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      return Collections.emptyMap();
    }

    @Override
    protected AbstractDBConnectorConfig getConnection() {
      return connection;
    }
  }
}
