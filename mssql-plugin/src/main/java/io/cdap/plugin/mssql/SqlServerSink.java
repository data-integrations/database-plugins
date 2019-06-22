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

package io.cdap.plugin.mssql;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Sink support for a MSSQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(SqlServerConstants.PLUGIN_NAME)
@Description("Writes records to a MSSQL table. Each record will be written in a row in the table")
public class SqlServerSink extends AbstractDBSink {
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerSink.class);

  private final SqlServerSinkConfig sqlServerSinkConfig;

  public SqlServerSink(SqlServerSinkConfig sqlServerSinkConfig) {
    super(sqlServerSinkConfig);
    this.sqlServerSinkConfig = sqlServerSinkConfig;
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SqlServerSinkSchemaReader();
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord.Builder output) {
    return new SqlServerSinkDBRecord(output.build(), columnTypes);
  }

  @Override
  protected boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {

    Schema.Type fieldType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType()
      : field.getSchema().getType();

    // DATETIMEOFFSET type is mapped to Schema.Type.STRING
    int type = metadata.getColumnType(index);
    if (SqlServerSinkSchemaReader.DATETIME_OFFSET_TYPE == type && !Objects.equals(fieldType, Schema.Type.STRING)) {
      LOG.error("Field '{}' was given as type '{}' but must be of type 'string' for the MS SQL column of " +
                  "DATETIMEOFFSET type.", field.getName(), fieldType);
      return false;
    }

    if (SqlServerSinkSchemaReader.GEOMETRY_TYPE != type && SqlServerSinkSchemaReader.GEOGRAPHY_TYPE != type) {
      return super.isFieldCompatible(field, metadata, index);
    }

    // Value of GEOMETRY and GEOGRAPHY type can be set as Well Known Text string such as "POINT(3 40 5 6)"
    if (!Objects.equals(fieldType, Schema.Type.BYTES) && !Objects.equals(fieldType, Schema.Type.STRING)) {
      LOG.error("Field '{}' was given as type '{}' but must be of type 'bytes' or 'string' for the MS SQL column of " +
                  "GEOMETRY/GEOGRAPHY type.", field.getName(), fieldType);
      return false;
    } else {
      return true;
    }
  }

  /**
   * MSSQL action configuration.
   */
  public static class SqlServerSinkConfig extends DBSpecificSinkConfig {

    @Name(SqlServerConstants.INSTANCE_NAME)
    @Description(SqlServerConstants.INSTANCE_NAME_DESCRIPTION)
    @Nullable
    public String instanceName;

    @Name(SqlServerConstants.QUERY_TIMEOUT)
    @Description(SqlServerConstants.QUERY_TIMEOUT_DESCRIPTION)
    @Nullable
    public Integer queryTimeout = -1;

    @Name(SqlServerConstants.AUTHENTICATION)
    @Description(SqlServerConstants.AUTHENTICATION_DESCRIPTION)
    @Nullable
    public String authenticationType;

    @Name(SqlServerConstants.CONNECT_TIMEOUT)
    @Description(SqlServerConstants.CONNECT_TIMEOUT_DESCRIPTION)
    @Nullable
    public Integer connectTimeout;

    @Name(SqlServerConstants.COLUMN_ENCRYPTION)
    @Description(SqlServerConstants.COLUMN_ENCRYPTION_DESCRIPTION)
    @Nullable
    public Boolean columnEncryption;

    @Name(SqlServerConstants.ENCRYPT)
    @Description(SqlServerConstants.ENCRYPT_DESCRIPTION)
    @Nullable
    public Boolean encrypt;

    @Name(SqlServerConstants.TRUST_SERVER_CERTIFICATE)
    @Description(SqlServerConstants.TRUST_SERVER_CERTIFICATE_DESCRIPTION)
    @Nullable
    public Boolean trustServerCertificate;

    @Name(SqlServerConstants.WORKSTATION_ID)
    @Description(SqlServerConstants.WORKSTATION_ID_DESCRIPTION)
    @Nullable
    public String workstationId;

    @Name(SqlServerConstants.FAILOVER_PARTNER)
    @Description(SqlServerConstants.FAILOVER_PARTNER_DESCRIPTION)
    @Nullable
    public String failoverPartner;

    @Name(SqlServerConstants.PACKET_SIZE)
    @Description(SqlServerConstants.PACKET_SIZE_DESCRIPTION)
    @Nullable
    public Integer packetSize;

    @Name(SqlServerConstants.CURRENT_LANGUAGE)
    @Description(SqlServerConstants.CURRENT_LANGUAGE_DESCRIPTION)
    @Nullable
    public String currentLanguage;

    @Override
    public String getConnectionString() {
      return String.format(SqlServerConstants.SQL_SERVER_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return SqlServerUtil.composeDbSpecificArgumentsMap(instanceName, authenticationType, null,
                                                         connectTimeout, columnEncryption, encrypt,
                                                         trustServerCertificate, workstationId, failoverPartner,
                                                         packetSize, queryTimeout);
    }

    @Override
    public List<String> getInitQueries() {
      if (!Strings.isNullOrEmpty(currentLanguage)) {
        return Collections.singletonList(String.format(SqlServerConstants.SET_LANGUAGE_QUERY_FORMAT, currentLanguage));
      }

      return Collections.emptyList();
    }
  }
}
