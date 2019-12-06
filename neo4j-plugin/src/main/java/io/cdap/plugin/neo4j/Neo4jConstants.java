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

package io.cdap.plugin.neo4j;

import io.cdap.cdap.api.data.schema.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Neo4j constants.
 */
public final class Neo4jConstants {
  public static final String NEO4J_CONNECTION_STRING_FORMAT = "jdbc:neo4j:bolt://%s:%s/?username=%s,password=%s";
  public static final String NAME_REFERENCE_NAME = "referenceName";
  public static final String NAME_DRIVER_NAME = "jdbcPluginName";
  public static final String NAME_HOST_STRING = "neo4jHost";
  public static final String NAME_PORT_STRING = "neo4jPort";
  public static final String NAME_USERNAME = "username";
  public static final String NAME_PASSWORD = "password";

  public static final String OUTPUT_QUERY = "cdap.neo4j.output.query";

  public static final List<Schema.Field> DURATION_RECORD_FIELDS = Collections.unmodifiableList(Arrays.asList(
    Schema.Field.of("duration", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("seconds", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("months", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("days", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("nanoseconds", Schema.of(Schema.Type.INT))
  ));

  public static final List<Schema.Field> POINT_2D_RECORD_FIELDS = Collections.unmodifiableList(Arrays.asList(
    Schema.Field.of("srid", Schema.of(Schema.Type.INT)),
    Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("y", Schema.of(Schema.Type.DOUBLE))
  ));

  public static final List<Schema.Field> POINT_3D_RECORD_FIELDS = Collections.unmodifiableList(Arrays.asList(
    Schema.Field.of("srid", Schema.of(Schema.Type.INT)),
    Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("y", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("z", Schema.of(Schema.Type.DOUBLE))
  ));

  public static final List<String> NEO4J_SYS_FIELDS = Collections.unmodifiableList(Arrays.asList(
    "_id",
    "_labels",
    "_type",
    "_startId",
    "_endId"
  ));
}
