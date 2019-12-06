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

package io.cdap.plugin.neo4j.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.neo4j.Neo4jConstants;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Batch source to read from Neo4j.
 */
public class Neo4jSinkConfig extends PluginConfig {

  private static final List<String> UNAVAILABLE_QUERY_KEYWORDS =
    Arrays.asList("UNWIND", "DELETE", "SET", "REMOVE");

  public static final String NAME_OUTPUT_QUERY = "outputQuery";

  @Name(Neo4jConstants.NAME_REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  private String referenceName;

  @Macro
  @Name(Neo4jConstants.NAME_DRIVER_NAME)
  @Description("Name of the JDBC driver to use. This is the value of the 'jdbcPluginName' key defined in the JSON " +
    "file for the JDBC plugin.")
  private String jdbcPluginName;

  @Macro
  @Name(Neo4jConstants.NAME_HOST_STRING)
  @Description("Neo4j database host.")
  private String neo4jHost;

  @Macro
  @Name(Neo4jConstants.NAME_PORT_STRING)
  @Description("Neo4j database port.")
  private Integer neo4jPort;

  @Macro
  @Name(Neo4jConstants.NAME_USERNAME)
  @Description("User to use to connect to the Neo4j database.")
  private String username;

  @Macro
  @Name(Neo4jConstants.NAME_PASSWORD)
  @Description("Password to use to connect to the Neo4j database.")
  private String password;

  @Name(NAME_OUTPUT_QUERY)
  @Description("The query to use to export data to the Neo4j database. Query example: " +
    "'CREATE (n:<label_field> $(*))' or 'CREATE (n:<label_field> $(property_1, property_2))'")
  private String outputQuery;

  public Neo4jSinkConfig(String referenceName, String jdbcPluginName, String neo4jHost, Integer neo4jPort,
                         String username, String password, String outputQuery) {
    this.referenceName = referenceName;
    this.jdbcPluginName = jdbcPluginName;
    this.neo4jHost = neo4jHost;
    this.neo4jPort = neo4jPort;
    this.username = username;
    this.password = password;
    this.outputQuery = outputQuery;
  }

  private Neo4jSinkConfig(Builder builder) {
    referenceName = builder.referenceName;
    jdbcPluginName = builder.jdbcPluginName;
    neo4jHost = builder.neo4jHost;
    neo4jPort = builder.neo4jPort;
    username = builder.username;
    password = builder.password;
    outputQuery = builder.outputQuery;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Neo4jSinkConfig copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setJdbcPluginName(copy.jdbcPluginName)
      .setNeo4jHost(copy.neo4jHost)
      .setNeo4jPort(copy.neo4jPort)
      .setUsername(copy.username)
      .setPassword(copy.password)
      .setOutputQuery(copy.outputQuery);
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getJdbcPluginName() {
    return jdbcPluginName;
  }

  public String getNeo4jHost() {
    return neo4jHost;
  }

  public int getNeo4jPort() {
    return neo4jPort == null ? 0 : neo4jPort;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getOutputQuery() {
    return outputQuery;
  }

  public String getConnectionString() {
    return String.format(Neo4jConstants.NEO4J_CONNECTION_STRING_FORMAT, getNeo4jHost(), getNeo4jPort(),
                         getUsername(), getPassword());
  }

  public void validate(FailureCollector collector, Schema inputSchema) {

    if (UNAVAILABLE_QUERY_KEYWORDS.stream().parallel().anyMatch(outputQuery.toUpperCase()::contains)) {
      collector.addFailure(
        String.format("The input request must not contain any of the following keywords: '%s'",
                      UNAVAILABLE_QUERY_KEYWORDS.toString()),
        "Proved correct Input query.")
        .withConfigProperty(NAME_OUTPUT_QUERY);
    }

    List<String> schemaFields = inputSchema.getFields().stream().map(Schema.Field::getName)
      .collect(Collectors.toList());

    if (!outputQuery.toUpperCase().startsWith("CREATE")) {
      collector.addFailure("Output query must start with 'CREATE' keyword.", null)
        .withConfigProperty(NAME_OUTPUT_QUERY);
    }

    String regex = "\\$\\([^()]+\\)";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(outputQuery);
    if (!matcher.find()) {
      collector.addFailure("Output query must contained at least one block of properties '$(...)'.",
                           "Provide correct output query.")
        .withConfigProperty(NAME_OUTPUT_QUERY);
      return;
    }

    matcher = pattern.matcher(outputQuery);

    while (matcher.find()) {
      String group = matcher.group();
      if (group.equals("$(*)")) {
        continue;
      }
      group = group.substring(group.indexOf("(") + 1, group.indexOf(")"));
      List<String> values = Arrays.stream(group.split(",")).map(String::trim).collect(Collectors.toList());
      if (values.isEmpty()) {
        collector.addFailure("The block of properties can not be empty.",
                             "Provide block of properties in format '$(*)' or '$(property_1, property_2)'.")
          .withConfigProperty(NAME_OUTPUT_QUERY);
      }
      for (String value : values) {
        if (!schemaFields.contains(value)) {
          collector.addFailure("Property '%s' not exists in input schema.",
                               "Provide property that present in input schema.")
            .withConfigProperty(NAME_OUTPUT_QUERY)
            .withInputSchemaField(value);
          break;
        }
      }
    }
  }

  /**
   * Builder for Neo4jSinkConfig
   */
  public static final class Builder {
    private String referenceName;
    private String jdbcPluginName;
    private String neo4jHost;
    private Integer neo4jPort;
    private String username;
    private String password;
    private String outputQuery;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setJdbcPluginName(String jdbcPluginName) {
      this.jdbcPluginName = jdbcPluginName;
      return this;
    }

    public Builder setNeo4jHost(String neo4jHost) {
      this.neo4jHost = neo4jHost;
      return this;
    }

    public Builder setNeo4jPort(Integer neo4jPort) {
      this.neo4jPort = neo4jPort;
      return this;
    }

    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setOutputQuery(String outputQuery) {
      this.outputQuery = outputQuery;
      return this;
    }

    public Neo4jSinkConfig build() {
      return new Neo4jSinkConfig(this);
    }
  }
}
