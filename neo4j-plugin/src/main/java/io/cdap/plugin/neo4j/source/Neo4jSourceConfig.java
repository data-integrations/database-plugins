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

package io.cdap.plugin.neo4j.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.neo4j.Neo4jConstants;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Config for Neo4j Source plugin
 */
public class Neo4jSourceConfig extends PluginConfig {

  private static final List<String> UNAVAILABLE_QUERY_KEYWORDS =
    Arrays.asList("UNWIND", "CREATE", "DELETE", "SET", "REMOVE", "MERGE");
  private static final List<String> REQUIRED_QUERY_KEYWORDS = Arrays.asList("MATCH", "RETURN");

  public static final String NAME_INPUT_QUERY = "inputQuery";
  public static final String NAME_SPLIT_NUM = "splitNum";
  public static final String NAME_ORDER_BY = "orderBy";

  @Name(Neo4jConstants.NAME_REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  private String referenceName;

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

  @Name(NAME_INPUT_QUERY)
  @Description("The query to use to import data from the Neo4j database. " +
    "Query example: 'MATCH (n:Label) RETURN n.property_1, n.property_2'.")
  private String inputQuery;

  @Macro
  @Nullable
  @Name(NAME_SPLIT_NUM)
  @Description("The number of splits to generate. If set to one, the orderBy is not needed.")
  private Integer splitNum;

  @Macro
  @Nullable
  @Name(NAME_ORDER_BY)
  @Description("Field Name which will be used for ordering during splits generation. " +
    "This is required unless numSplits is set to one and 'ORDER BY' keyword not exist in Input Query.")
  private String orderBy;

  public Neo4jSourceConfig(String referenceName, String neo4jHost, Integer neo4jPort, String username, String password,
                           String inputQuery, int splitNum, @Nullable String orderBy) {
    this.referenceName = referenceName;
    this.neo4jHost = neo4jHost;
    this.neo4jPort = neo4jPort;
    this.username = username;
    this.password = password;
    this.inputQuery = inputQuery;
    this.splitNum = splitNum;
    this.orderBy = orderBy;
  }

  private Neo4jSourceConfig(Builder builder) {
    referenceName = builder.referenceName;
    neo4jHost = builder.neo4jHost;
    neo4jPort = builder.neo4jPort;
    username = builder.username;
    password = builder.password;
    inputQuery = builder.inputQuery;
    splitNum = builder.splitNum;
    orderBy = builder.orderBy;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Neo4jSourceConfig copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setNeo4jHost(copy.neo4jHost)
      .setNeo4jPort(copy.neo4jPort)
      .setUsername(copy.username)
      .setPassword(copy.password)
      .setInputQuery(copy.inputQuery)
      .setSplitNum(copy.splitNum)
      .setOrderBy(copy.orderBy);
  }

  public String getReferenceName() {
    return referenceName;
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

  public String getInputQuery() {
    return inputQuery;
  }

  public int getSplitNum() {
    return splitNum == null ? 1 : splitNum;
  }

  @Nullable
  public String getOrderBy() {
    return orderBy;
  }

  public String getConnectionString() {
    return String.format(Neo4jConstants.NEO4J_CONNECTION_STRING_FORMAT, getNeo4jHost(), getNeo4jPort(),
                         getUsername(), getPassword());
  }

  public void validate(FailureCollector collector) {
    if (UNAVAILABLE_QUERY_KEYWORDS.stream().parallel().anyMatch(inputQuery.toUpperCase()::contains)) {
      collector.addFailure(
        String.format("The input request must not contain any of the following keywords: '%s'",
                      UNAVAILABLE_QUERY_KEYWORDS.toString()),
        "Proved correct Input query.")
        .withConfigProperty(NAME_INPUT_QUERY);
    }
    if (!REQUIRED_QUERY_KEYWORDS.stream().parallel().allMatch(inputQuery.toUpperCase()::contains)) {
      collector.addFailure(
        String.format("The input request must contain following keywords: '%s'",
                      REQUIRED_QUERY_KEYWORDS.toString()),
        "Proved correct Input query.")
        .withConfigProperty(NAME_INPUT_QUERY);
    }
    if (!containsMacro(NAME_SPLIT_NUM) && getSplitNum() < 1) {
      collector.addFailure(
        String.format("Invalid value for Splits Number. Must be at least 1, but got: '%d'", getSplitNum()),
        null)
        .withConfigProperty(NAME_SPLIT_NUM);
    }
    if (!containsMacro(NAME_SPLIT_NUM) && getSplitNum() > 1) {
      boolean existOrderBy = inputQuery.toUpperCase().contains(" ORDER BY ");
      if (!containsMacro(NAME_ORDER_BY) && !existOrderBy && orderBy == null) {
        collector.addFailure(
          "Order by field required if Splits number greater than 1 and ORDER BY not exists in Input query.",
          null)
          .withConfigProperty(NAME_ORDER_BY);
      }
    }
  }

  /**
   * Builder for Neo4jSourceConfig
   */
  public static final class Builder {
    private String referenceName;
    private String neo4jHost;
    private Integer neo4jPort;
    private String username;
    private String password;
    private String inputQuery;
    @Nullable
    private Integer splitNum;
    @Nullable
    private String orderBy;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setNeo4jHost(String neo4jHost) {
      this.neo4jHost = neo4jHost;
      return this;
    }

    public Builder setNeo4jPort(int neo4jPort) {
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

    public Builder setInputQuery(String inputQuery) {
      this.inputQuery = inputQuery;
      return this;
    }

    public Builder setSplitNum(Integer splitNum) {
      this.splitNum = splitNum;
      return this;
    }

    public Builder setOrderBy(String orderBy) {
      this.orderBy = orderBy;
      return this;
    }

    public Neo4jSourceConfig build() {
      return new Neo4jSourceConfig(this);
    }
  }
}
