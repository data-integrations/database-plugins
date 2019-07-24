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
package io.cdap.plugin.batch.source;

import io.cdap.plugin.MongoDBConfig;

/**
 * Builder class that provides handy methods to construct {@link MongoDBBatchSource.MongoDBSourceConfig} for testing.
 */
public class MongoDBSourceConfigBuilder {

  protected final MongoDBBatchSource.MongoDBSourceConfig config;

  public static MongoDBSourceConfigBuilder builder() {
    return new MongoDBSourceConfigBuilder(new MongoDBBatchSource.MongoDBSourceConfig());
  }

  public static MongoDBSourceConfigBuilder builder(MongoDBConfig original) {
    return builder()
      .setReferenceName(original.referenceName)
      .setHost(original.host)
      .setPort(original.port)
      .setDatabase(original.database)
      .setCollection(original.collection)
      .setUser(original.user)
      .setPassword(original.password)
      .setConnectionArguments(original.connectionArguments);
  }

  public MongoDBSourceConfigBuilder(MongoDBBatchSource.MongoDBSourceConfig config) {
    this.config = config;
  }

  public MongoDBSourceConfigBuilder setReferenceName(String referenceName) {
    this.config.referenceName = referenceName;
    return this;
  }

  public MongoDBSourceConfigBuilder setHost(String host) {
    this.config.host = host;
    return this;
  }

  public MongoDBSourceConfigBuilder setPort(Integer port) {
    this.config.port = port;
    return this;
  }

  public MongoDBSourceConfigBuilder setDatabase(String database) {
    this.config.database = database;
    return this;
  }

  public MongoDBSourceConfigBuilder setCollection(String collection) {
    this.config.collection = collection;
    return this;
  }

  public MongoDBSourceConfigBuilder setUser(String user) {
    this.config.user = user;
    return this;
  }

  public MongoDBSourceConfigBuilder setPassword(String password) {
    this.config.password = password;
    return this;
  }

  public MongoDBSourceConfigBuilder setConnectionArguments(String connectionArguments) {
    this.config.connectionArguments = connectionArguments;
    return this;
  }

  public MongoDBSourceConfigBuilder setSchema(String schema) {
    this.config.schema = schema;
    return this;
  }

  public MongoDBSourceConfigBuilder setInputQuery(String inputQuery) {
    this.config.inputQuery = inputQuery;
    return this;
  }

  public MongoDBSourceConfigBuilder setInputFields(String inputFields) {
    this.config.inputFields = inputFields;
    return this;
  }

  public MongoDBSourceConfigBuilder setSplitterClass(String splitterClass) {
    this.config.splitterClass = splitterClass;
    return this;
  }

  public MongoDBSourceConfigBuilder setAuthConnectionString(String authConnectionString) {
    this.config.authConnectionString = authConnectionString;
    return this;
  }

  public MongoDBBatchSource.MongoDBSourceConfig build() {
    return this.config;
  }
}
