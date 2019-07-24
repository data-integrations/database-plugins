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
package io.cdap.plugin.batch;

import io.cdap.plugin.MongoDBConfig;

/**
 * Builder class that provides handy methods to construct {@link MongoDBConfig} for testing.
 */
public class MongoDBConfigBuilder {

  protected final MongoDBConfig config;

  public static MongoDBConfigBuilder builder() {
    return new MongoDBConfigBuilder(new MongoDBConfig());
  }

  public static MongoDBConfigBuilder builder(MongoDBConfig original) {
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

  public MongoDBConfigBuilder(MongoDBConfig config) {
    this.config = config;
  }

  public MongoDBConfigBuilder setReferenceName(String referenceName) {
    this.config.referenceName = referenceName;
    return this;
  }

  public MongoDBConfigBuilder setHost(String host) {
    this.config.host = host;
    return this;
  }

  public MongoDBConfigBuilder setPort(Integer port) {
    this.config.port = port;
    return this;
  }

  public MongoDBConfigBuilder setDatabase(String database) {
    this.config.database = database;
    return this;
  }

  public MongoDBConfigBuilder setCollection(String collection) {
    this.config.collection = collection;
    return this;
  }

  public MongoDBConfigBuilder setUser(String user) {
    this.config.user = user;
    return this;
  }

  public MongoDBConfigBuilder setPassword(String password) {
    this.config.password = password;
    return this;
  }

  public MongoDBConfigBuilder setConnectionArguments(String connectionArguments) {
    this.config.connectionArguments = connectionArguments;
    return this;
  }

  public MongoDBConfig build() {
    return this.config;
  }
}
