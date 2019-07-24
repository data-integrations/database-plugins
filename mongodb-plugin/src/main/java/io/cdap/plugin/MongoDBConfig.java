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

package io.cdap.plugin;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;

import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that MongoDB Source and Sink can re-use.
 */
public class MongoDBConfig extends PluginConfig {

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  @Macro
  public String referenceName;

  @Name(MongoDBConstants.HOST)
  @Description("Host that MongoDB is running on.")
  @Macro
  public String host;

  @Name(MongoDBConstants.PORT)
  @Description("Port that MongoDB is listening to.")
  @Macro
  public Integer port;

  @Name(MongoDBConstants.DATABASE)
  @Description("MongoDB database name.")
  @Macro
  public String database;

  @Name(MongoDBConstants.COLLECTION)
  @Description("Name of the database collection.")
  @Macro
  public String collection;

  @Name(MongoDBConstants.USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Macro
  @Nullable
  public String user;

  @Name(MongoDBConstants.PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Macro
  @Nullable
  public String password;

  @Name(MongoDBConstants.CONNECTION_ARGUMENTS)
  @Description("A list of arbitrary string key/value pairs as connection arguments.")
  @Macro
  @Nullable
  public String connectionArguments;

  /**
   * Validates {@link MongoDBConfig} instance.
   */
  public void validate() {
    if (!containsMacro(Constants.Reference.REFERENCE_NAME) && Strings.isNullOrEmpty(referenceName)) {
      throw new InvalidConfigPropertyException("Reference name must be specified", Constants.Reference.REFERENCE_NAME);
    } else {
      try {
        IdUtils.validateId(referenceName);
      } catch (IllegalArgumentException e) {
        // InvalidConfigPropertyException should be thrown instead of IllegalArgumentException
        throw new InvalidConfigPropertyException("Invalid reference name", e, Constants.Reference.REFERENCE_NAME);
      }
    }
    if (!containsMacro(MongoDBConstants.HOST) && Strings.isNullOrEmpty(host)) {
      throw new InvalidConfigPropertyException("Host must be specified", MongoDBConstants.HOST);
    }
    if (!containsMacro(MongoDBConstants.PORT)) {
      if (port == null) {
        throw new InvalidConfigPropertyException("Port number must be specified", MongoDBConstants.PORT);
      }
      if (port < 1) {
        throw new InvalidConfigPropertyException("Port number must be greater than 0", MongoDBConstants.PORT);
      }
    }
    if (!containsMacro(MongoDBConstants.DATABASE) && Strings.isNullOrEmpty(database)) {
      throw new InvalidConfigPropertyException("Database name must be specified", MongoDBConstants.DATABASE);
    }
    if (!containsMacro(MongoDBConstants.COLLECTION) && Strings.isNullOrEmpty(collection)) {
      throw new InvalidConfigPropertyException("Collection name must be specified", MongoDBConstants.COLLECTION);
    }
  }

  /**
   * Constructs a connection string from host, port, username, password and database properties.
   * @return connection string.
   */
  public String getConnectionString() {
    StringBuilder connectionStringBuilder = new StringBuilder("mongodb://");
    if (!Strings.isNullOrEmpty(user) || !Strings.isNullOrEmpty(password)) {
      connectionStringBuilder.append(user).append(":").append(password).append("@");
    }
    connectionStringBuilder.append(host).append(":").append(port).append("/")
      .append(database).append(".").append(collection);

    if (!Strings.isNullOrEmpty(connectionArguments)) {
      connectionStringBuilder.append("?").append(connectionArguments);
    }

    return connectionStringBuilder.toString();
  }
}
