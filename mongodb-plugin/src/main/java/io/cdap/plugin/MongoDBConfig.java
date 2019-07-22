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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.Constants;

import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that MongoDB Source and Sink can re-use.
 */
public class MongoDBConfig extends PluginConfig {

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  public String referenceName;

  @Name(MongoDBConstants.HOST)
  @Description("Host that MongoDB is running on.")
  public String host;

  @Name(MongoDBConstants.PORT)
  @Description("Port that MongoDB is listening to.")
  public Integer port;

  @Name(MongoDBConstants.DATABASE)
  @Description("MongoDB database name.")
  public String database;

  @Name(MongoDBConstants.COLLECTION)
  @Description("Name of the database collection.")
  public String collection;

  @Name(MongoDBConstants.USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  public String user;

  @Name(MongoDBConstants.PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  public String password;

  @Name(MongoDBConstants.CONNECTION_ARGUMENTS)
  @Description("A list of arbitrary string key/value pairs as connection arguments.")
  @Nullable
  public String connectionArguments;

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
