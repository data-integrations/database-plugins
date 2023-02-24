/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.jdbc;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.plugin.db.action.AbstractDBArgumentSetter;
import io.cdap.plugin.db.action.ArgumentSetterConfig;

/**
 * Action that runs a db command
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("DatabaseArgumentSetter")
@Description("Reads single record from database table and converts it to arguments for pipeline")
public class DatabaseArgumentSetter extends AbstractDBArgumentSetter {

  private final DatabaseArgumentSetterConfig config;

  public DatabaseArgumentSetter(DatabaseArgumentSetterConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Database action databaseActionConfig.
   */
  public static class DatabaseArgumentSetterConfig extends ArgumentSetterConfig {

    @Override
    public String getConnectionString() {
      return connectionString;
    }
  }
}
