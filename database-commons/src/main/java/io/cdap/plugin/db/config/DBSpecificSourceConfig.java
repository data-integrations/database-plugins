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

package io.cdap.plugin.db.config;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.source.AbstractDBSource;


/**
 * Base config class for every database specific source plugin.
 */
public abstract class DBSpecificSourceConfig extends AbstractDBSource.DBSourceConfig {
  @Name(HOST)
  @Description("Database host")
  @Macro
  public String host;

  @Name(PORT)
  @Description("Specific database port")
  @Macro
  public Integer port;

  @Name(DATABASE)
  @Description("Database name to connect to")
  @Macro
  public String database;
}
