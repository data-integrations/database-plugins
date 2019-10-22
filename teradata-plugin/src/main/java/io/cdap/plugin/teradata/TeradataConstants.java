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

package io.cdap.plugin.teradata;

/**
 * Teradata Constants.
 */
public final class TeradataConstants {
  public static final String PLUGIN_NAME = "Teradata";
  public static final String TERADATA_CONNECTION_STRING_FORMAT = "jdbc:teradata://%s/DATABASE=%s,DBS_PORT=%s%s";
}
