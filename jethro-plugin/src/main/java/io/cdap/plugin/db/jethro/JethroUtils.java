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
package io.cdap.plugin.db.jethro;

/**
 * Jethro utils
 */
public class JethroUtils {

  private static final String JETHRO_CONNECTION_STRING_FORMAT = "jdbc:JethroData://%s:%d/%s";

  public static String getConnectionString(String host, int port, String database) {
    return String.format(JETHRO_CONNECTION_STRING_FORMAT, host, port, database);
  }
}
