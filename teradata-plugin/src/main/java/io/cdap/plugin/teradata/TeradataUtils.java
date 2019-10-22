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

package io.cdap.plugin.teradata;

import com.google.common.base.Strings;
import io.cdap.plugin.common.KeyValueListParser;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Teradata util methods.
 */
public final class TeradataUtils {

  /**
   * Creates Teradata specific JDBC connection string.
   *
   * @param host                server host.
   * @param port                server port.
   * @param database            database name.
   * @param connectionArguments connection arguments.
   * @return Teradata specific JDBC connection string
   */
  public static String getConnectionString(String host, Integer port, String database, String connectionArguments) {
    String arguments = getConnectionArguments(connectionArguments);
    return String.format(
      TeradataConstants.TERADATA_CONNECTION_STRING_FORMAT,
      host,
      database,
      port,
      arguments.length() == 1 ? "" : arguments
    );
  }

  /**
   * Format Teradata connection parameters.
   *
   * @param connectionArguments server host.
   * @return Teradata connection parameters.
   */
  public static String getConnectionArguments(@Nullable String connectionArguments) {
    KeyValueListParser kvParser = new KeyValueListParser("\\s*;\\s*", "=");
    String result = "";

    if (!Strings.isNullOrEmpty(connectionArguments)) {
      result = StreamSupport.stream(kvParser.parse(connectionArguments).spliterator(), false)
        .map(keyVal -> String.format("%s=%s", keyVal.getKey().toUpperCase(), keyVal.getValue()))
        .collect(Collectors.joining(",", ",", ""));
    }

    return result;
  }
}
