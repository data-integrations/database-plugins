/*
 * Copyright © 2021 Cask Data, Inc.
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
import org.jetbrains.annotations.NotNull;

import java.net.URI;

/**
 * URL Parser for DB
 */

public class DatabaseURLParser {
  // TODO: Implement Parsing Logic
  public static URI parseURL(@NotNull String connectionString) {
    String cleanURI = connectionString.substring(5);
    URI uri = URI.create(cleanURI);
    return uri;
  }

  public static String constructFQN(URI uri, String tableName) {
    if (uri.getScheme() == "postgres")
      return "";
    else
      return String.format("%s://%s:%s/%s/%s", uri.getScheme(), uri.getHost(), uri.getPort(), uri.getPath(), tableName);
  }
}
