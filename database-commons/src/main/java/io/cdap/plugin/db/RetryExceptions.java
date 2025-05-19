/*
 * Copyright © 2025 Cask Data, Inc.
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


package io.cdap.plugin.db;

import java.sql.SQLTransientException;
import java.util.HashSet;
import java.util.Set;


/**
 * Checks whether the given exception or one of its causes is a known retryable SQLException.
 */
public class RetryExceptions {
  public static boolean isRetryable(Throwable t) {
    Set<Throwable> seen = new HashSet<>();
    while (t != null && seen.add(t)) {
      if (t instanceof SQLTransientException) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }
}

