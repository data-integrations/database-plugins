/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.mysql;

import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.plugin.common.db.DBErrorDetailsProvider;
import io.cdap.plugin.util.DBUtils;

/**
 * A custom ErrorDetailsProvider for MySQL plugins.
 */
public class MysqlErrorDetailsProvider extends DBErrorDetailsProvider {

  @Override
  protected String getExternalDocumentationLink() {
    return DBUtils.MYSQL_SUPPORTED_DOC_URL;
  }

  @Override
  protected ErrorType getErrorTypeFromErrorCodeAndSqlState(int errorCode, String sqlState) {
    // https://dev.mysql.com/doc/refman/9.0/en/error-message-elements.html#error-code-ranges
    if (errorCode >= 1000 && errorCode <= 5999) {
      return ErrorType.USER;
    } else if (errorCode >= 10000 && errorCode <= 51999) {
      // SYSTEM errors: Enterprise and user-defined custom error messages
      return ErrorType.SYSTEM;
    } else {
      // UNKNOWN errors: Anything outside defined range
      return ErrorType.UNKNOWN;
    }
  }
}
