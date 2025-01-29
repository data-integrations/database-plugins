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

package io.cdap.plugin.postgres;

import com.google.common.base.Strings;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.plugin.common.db.DBErrorDetailsProvider;
import io.cdap.plugin.util.DBUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * A custom ErrorDetailsProvider for Postgres plugins.
 */
public class PostgresErrorDetailsProvider extends DBErrorDetailsProvider {
  // https://www.postgresql.org/docs/current/errcodes-appendix.html
  private static final Map<String, ErrorType> ERROR_CODE_TO_ERROR_TYPE;
  private static final Map<String, ErrorCategory> ERROR_CODE_TO_ERROR_CATEGORY;
  static {
    ERROR_CODE_TO_ERROR_TYPE = new HashMap<>();
    ERROR_CODE_TO_ERROR_TYPE.put("01", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("02", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("08", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("0A", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("22", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("23", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("28", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("40", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("42", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("53", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("54", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("55", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("57", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("58", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("P0", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("XX", ErrorType.SYSTEM);

    ErrorCategory.ErrorCategoryEnum plugin = ErrorCategory.ErrorCategoryEnum.PLUGIN;
    ERROR_CODE_TO_ERROR_CATEGORY = new HashMap<>();
    ERROR_CODE_TO_ERROR_CATEGORY.put("01", new ErrorCategory(plugin, "Warning"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("02", new ErrorCategory(plugin, "No Data"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("08", new ErrorCategory(plugin, "Postgres Server Connection Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0A", new ErrorCategory(plugin, "Postgres Server Feature Not Supported"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("22", new ErrorCategory(plugin, "Postgres Server Data Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("23", new ErrorCategory(plugin, "Postgres Integrity Constraint Violation"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("28", new ErrorCategory(plugin, "Postgres Invalid Authorization Specification"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("40", new ErrorCategory(plugin, "Transaction Rollback"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("42", new ErrorCategory(plugin, "Syntax Error or Access Rule Violation"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("53", new ErrorCategory(plugin, "Postgres Server Insufficient Resources"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("54", new ErrorCategory(plugin, "Postgres Program Limit Exceeded"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("55", new ErrorCategory(plugin, "Object Not in Prerequisite State"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("57", new ErrorCategory(plugin, "Operator Intervention"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("58", new ErrorCategory(plugin, "Postgres Server System Error"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("P0", new ErrorCategory(plugin, "PL/pgSQL Error"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("XX", new ErrorCategory(plugin, "Postgres Server Internal Error"));
  }

  @Override
  protected String getExternalDocumentationLink() {
    return DBUtils.POSTGRES_SUPPORTED_DOC_URL;
  }

  @Override
  protected ErrorType getErrorTypeFromErrorCodeAndSqlState(int errorCode, String sqlState) {
    if (!Strings.isNullOrEmpty(sqlState) && sqlState.length() >= 2 &&
      ERROR_CODE_TO_ERROR_TYPE.containsKey(sqlState.substring(0, 2))) {
      return ERROR_CODE_TO_ERROR_TYPE.get(sqlState.substring(0, 2));
    }
    return ErrorType.UNKNOWN;
  }

  @Override
  protected ErrorCategory getErrorCategoryFromSqlState(String sqlState) {
    if (!Strings.isNullOrEmpty(sqlState) && sqlState.length() >= 2 &&
      ERROR_CODE_TO_ERROR_CATEGORY.containsKey(sqlState.substring(0, 2))) {
      return ERROR_CODE_TO_ERROR_CATEGORY.get(sqlState.substring(0, 2));
    }
    return new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN);
  }
}
