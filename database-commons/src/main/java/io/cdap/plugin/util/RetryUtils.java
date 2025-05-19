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

package io.cdap.plugin.util;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.cdap.plugin.common.db.DBErrorDetailsProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Utility class for retrieving common methods using {@link dev.failsafe.RetryPolicy}
 */
public final class RetryUtils {

    public static final String NAME_INITIAL_RETRY_DURATION = "initialRetryDuration";
    public static final String NAME_MAX_RETRY_DURATION = "maxRetryDuration";
    public static final String NAME_MAX_RETRY_COUNT = "maxRetryCount";
    public static final int DEFAULT_INITIAL_RETRY_DURATION_SECONDS = 5;
    public static final int DEFAULT_MAX_RETRY_COUNT = 5;
    public static final int DEFAULT_MAX_RETRY_DURATION_SECONDS = 80;

    public static Connection createConnectionWithRetry(RetryPolicy<Connection> retryPolicy, String connectionString,
      Properties connectionProperties, DBErrorDetailsProvider dbErrorDetailsProvider) {
        try {
            return Failsafe.with(retryPolicy).get(() -> DriverManager
              .getConnection(connectionString, connectionProperties)
            );
        } catch (Exception e) {
            throw unwrapFailsafeException(e, dbErrorDetailsProvider);
        }
    }

    public static Statement createStatementWithRetry(RetryPolicy<Statement> retryPolicy, Connection connection,
      DBErrorDetailsProvider dbErrorDetailsProvider) {
        try {
            return Failsafe.with(retryPolicy).get(connection::createStatement);
        } catch (Exception e) {
            throw unwrapFailsafeException(e, dbErrorDetailsProvider);
        }
    }

    public static PreparedStatement prepareStatementWithRetry(RetryPolicy<PreparedStatement> retryPolicy,
      Connection connection, String sqlQuery, DBErrorDetailsProvider dbErrorDetailsProvider) {
        try {
            return Failsafe.with(retryPolicy).get(() -> connection.prepareStatement(sqlQuery));
        } catch (Exception e) {
            throw unwrapFailsafeException(e, dbErrorDetailsProvider);
        }
    }

    public static ResultSet executeQueryWithRetry(RetryPolicy<ResultSet> retryPolicy,
      PreparedStatement preparedStatement, DBErrorDetailsProvider dbErrorDetailsProvider) {
        try {
            return Failsafe.with(retryPolicy).get(() -> preparedStatement.executeQuery());
        } catch (Exception e) {
            throw unwrapFailsafeException(e, dbErrorDetailsProvider);
        }
    }

    public static ResultSet executeQueryWithRetry(RetryPolicy<ResultSet> retryPolicy, Statement statement,
      String query, DBErrorDetailsProvider dbErrorDetailsProvider) {
        try {
            return Failsafe.with(retryPolicy).get(() -> statement.executeQuery(query));
        } catch (Exception e) {
            throw unwrapFailsafeException(e, dbErrorDetailsProvider);
        }
    }

    public static void executeInitQueryWithRetry(RetryPolicy<?> retryPolicy, Statement statement, String query,
      DBErrorDetailsProvider dbErrorDetailsProvider) {
        try {
            Failsafe.with(retryPolicy).run(() -> statement.execute(query));
        } catch (Exception e) {
            throw unwrapFailsafeException(e, dbErrorDetailsProvider);
        }
    }

    private static RuntimeException unwrapFailsafeException(Exception e,
      DBErrorDetailsProvider dbErrorDetailsProvider) {
        if (e instanceof FailsafeException) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException) {
                return dbErrorDetailsProvider.getProgramFailureException((SQLException) cause, null);
            } else if (cause instanceof RuntimeException) {
                return (RuntimeException) cause;
            } else if (cause instanceof Error) {
                return new RuntimeException("Operation failed with error", cause);
            } else {
                return new RuntimeException("Operation failed", cause);
            }
        }
        if (e instanceof SQLException) {
            return dbErrorDetailsProvider.getProgramFailureException((SQLException) e, null);
        }
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new RuntimeException("Unexpected checked exception", e);
    }
}
