/*
 * Copyright © 2026 Cask Data, Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A JDBC {@link Connection} hardened for read-only use (connection browsing, spec generation and data sampling).
 *
 * <p>The wrapped connection is switched to {@code autoCommit=false} and {@code readOnly=true}. With auto-commit
 * disabled, drivers such as PostgreSQL JDBC start every transaction with {@code BEGIN READ ONLY}, so the database
 * itself rejects any DML/DDL that might be smuggled into a query. In addition:
 * <ul>
 *   <li>{@link #commit()} is turned into a {@link #rollback()} so nothing can ever be persisted through this
 *   connection, even by callers that commit unconditionally (e.g. Hadoop's {@code DBRecordReader#close()}).</li>
 *   <li>{@link #close()} rolls back any open transaction before closing. This matters for drivers that commit on
 *   close by default (e.g. Oracle).</li>
 * </ul>
 *
 * <p>Note: {@link Connection#setReadOnly(boolean)} is only a hint per the JDBC specification and is ignored by some
 * drivers (e.g. SQL Server). It is used here as defense in depth; the primary protection against SQL injection is
 * strict identifier quoting at query construction time.
 */
public class ReadOnlyConnection extends ForwardingConnection {

  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyConnection.class);

  private ReadOnlyConnection(Connection delegate) {
    super(delegate);
  }

  /**
   * Configures the given connection as a read-only, non auto-commit connection and wraps it.
   * If the connection cannot be configured, it is closed and the exception is propagated (fail closed).
   *
   * @param connection a freshly opened connection that is not yet in a transaction
   * @return the hardened connection
   * @throws SQLException if the connection cannot be switched to read-only / non auto-commit mode
   */
  public static Connection wrap(Connection connection) throws SQLException {
    if (connection instanceof ReadOnlyConnection) {
      return connection;
    }
    try {
      // Order matters: disable auto-commit first so that the read-only transaction characteristic set below is not
      // immediately ended by an auto-commit (Oracle) and is applied at the start of the next transaction (PostgreSQL).
      connection.setAutoCommit(false);
      connection.setReadOnly(true);
    } catch (SQLException e) {
      try {
        connection.close();
      } catch (SQLException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
    return new ReadOnlyConnection(connection);
  }

  /**
   * Auto-commit must stay disabled so that read-only transactions are enforced by the database.
   */
  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit) {
      throw new SQLException("Auto-commit cannot be enabled on a read-only connection.");
    }
    super.setAutoCommit(false);
  }

  /**
   * The connection must stay read-only.
   */
  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (!readOnly) {
      throw new SQLException("Read-only mode cannot be disabled on a read-only connection.");
    }
    super.setReadOnly(true);
  }

  /**
   * Nothing is ever persisted through a read-only connection: a commit is executed as a rollback.
   */
  @Override
  public void commit() throws SQLException {
    rollback();
  }

  @Override
  public void close() throws SQLException {
    try {
      if (!getDelegate().isClosed()) {
        getDelegate().rollback();
      }
    } catch (SQLException e) {
      // Best effort: the connection is being closed anyway and nothing may have been started on it.
      LOG.debug("Failed to rollback read-only connection before closing it.", e);
    } finally {
      super.close();
    }
  }
}
