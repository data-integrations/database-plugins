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
 * A {@link NoOpCommitConnection} for read-only operations whose {@link #commit()} is a no-op and whose {@link #close()}
 * rolls back any active transaction before closing, so Hadoop's {@code DBRecordReader#close()} (which calls
 * {@code connection.commit(); connection.close();} without a {@code finally} block) or an implicit driver
 * commit-on-close can never commit changes or skip closing the connection when rollback fails. Callers must disable
 * auto-commit ({@code setAutoCommit(false)}) on the delegate connection before executing statements.
 */
public class RollbackOnCloseConnection extends NoOpCommitConnection {

  private static final Logger LOG = LoggerFactory.getLogger(RollbackOnCloseConnection.class);

  public RollbackOnCloseConnection(Connection delegate) {
    super(delegate);
  }

  @Override
  public void close() throws SQLException {
    try {
      if (!getDelegate().isClosed() && !getDelegate().getAutoCommit()) {
        getDelegate().rollback();
      }
    } catch (SQLException e) {
      LOG.warn("Failed to roll back read-only connection ({}) before close.",
               getDelegate().getClass().getName(), e);
    } finally {
      super.close();
    }
  }
}
