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
 * A {@link ForwardingConnection} for read-only operations that turns {@link #commit()} into {@link #rollback()} and
 * rolls back any active transaction before closing, so Hadoop's {@code DBRecordReader#close()} or an implicit
 * driver commit-on-close can never commit changes.
 */
public class RollbackOnCloseConnection extends ForwardingConnection {

  private static final Logger LOG = LoggerFactory.getLogger(RollbackOnCloseConnection.class);

  public RollbackOnCloseConnection(Connection delegate) {
    super(delegate);
  }

  @Override
  public void commit() throws SQLException {
    getDelegate().rollback();
  }

  @Override
  public void close() throws SQLException {
    try {
      if (!getDelegate().isClosed() && !getDelegate().getAutoCommit()) {
        getDelegate().rollback();
      }
    } catch (SQLException e) {
      LOG.debug("Failed to roll back read-only connection before close.", e);
    } finally {
      super.close();
    }
  }
}
