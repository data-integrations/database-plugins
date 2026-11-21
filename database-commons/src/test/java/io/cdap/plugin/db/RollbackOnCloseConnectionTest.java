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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Unit tests for {@link RollbackOnCloseConnection}.
 */
public class RollbackOnCloseConnectionTest {

  @Test
  public void commit_delegatesToRollbackInsteadOfCommit() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    RollbackOnCloseConnection wrapper = new RollbackOnCloseConnection(delegate);

    wrapper.commit();

    Mockito.verify(delegate).rollback();
    Mockito.verify(delegate, Mockito.never()).commit();
  }

  @Test
  public void close_activeTransaction_rollsBackBeforeClose() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Mockito.when(delegate.isClosed()).thenReturn(false);
    Mockito.when(delegate.getAutoCommit()).thenReturn(false);
    RollbackOnCloseConnection wrapper = new RollbackOnCloseConnection(delegate);

    wrapper.close();

    InOrder inOrder = Mockito.inOrder(delegate);
    inOrder.verify(delegate).rollback();
    inOrder.verify(delegate).close();
    Mockito.verify(delegate, Mockito.never()).commit();
  }

  @Test
  public void close_rollbackThrows_stillClosesDelegate() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Mockito.when(delegate.isClosed()).thenReturn(false);
    Mockito.when(delegate.getAutoCommit()).thenReturn(false);
    Mockito.doThrow(new SQLException("rollback failed")).when(delegate).rollback();
    RollbackOnCloseConnection wrapper = new RollbackOnCloseConnection(delegate);

    wrapper.close();

    Mockito.verify(delegate).close();
    Assert.assertNotNull(wrapper.getDelegate());
  }
}
