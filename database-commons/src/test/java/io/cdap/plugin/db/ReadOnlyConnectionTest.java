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
 * Unit tests for {@link ReadOnlyConnection}.
 */
public class ReadOnlyConnectionTest {

  @Test
  public void testWrapDisablesAutoCommitThenEnablesReadOnly() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);

    Connection connection = ReadOnlyConnection.wrap(delegate);

    Assert.assertTrue(connection instanceof ReadOnlyConnection);
    InOrder inOrder = Mockito.inOrder(delegate);
    inOrder.verify(delegate).setAutoCommit(false);
    inOrder.verify(delegate).setReadOnly(true);
    Mockito.verify(delegate, Mockito.never()).close();
  }

  @Test
  public void testWrapIsIdempotent() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Connection connection = ReadOnlyConnection.wrap(delegate);

    Assert.assertSame(connection, ReadOnlyConnection.wrap(connection));
    // the delegate was only configured once
    Mockito.verify(delegate, Mockito.times(1)).setAutoCommit(false);
    Mockito.verify(delegate, Mockito.times(1)).setReadOnly(true);
  }

  @Test
  public void testWrapClosesConnectionAndFailsWhenReadOnlyCannotBeSet() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    SQLException failure = new SQLException("read only not supported");
    Mockito.doThrow(failure).when(delegate).setReadOnly(true);

    try {
      ReadOnlyConnection.wrap(delegate);
      Assert.fail("Expected the SQLException to be propagated");
    } catch (SQLException e) {
      Assert.assertSame(failure, e);
    }
    // fail closed: the half configured connection must not leak
    Mockito.verify(delegate).close();
  }

  @Test
  public void testWrapClosesConnectionAndFailsWhenAutoCommitCannotBeDisabled() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Mockito.doThrow(new SQLException("boom")).when(delegate).setAutoCommit(false);
    Mockito.doThrow(new SQLException("close failed")).when(delegate).close();

    try {
      ReadOnlyConnection.wrap(delegate);
      Assert.fail("Expected the SQLException to be propagated");
    } catch (SQLException e) {
      Assert.assertEquals("boom", e.getMessage());
      Assert.assertEquals(1, e.getSuppressed().length);
      Assert.assertEquals("close failed", e.getSuppressed()[0].getMessage());
    }
  }

  @Test
  public void testCommitIsExecutedAsRollback() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Connection connection = ReadOnlyConnection.wrap(delegate);

    connection.commit();

    Mockito.verify(delegate).rollback();
    Mockito.verify(delegate, Mockito.never()).commit();
  }

  @Test
  public void testCloseRollsBackBeforeClosing() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Connection connection = ReadOnlyConnection.wrap(delegate);

    connection.close();

    InOrder inOrder = Mockito.inOrder(delegate);
    inOrder.verify(delegate).rollback();
    inOrder.verify(delegate).close();
    Mockito.verify(delegate, Mockito.never()).commit();
  }

  @Test
  public void testCloseStillClosesWhenRollbackFails() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Mockito.doThrow(new SQLException("rollback failed")).when(delegate).rollback();
    Connection connection = ReadOnlyConnection.wrap(delegate);

    connection.close();

    Mockito.verify(delegate).close();
  }

  @Test
  public void testCloseSkipsRollbackWhenAlreadyClosed() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Connection connection = ReadOnlyConnection.wrap(delegate);
    Mockito.when(delegate.isClosed()).thenReturn(true);

    connection.close();

    Mockito.verify(delegate, Mockito.never()).rollback();
    Mockito.verify(delegate).close();
  }

  @Test
  public void testReadOnlyAndAutoCommitCannotBeDowngraded() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    Connection connection = ReadOnlyConnection.wrap(delegate);

    try {
      connection.setAutoCommit(true);
      Assert.fail("Expected auto-commit to be rejected");
    } catch (SQLException e) {
      // expected
    }
    try {
      connection.setReadOnly(false);
      Assert.fail("Expected read-write to be rejected");
    } catch (SQLException e) {
      // expected
    }
    // re-asserting the hardened settings is allowed
    connection.setAutoCommit(false);
    connection.setReadOnly(true);

    Mockito.verify(delegate, Mockito.never()).setAutoCommit(true);
    Mockito.verify(delegate, Mockito.never()).setReadOnly(false);
  }
}
