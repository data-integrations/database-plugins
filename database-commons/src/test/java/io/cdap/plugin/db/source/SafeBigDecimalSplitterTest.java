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

package io.cdap.plugin.db.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.db.BigDecimalSplitter;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link SafeBigDecimalSplitter}
 */
public class SafeBigDecimalSplitterTest {
  private final SafeBigDecimalSplitter splitter = new SafeBigDecimalSplitter();

  @Test
  public void testSmallRangeDivision() {
    BigDecimal result = splitter.tryDivide(BigDecimal.ONE, new BigDecimal("4"));
    assertEquals(new BigDecimal("0.25000"), result);
  }

  @Test
  public void testLargePrecision() {
    BigDecimal numerator = new BigDecimal("1.0000000000000000001");
    BigDecimal denominator = new BigDecimal("3");
    BigDecimal result = splitter.tryDivide(numerator, denominator);
    assertTrue(result.compareTo(BigDecimal.ZERO) > 0);
  }

  @Test
  public void testDivisionByZero() {
    assertThrows(ArithmeticException.class, () ->
      splitter.tryDivide(BigDecimal.ONE, BigDecimal.ZERO));
  }

  @Test
  public void testDivisionWithZeroNumerator() {
    // when minVal == maxVal
    BigDecimal result = splitter.tryDivide(BigDecimal.ZERO, BigDecimal.ONE);
    assertEquals(0, result.compareTo(BigDecimal.ZERO));
  }

  @Test
  public void testSplits() throws SQLException {
    BigDecimal minVal = BigDecimal.valueOf(1);
    BigDecimal maxVal = BigDecimal.valueOf(2);
    int numSplits = 4;
    ResultSet resultSet = mock(ResultSet.class);
    Configuration conf = mock(Configuration.class);
    when(conf.getInt("mapreduce.job.maps", 1)).thenReturn(numSplits);
    when(resultSet.getBigDecimal(1)).thenReturn(minVal);
    when(resultSet.getBigDecimal(2)).thenReturn(maxVal);
    BigDecimalSplitter bigDecimalSplitter = new SafeBigDecimalSplitter();
    List<InputSplit> actualSplits = bigDecimalSplitter.split(conf, resultSet, "id");
    assertEquals(numSplits, actualSplits.size());
  }

  @Test
  public void testSplitsWithMinValueEqualToMaxValue() throws SQLException {
    // when minVal == maxVal
    BigDecimal minVal = BigDecimal.valueOf(1);
    BigDecimal maxVal = BigDecimal.valueOf(1);
    int numSplits = 1;
    ResultSet resultSet = mock(ResultSet.class);
    Configuration conf = mock(Configuration.class);
    when(conf.getInt("mapreduce.job.maps", 1)).thenReturn(numSplits);
    when(resultSet.getBigDecimal(1)).thenReturn(minVal);
    when(resultSet.getBigDecimal(2)).thenReturn(maxVal);
    BigDecimalSplitter bigDecimalSplitter = new SafeBigDecimalSplitter();
    List<InputSplit> actualSplits = bigDecimalSplitter.split(conf, resultSet, "id");
    assertEquals(numSplits, actualSplits.size());
  }
}
