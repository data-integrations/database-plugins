/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class DataDrivenETLDBInputFormatTest {

  @Mock
  private JobContext mockJobContext;
  @Mock
  private DBConfiguration mockDbConfiguration;

  private DataDrivenETLDBInputFormat inputFormat;

  @Before
  public void setUp() {
    inputFormat = Mockito.spy(new DataDrivenETLDBInputFormat());
    Mockito.doReturn(mockDbConfiguration).when(inputFormat).getDBConf();
    Mockito.doReturn("id").when(mockDbConfiguration).getInputOrderBy();
  }

  @Test
  public void testGetSplitsAddsNullSplit() throws IOException {
    DataDrivenDBInputFormat.DataDrivenDBInputSplit existingSplit =
        new DataDrivenDBInputFormat.DataDrivenDBInputSplit("id >= 0", "id < 100");
    List<InputSplit> initialSplits = ImmutableList.of(existingSplit);
    Mockito.doReturn(initialSplits).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);

    Assert.assertEquals("A new split for NULLs should be added", 2, finalSplits.size());

    DataDrivenDBInputFormat.DataDrivenDBInputSplit nullSplit =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) finalSplits.get(1);
    Assert.assertEquals("id IS NULL", nullSplit.getLowerClause());
    Assert.assertEquals("id IS NULL", nullSplit.getUpperClause());
  }

  @Test
  public void testGetSplitsDoesNotAddNullSplitIfPresent() throws IOException {
    DataDrivenDBInputFormat.DataDrivenDBInputSplit existingSplit =
        new DataDrivenDBInputFormat.DataDrivenDBInputSplit("id >= 0", "id < 100");
    DataDrivenDBInputFormat.DataDrivenDBInputSplit nullSplit =
        new DataDrivenDBInputFormat.DataDrivenDBInputSplit("id IS NULL", "id IS NULL");
    List<InputSplit> initialSplits = ImmutableList.of(existingSplit, nullSplit);

    Mockito.doReturn(initialSplits).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);

    Assert.assertEquals("Should not add a duplicate NULL split", 2, finalSplits.size());
  }

  @Test
  public void testGetSplitsDoesNotAddNullSplitIfSelectAllPresent() throws IOException {
    DataDrivenDBInputFormat.DataDrivenDBInputSplit existingSplit =
        new DataDrivenDBInputFormat.DataDrivenDBInputSplit("1=1", "1=1");
    List<InputSplit> initialSplits = ImmutableList.of(existingSplit);

    Mockito.doReturn(initialSplits).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);

    Assert.assertEquals("Should not add a NULL split", 1, finalSplits.size());
  }

  @Test
  public void testGetSplitsDoesNotAddNullSplitIfBaseReturnsNull() throws IOException {
    Mockito.doReturn(null).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);
    Assert.assertEquals("Should not add a NULL split", 1, finalSplits.size());

    DataDrivenDBInputFormat.DataDrivenDBInputSplit split =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) finalSplits.get(0);
    Assert.assertEquals("1=1", split.getLowerClause());
  }

  @Test
  public void testGetSplitsDoesNotAddNullSplitIfBaseReturnsEmptyList() throws IOException {
    Mockito.doReturn(Collections.emptyList()).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);
    Assert.assertEquals("Should not add a NULL split", 1, finalSplits.size());

    DataDrivenDBInputFormat.DataDrivenDBInputSplit split =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) finalSplits.get(0);
    Assert.assertEquals("1=1", split.getLowerClause());
  }
}
