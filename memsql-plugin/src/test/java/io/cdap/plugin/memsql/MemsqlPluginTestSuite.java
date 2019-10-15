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

package io.cdap.plugin.memsql;

import io.cdap.cdap.common.test.TestSuite;
import io.cdap.plugin.memsql.action.MemsqlActionTestRun;
import io.cdap.plugin.memsql.postaction.MemsqlPostActionTestRun;
import io.cdap.plugin.memsql.sink.MemsqlSinkTestRun;
import io.cdap.plugin.memsql.source.MemsqlSourceTestRun;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


/**
 * This is a test suite that runs all the tests for Database plugins.
 */
@RunWith(TestSuite.class)
@Suite.SuiteClasses({
  MemsqlSinkTestRun.class,
  MemsqlSourceTestRun.class,
  MemsqlActionTestRun.class,
  MemsqlPostActionTestRun.class
})
public class MemsqlPluginTestSuite extends MemsqlPluginTestBase {

  @BeforeClass
  public static void setup() {
    tearDown = false;
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tearDown = true;
  }
}
