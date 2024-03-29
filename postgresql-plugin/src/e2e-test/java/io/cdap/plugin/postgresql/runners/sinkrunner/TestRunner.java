/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.postgresql.runners.sinkrunner;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Test Runner to execute PostgreSQL Sink plugin testcases.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
  features = {"src/e2e-test/features"},
  glue = {"io.cdap.plugin.postgresql.stepsdesign", "stepsdesign", "io.cdap.plugin.common.stepsdesign"},
  tags = {"@PostgreSQL_Sink and not @PLUGIN-1628 and not @Plugin-1526"},
  /* TODO :Enable tests once issue fixed https://cdap.atlassian.net/browse/PLUGIN-1628,
      https://cdap.atlassian.net/browse/PLUGIN-1526
   */
  monochrome = true,
  plugin = {"pretty", "html:target/cucumber-html-report/postgresql-sink",
    "json:target/cucumber-reports/cucumber-postgresql-sink.json",
    "junit:target/cucumber-reports/cucumber-postgresql-sink.xml"}
)
public class TestRunner {
}
