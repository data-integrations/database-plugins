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

package io.cdap.plugin.teradata.postaction;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.db.batch.action.AbstractQueryAction;
import io.cdap.plugin.teradata.TeradataConstants;

/**
 * Represents Teradata post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(TeradataConstants.PLUGIN_NAME)
@Description("Runs a Teradata query after a pipeline run.")
public class TeradataPostAction extends AbstractQueryAction {
  private final TeradataPostActionConfig config;

  public TeradataPostAction(TeradataPostActionConfig config) {
    super(config, false);
    this.config = config;
  }
}
