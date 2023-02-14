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

package io.cdap.plugin.saphana;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.common.db.dbrecordwriter.ColumnType;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.util.List;


/**
 * SAP HANA Database record
 */
public class SapHanaDBRecord extends DBRecord {

  public SapHanaDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  @SuppressWarnings("unused")
  public SapHanaDBRecord() {
  }
}
