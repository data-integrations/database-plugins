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

package io.cdap.plugin.neo4j.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBRecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A RecordReader that reads records from a Neo4j.
 *
 * @param <T>
 */
public class Neo4jDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jDBRecordReader.class);

  public Neo4jDBRecordReader(DBInputFormat.DBInputSplit split, Class<T> inputClass, Configuration conf,
                             Connection conn, DBConfiguration dbConfig, String cond, String[] fields,
                             String table) throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
  }

  @Override
  protected String getSelectQuery() {
    StringBuilder query = new StringBuilder();

    //PREBUILT QUERY
    String inputQuery = getDBConf().getInputQuery();
    boolean existOrderBy = inputQuery.toUpperCase().contains(" ORDER BY ");
    query.append(getDBConf().getInputQuery());
    String orderBy = getDBConf().getInputOrderBy();
    if (!existOrderBy && orderBy != null && orderBy.length() > 0) {
      query.append(" ORDER BY ").append(orderBy);
    }

    try {
      query.append(" SKIP ").append(getSplit().getStart());
      query.append(" LIMIT ").append(getSplit().getLength());
    } catch (IOException ex) {
      // should never happen
      throw new IllegalStateException(ex);
    }
    return query.toString();
  }
}
