
/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.mysql;

import org.junit.Test;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MysqlUtilUnitTest {

  @Test
  public void testIsZeroDateTimeToNull() {
    Map<String, String> connArgsMap = new HashMap<>(1);

    connArgsMap.put("zeroDateTimeBehavior", "");
    assertFalse(MysqlUtil.isZeroDateTimeToNull(connArgsMap));

    connArgsMap.put("zeroDateTimeBehavior", "ROUND");
    assertFalse(MysqlUtil.isZeroDateTimeToNull(connArgsMap));

    connArgsMap.put("zeroDateTimeBehavior", "CONVERT_TO_NULL");
    assertTrue(MysqlUtil.isZeroDateTimeToNull(connArgsMap));

    connArgsMap.put("zeroDateTimeBehavior", "convertToNull");
    assertTrue(MysqlUtil.isZeroDateTimeToNull(connArgsMap));
  }

  @Test
  public void testIsDateTimeLikeType() {
    int dateType = Types.DATE;
    int timestampType = Types.TIMESTAMP;
    int timestampWithTimezoneType = Types.TIMESTAMP_WITH_TIMEZONE;
    int timeType = Types.TIME;
    int stringType = Types.VARCHAR;

    assertTrue(MysqlUtil.isDateTimeLikeType(dateType));
    assertTrue(MysqlUtil.isDateTimeLikeType(timestampType));
    assertTrue(MysqlUtil.isDateTimeLikeType(timestampWithTimezoneType));
    assertFalse(MysqlUtil.isDateTimeLikeType(timeType));
    assertFalse(MysqlUtil.isDateTimeLikeType(stringType));
  }
}
