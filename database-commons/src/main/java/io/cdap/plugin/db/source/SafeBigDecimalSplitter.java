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

import org.apache.hadoop.mapreduce.lib.db.BigDecimalSplitter;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Safe implementation of {@link BigDecimalSplitter} to ensure precise division of BigDecimal values while calculating
 * split points for NUMERIC and DECIMAL types.
 *
 * <p>Problem: The default {@link BigDecimalSplitter} implementation may return 0 when the numerator is smaller than the
 * denominator (e.g., 1 / 4 = 0), due to the lack of a defined scale for division. Since the result (0) is smaller than
 * {@link BigDecimalSplitter#MIN_INCREMENT} (i.e. {@code 10000 * Double.MIN_VALUE}), the split size defaults to
 * {@code MIN_INCREMENT}, leading to an excessive number of splits (~10M) and potential OOM errors.</p>
 *
 * <p>Fix: This implementation derives scale from column metadata, adds a buffer of 5 decimal places, and uses
 * {@link RoundingMode#HALF_UP} as the rounding mode.</p
 *
 * <p>Note: This class is used by {@link DataDrivenETLDBInputFormat}.</p>
 */
public class SafeBigDecimalSplitter extends BigDecimalSplitter {

  /* An additional buffer of +5 digits is applied to preserve accuracy during division. */
  public static final int SCALE_BUFFER = 5;
  /**
   * Performs safe division with correct scale handling.
   *
   * @param numerator   the dividend (BigDecimal)
   * @param denominator the divisor (BigDecimal)
   * @return quotient with derived scale
   * @throws ArithmeticException if denominator is zero
   */
  @Override
  protected BigDecimal tryDivide(BigDecimal numerator, BigDecimal denominator) {
    // Determine the required scale for the division and add a buffer to ensure accuracy
    int effectiveScale = Math.max(numerator.scale(), denominator.scale()) + SCALE_BUFFER;
    return numerator.divide(denominator, effectiveScale, RoundingMode.HALF_UP);
  }
}
