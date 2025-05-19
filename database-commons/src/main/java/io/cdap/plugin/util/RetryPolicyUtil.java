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


package io.cdap.plugin.util;

import dev.failsafe.RetryPolicy;
import io.cdap.cdap.api.Config;
import io.cdap.plugin.db.RetryExceptions;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLTransientConnectionException;
import java.time.Duration;

/**
 * Utility class for creating standardized {@link dev.failsafe.RetryPolicy} configurations
 * to handle transient SQL exceptions using the Failsafe library.
 */
public class RetryPolicyUtil extends Config {
    public static final Logger LOG = LoggerFactory.getLogger(RetryPolicyUtil.class);

    /**
     * Create a RetryPolicy using custom config values.
     */
    public static <T> RetryPolicy<T> createConnectionRetryPolicy(Integer initialRetryDuration,
      Integer maxRetryDuration, Integer maxRetryCount) {
        return RetryPolicy.<T>builder()
          .handleIf((failure) -> RetryExceptions.isRetryable(failure))
          .withBackoff(Duration.ofSeconds(initialRetryDuration), Duration.ofSeconds(maxRetryDuration))
          .withMaxRetries(maxRetryCount)
          .onRetry(e -> LOG.debug("Retrying... Attempt {}",
                                  e.getAttemptCount()))
          .onFailedAttempt(e -> LOG.debug("Failed Attempt : {}", e.getLastException()))
          .onFailure(e -> LOG.debug("Failed after retries." +
                                      " Reason: {}",
                                    e.getException() != null ? e.getException().getMessage() : "Unknown error"))
          .build();
    }
}
