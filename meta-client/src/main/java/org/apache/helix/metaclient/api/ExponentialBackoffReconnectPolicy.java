package org.apache.helix.metaclient.api;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.apache.helix.metaclient.constants.MetaClientConstants.DEFAULT_INIT_EXP_BACKOFF_RETRY_INTERVAL_MS;
import static org.apache.helix.metaclient.constants.MetaClientConstants.DEFAULT_MAX_EXP_BACKOFF_RETRY_INTERVAL_MS;

/**
 * Policy to define client re-establish connection behavior when connection to underlying metadata
 * store is expired.
 * Wait time before each backoff period will increase exponentially until a user defined max
 * backoff interval.
 */
public class ExponentialBackoffReconnectPolicy implements MetaClientReconnectPolicy {

  private long _maxBackOffInterval;
  private long _initBackoffInterval;

  @Override
  public RetryPolicyName getPolicyName() {
    return RetryPolicyName.EXP_BACKOFF;
  }

  public ExponentialBackoffReconnectPolicy() {
    _initBackoffInterval = DEFAULT_INIT_EXP_BACKOFF_RETRY_INTERVAL_MS;
    _maxBackOffInterval = DEFAULT_MAX_EXP_BACKOFF_RETRY_INTERVAL_MS;
  }

  public ExponentialBackoffReconnectPolicy(long maxBackOffInterval, long initBackoffInterval) {
    _maxBackOffInterval = maxBackOffInterval;
    _initBackoffInterval = initBackoffInterval;

  }

  long getMaxBackOffInterval() {
    return _maxBackOffInterval;
  }

  long getInitBackOffInterval() {
    return _initBackoffInterval;
  }
}
