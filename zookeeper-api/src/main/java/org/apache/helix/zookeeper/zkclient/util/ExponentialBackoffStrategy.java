package org.apache.helix.zookeeper.zkclient.util;

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

import java.util.Random;


public class ExponentialBackoffStrategy {
  private static long INIT_RETRY_INTERVAL = 500;

  public static long getWaitInterval(long currTime, long maxRetryInterval, boolean addJitter, int numberOfTriesFailed) {
    Random ran = new Random(currTime);
    double exponentialMultiplier = Math.pow(2.0, numberOfTriesFailed - 1);
    double result = exponentialMultiplier * INIT_RETRY_INTERVAL;

    if (maxRetryInterval > 0 && result > maxRetryInterval) {
      result = maxRetryInterval;
    }

    if (addJitter) {
      // Adding jitter so the real result would be 75% to 100% of the original result.
      // Don't directly add jitter here, since it may exceed the max retry interval setup
      result = result * (0.75 + ran.nextDouble() % 0.25);
    }
    return (long) result;
  }
}
