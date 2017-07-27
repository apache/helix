package org.apache.helix.monitoring.mbeans;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.model.Message;

public class MessageLatencyMonitor implements MessageLatencyMBean {
  private static long DEFAULT_RESET_TIME = 60 * 60 * 1000;
  private long _totalMessageLatency;
  private long _totalMessageCount;
  private long _maxSingleMessageLatency;
  private long _lastResetTime;

  public MessageLatencyMonitor() {
    _totalMessageLatency = 0L;
    _totalMessageCount = 0L;
    _maxSingleMessageLatency = 0;
    _lastResetTime = System.currentTimeMillis();
  }

  public void updateLatency(Message message) {
    long latency = System.currentTimeMillis() - message.getCreateTimeStamp();
    _totalMessageCount++;
    _totalMessageLatency += latency;
    if (_lastResetTime + DEFAULT_RESET_TIME <= System.currentTimeMillis()) {
      _maxSingleMessageLatency = 0L;
      _lastResetTime = System.currentTimeMillis();
    }
    _maxSingleMessageLatency = Math.max(_maxSingleMessageLatency, latency);
  }


  @Override
  public long getTotalMessageLatency() {
    return _totalMessageLatency;
  }

  @Override
  public long getTotalMessageCount() {
    return _totalMessageCount;
  }

  @Override
  public long getMaxSingleMessageLatency() {
    return _maxSingleMessageLatency;
  }
}
