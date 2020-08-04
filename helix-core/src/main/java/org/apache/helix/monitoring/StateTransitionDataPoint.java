package org.apache.helix.monitoring;

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

public class StateTransitionDataPoint {
  long _totalDelay;
  long _executionDelay;
  long _messageLatency;
  boolean _isSuccess;

  public StateTransitionDataPoint(long totalDelay, long executionDelay, long messageLatency,
      boolean isSuccess) {
    _totalDelay = totalDelay;
    _executionDelay = executionDelay;
    _messageLatency = messageLatency;
    _isSuccess = isSuccess;
  }

  public long getTotalDelay() {
    return _totalDelay;
  }

  public long getExecutionDelay() {
    return _executionDelay;
  }

  public long getMessageLatency() {
    return _messageLatency;
  }

  public boolean getSuccess() {
    return _isSuccess;
  }
}
