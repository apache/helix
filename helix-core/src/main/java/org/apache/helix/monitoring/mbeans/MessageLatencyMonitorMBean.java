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

import org.apache.helix.monitoring.SensorNameProvider;

public interface MessageLatencyMonitorMBean extends SensorNameProvider{

  /**
   * Mesure the total message latency from ZK
   * @return
   */
  public long getTotalMessageLatency();

  /**
   * The total count of received NEW message
   * @return
   */
  public long getTotalMessageCount();

  /**
   * The maximal message latency for single message. It will be cleared in one hour
   * @return
   */
  public long getMaxSingleMessageLatency();
}
