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

public class ClusterEventMonitor implements ClusterEventMonitorMBean {

  public enum PhaseName {
    Callback,
    InQueue,
    TotalProcessed
  }

  private static final long RESET_INTERVAL = 1000 * 60 * 10; // 1 hour
  private static final String CLUSTEREVENT_DN_KEY = "ClusterEventStatus";
  private static final String EVENT_DN_KEY = "eventName";
  private static final String PHASE_DN_KEY = "phaseName";

  private String _phaseName;
  private long _totalDuration;
  private long _maxDuration;
  private long _count;

  private long _lastResetTime;

  private ClusterStatusMonitor _clusterStatusMonitor;

  public ClusterEventMonitor(ClusterStatusMonitor clusterStatusMonitor, String phaseName) {
    _phaseName = phaseName;
    _clusterStatusMonitor = clusterStatusMonitor;
  }

  @Override
  public long getTotalDuration() {
    return _totalDuration;
  }

  @Override
  public long getMaxDuration() {
    return _maxDuration;
  }

  @Override
  public long getEventCount() {
    return _count;
  }

  public void reportDuration(long duration) {
    _totalDuration += duration;
    _count++;

    if (_lastResetTime + RESET_INTERVAL <= System.currentTimeMillis()) {
      _maxDuration = duration;
      _lastResetTime = System.currentTimeMillis();
    } else {
      _maxDuration = Math.max(_maxDuration, duration);
    }
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s.%s", CLUSTEREVENT_DN_KEY, _clusterStatusMonitor.getClusterName(),
        ClusterStatusMonitor.DEFAULT_TAG, _phaseName);
  }

  /**
   * get clusterEvent bean name
   *
   * @return clusterEvent bean name
   */
  public String getBeanName() {
    return String.format("%s,%s=%s,%s=%s", _clusterStatusMonitor.clusterBeanName(), EVENT_DN_KEY,
        "ClusterEvent", PHASE_DN_KEY, _phaseName);
  }
}
