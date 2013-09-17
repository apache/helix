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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.helix.monitoring.StatCollector;
import org.apache.helix.monitoring.StateTransitionContext;
import org.apache.helix.monitoring.StateTransitionDataPoint;

public class StateTransitionStatMonitor implements StateTransitionStatMonitorMBean {
  public enum LATENCY_TYPE {
    TOTAL,
    EXECUTION
  };

  private static final int DEFAULT_WINDOW_SIZE = 4000;
  private long _numDataPoints;
  private long _successCount;
  private TimeUnit _unit;

  private ConcurrentHashMap<LATENCY_TYPE, StatCollector> _monitorMap =
      new ConcurrentHashMap<LATENCY_TYPE, StatCollector>();

  StateTransitionContext _context;

  public StateTransitionStatMonitor(StateTransitionContext context, TimeUnit unit) {
    _context = context;
    _monitorMap.put(LATENCY_TYPE.TOTAL, new StatCollector());
    _monitorMap.put(LATENCY_TYPE.EXECUTION, new StatCollector());
    reset();
  }

  public StateTransitionContext getContext() {
    return _context;
  }

  public String getBeanName() {
    return _context.getClusterName() + " " + _context.getResourceName() + " "
        + _context.getTransition();
  }

  public void addDataPoint(StateTransitionDataPoint data) {
    _numDataPoints++;
    if (data.getSuccess()) {
      _successCount++;
    }
    // should we count only the transition time for successful transitions?
    addLatency(LATENCY_TYPE.TOTAL, data.getTotalDelay());
    addLatency(LATENCY_TYPE.EXECUTION, data.getExecutionDelay());
  }

  void addLatency(LATENCY_TYPE type, double latency) {
    assert (_monitorMap.containsKey(type));
    _monitorMap.get(type).addData(latency);
  }

  public long getNumDataPoints() {
    return _numDataPoints;
  }

  public void reset() {
    _numDataPoints = 0;
    _successCount = 0;
    for (StatCollector monitor : _monitorMap.values()) {
      monitor.reset();
    }
  }

  @Override
  public long getTotalStateTransitionGauge() {
    return _numDataPoints;
  }

  @Override
  public long getTotalFailedTransitionGauge() {
    return _numDataPoints - _successCount;
  }

  @Override
  public long getTotalSuccessTransitionGauge() {
    return _successCount;
  }

  @Override
  public double getMeanTransitionLatency() {
    return _monitorMap.get(LATENCY_TYPE.TOTAL).getMean();
  }

  @Override
  public double getMaxTransitionLatency() {
    return _monitorMap.get(LATENCY_TYPE.TOTAL).getMax();
  }

  @Override
  public double getMinTransitionLatency() {
    return _monitorMap.get(LATENCY_TYPE.TOTAL).getMin();
  }

  @Override
  public double getPercentileTransitionLatency(int percentage) {
    return _monitorMap.get(LATENCY_TYPE.TOTAL).getPercentile(percentage);
  }

  @Override
  public double getMeanTransitionExecuteLatency() {
    return _monitorMap.get(LATENCY_TYPE.EXECUTION).getMean();
  }

  @Override
  public double getMaxTransitionExecuteLatency() {
    return _monitorMap.get(LATENCY_TYPE.EXECUTION).getMax();
  }

  @Override
  public double getMinTransitionExecuteLatency() {
    return _monitorMap.get(LATENCY_TYPE.EXECUTION).getMin();
  }

  @Override
  public double getPercentileTransitionExecuteLatency(int percentage) {
    return _monitorMap.get(LATENCY_TYPE.EXECUTION).getPercentile(percentage);
  }
}
