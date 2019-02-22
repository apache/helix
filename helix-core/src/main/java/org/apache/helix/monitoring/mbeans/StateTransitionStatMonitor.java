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

import org.apache.helix.monitoring.StatCollector;
import org.apache.helix.monitoring.StateTransitionContext;
import org.apache.helix.monitoring.StateTransitionDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO convert StateTransitionStatMonitor to extends DynamicMBeanProvider.
// Note this might change the attributes name.
public class StateTransitionStatMonitor implements StateTransitionStatMonitorMBean {
  private static final Logger _logger = LoggerFactory.getLogger(StateTransitionStatMonitor.class);
  public enum LATENCY_TYPE {
    TOTAL,
    EXECUTION,
    MESSAGE
  }

  private long _numDataPoints;
  private long _successCount;

  private ConcurrentHashMap<LATENCY_TYPE, StatCollector> _monitorMap = new ConcurrentHashMap<>();

  StateTransitionContext _context;

  public StateTransitionStatMonitor(StateTransitionContext context) {
    _context = context;
    for (LATENCY_TYPE type : LATENCY_TYPE.values()) {
      _monitorMap.put(type, new StatCollector());
    }
    reset();
  }

  public StateTransitionContext getContext() {
    return _context;
  }

  public String getSensorName() {
    return String.format("StateTransitionStat.%s.%s.%s", _context.getClusterName(),
        _context.getResourceName(), _context.getTransition());
  }

  public void addDataPoint(StateTransitionDataPoint data) {
    _numDataPoints++;
    if (data.getSuccess()) {
      _successCount++;
    }
    addLatency(LATENCY_TYPE.TOTAL, data.getTotalDelay());
    addLatency(LATENCY_TYPE.EXECUTION, data.getExecutionDelay());
    addLatency(LATENCY_TYPE.MESSAGE, data.getMessageLatency());
  }

  private void addLatency(LATENCY_TYPE type, double latency) {
    if (latency < 0) {
      _logger.warn("Ignore negative latency data {} for type {}.", latency, type.name());
      return;
    }
    assert(_monitorMap.containsKey(type));
    _monitorMap.get(type).addData(latency);
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

  @Override
  public double getMeanTransitionMessageLatency() {
    return _monitorMap.get(LATENCY_TYPE.MESSAGE).getMean();
  }

  @Override
  public double getMaxTransitionMessageLatency() {
    return _monitorMap.get(LATENCY_TYPE.MESSAGE).getMax();
  }

  @Override
  public double getMinTransitionMessageLatency() {
    return _monitorMap.get(LATENCY_TYPE.MESSAGE).getMin();
  }

  @Override
  public double getPercentileTransitionMessageLatency(int percentage) {
    return _monitorMap.get(LATENCY_TYPE.MESSAGE).getPercentile(percentage);
  }

}
