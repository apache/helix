package org.apache.helix.monitoring.metrics.implementation;

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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceLatencyGauge extends LatencyMetric {
  private static final Logger LOG = LoggerFactory.getLogger(RebalanceLatencyGauge.class);
  private static final long VALUE_NOT_SET = -1;
  private long _lastEmittedMetricValue = VALUE_NOT_SET;
  // Use threadlocal here so the start time can be updated and recorded in multi-threads.
  private final ThreadLocal<Long> _startTime;

  /**
   * Instantiates a new Histogram dynamic metric.
   * @param metricName the metric name
   */
  public RebalanceLatencyGauge(String metricName, long slidingTimeWindow) {
    super(metricName, new Histogram(
        new SlidingTimeWindowArrayReservoir(slidingTimeWindow, TimeUnit.MILLISECONDS)));
    _metricName = metricName;
    _startTime = ThreadLocal.withInitial(() -> VALUE_NOT_SET);
  }

  /**
   * WARNING: this method is not thread-safe.
   * Calling this method multiple times would simply overwrite the previous state. This is because
   * the rebalancer could fail at any point, and we want it to recover gracefully by resetting the
   * internal state of this metric.
   */
  @Override
  public void startMeasuringLatency() {
    reset();
    _startTime.set(System.currentTimeMillis());
  }

  /**
   * WARNING: this method is not thread-safe.
   */
  @Override
  public void endMeasuringLatency() {
    if (_startTime.get() == VALUE_NOT_SET) {
      LOG.error(
          "Needs to call startMeasuringLatency first! Ignoring and resetting the metric. Metric name: {}",
          _metricName);
      return;
    }
    synchronized (this) {
      _lastEmittedMetricValue = System.currentTimeMillis() - _startTime.get();
      updateValue(_lastEmittedMetricValue);
    }
    reset();
  }

  /**
   * Returns the most recently emitted metric value at the time of the call.
   * @return
   */
  @Override
  public Long getLastEmittedMetricValue() {
    return _lastEmittedMetricValue;
  }

  /**
   * Resets the internal state of this metric.
   */
  private void reset() {
    _startTime.set(VALUE_NOT_SET);
  }
}
