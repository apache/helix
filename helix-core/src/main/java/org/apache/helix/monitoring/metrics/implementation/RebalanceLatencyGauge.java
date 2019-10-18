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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.concurrent.TimeUnit;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceLatencyGauge extends LatencyMetric {
  private static final Logger LOG = LoggerFactory.getLogger(RebalanceLatencyGauge.class);
  private static final long VALUE_NOT_SET = -1;
  private long _lastEmittedMetricValue = VALUE_NOT_SET;

  /**
   * Instantiates a new Histogram dynamic metric.
   * @param metricName the metric name
   */
  public RebalanceLatencyGauge(String metricName, long slidingTimeWindow) {
    super(metricName, new Histogram(
        new SlidingTimeWindowArrayReservoir(slidingTimeWindow, TimeUnit.MILLISECONDS)));
    _metricName = metricName;
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
    _startTime = System.currentTimeMillis();
  }

  /**
   * WARNING: this method is not thread-safe.
   */
  @Override
  public void endMeasuringLatency() {
    if (_startTime == VALUE_NOT_SET || _endTime != VALUE_NOT_SET) {
      LOG.error(
          "Needs to call startMeasuringLatency first! Ignoring and resetting the metric. Metric name: {}",
          _metricName);
      reset();
      return;
    }
    _endTime = System.currentTimeMillis();
    _lastEmittedMetricValue = _endTime - _startTime;
    updateValue(_lastEmittedMetricValue);
    reset();
  }

  /**
   * Returns the most recently emitted metric value at the time of the call.
   * @return
   */
  @Override
  public long getLastEmittedMetricValue() {
    return _lastEmittedMetricValue;
  }

  /**
   * Resets the internal state of this metric.
   */
  private void reset() {
    _startTime = VALUE_NOT_SET;
    _endTime = VALUE_NOT_SET;
  }
}
