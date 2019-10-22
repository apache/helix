package org.apache.helix.monitoring.metrics.model;

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
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;

/**
 * Represents a latency metric and defines methods to help with calculation. A latency metric gives
 * how long a particular stage in the logic took in milliseconds.
 */
public abstract class LatencyMetric extends HistogramDynamicMetric implements Metric<Long> {
  protected long _startTime;
  protected long _endTime;
  protected String _metricName;

  /**
   * Instantiates a new Histogram dynamic metric.
   * @param metricName the metric name
   * @param metricObject the metric object
   */
  public LatencyMetric(String metricName, Histogram metricObject) {
    super(metricName, metricObject);
    _metricName = metricName;
  }

  /**
   * Starts measuring the latency.
   */
  public abstract void startMeasuringLatency();

  /**
   * Ends measuring the latency.
   */
  public abstract void endMeasuringLatency();

  @Override
  public String getMetricName() {
    return _metricName;
  }

  @Override
  public String toString() {
    return String.format("Metric %s's latency is %d", getMetricName(), getLastEmittedMetricValue());
  }

  @Override
  public DynamicMetric getDynamicMetric() {
    return this;
  }
}
