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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.concurrent.TimeUnit;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

import javax.management.JMException;
import java.util.ArrayList;
import java.util.List;

public class ClusterEventMonitor extends DynamicMBeanProvider {
  public enum PhaseName {
    Callback,
    InQueue,
    TotalProcessed
  }

  private static final String CLUSTEREVENT_DN_KEY = "ClusterEventStatus";
  private static final String EVENT_DN_KEY = "eventName";
  private static final String PHASE_DN_KEY = "phaseName";

  private final String _phaseName;

  private SimpleDynamicMetric<Long> _totalDuration;
  private SimpleDynamicMetric<Long> _maxDuration;
  private SimpleDynamicMetric<Long> _count;
  private HistogramDynamicMetric _duration;

  private long _lastResetTime;
  private ClusterStatusMonitor _clusterStatusMonitor;

  public ClusterEventMonitor(ClusterStatusMonitor clusterStatusMonitor, String phaseName) {
    _phaseName = phaseName;
    _clusterStatusMonitor = clusterStatusMonitor;

    _duration = new HistogramDynamicMetric("DurationGauge", new Histogram(
        new SlidingTimeWindowArrayReservoir(DEFAULT_RESET_INTERVAL_MS, TimeUnit.MILLISECONDS)));
    _count = new SimpleDynamicMetric("EventCounter", 0l);
    _maxDuration = new SimpleDynamicMetric("MaxSingleDurationGauge", 0l);
    _totalDuration = new SimpleDynamicMetric("TotalDurationCounter", 0l);
  }

  public ClusterEventMonitor(ClusterStatusMonitor clusterStatusMonitor, String phaseName,
      int histogramTimeWindowMs) {
    _phaseName = phaseName;
    _clusterStatusMonitor = clusterStatusMonitor;

    _duration = new HistogramDynamicMetric("DurationGauge", new Histogram(
        new SlidingTimeWindowArrayReservoir(histogramTimeWindowMs, TimeUnit.MILLISECONDS)));
    _count = new SimpleDynamicMetric("EventCounter", 0l);
    _maxDuration = new SimpleDynamicMetric("MaxSingleDurationGauge", 0l);
    _totalDuration = new SimpleDynamicMetric("TotalDurationCounter", 0l);
  }

  public void reportDuration(long duration) {
    _totalDuration.updateValue(_totalDuration.getValue() + duration);
    _count.updateValue(_count.getValue() + 1);
    _duration.updateValue(duration);
    if (_lastResetTime + DEFAULT_RESET_INTERVAL_MS <= System.currentTimeMillis() ||
        duration > _maxDuration.getValue()) {
      _maxDuration.updateValue(duration);
      _lastResetTime = System.currentTimeMillis();
    }
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s.%s", CLUSTEREVENT_DN_KEY, _clusterStatusMonitor.getClusterName(),
        ClusterStatusMonitor.DEFAULT_TAG, _phaseName);
  }

  private String getBeanName() {
    return String.format("%s,%s=%s,%s=%s", _clusterStatusMonitor.clusterBeanName(), EVENT_DN_KEY,
        "ClusterEvent", PHASE_DN_KEY, _phaseName);
  }

  public ClusterEventMonitor register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_totalDuration);
    attributeList.add(_maxDuration);
    attributeList.add(_count);
    attributeList.add(_duration);
    doRegister(attributeList, _clusterStatusMonitor.getObjectName(getBeanName()));
    return this;
  }
}
