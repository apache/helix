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

import org.apache.helix.HelixConstants;
import org.apache.helix.InstanceType;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

import javax.management.JMException;
import java.util.ArrayList;
import java.util.List;

public class HelixCallbackMonitor extends DynamicMBeanProvider {
  public static final String MONITOR_TYPE = "Type";
  public static final String MONITOR_KEY = "Key";
  public static final String MONITOR_CHANGE_TYPE = "Change";

  private static final String MBEAN_DESCRIPTION = "Helix Callback Monitor";
  private final String _sensorName;
  private final HelixConstants.ChangeType _changeType;

  private SimpleDynamicMetric<Long> _counter = new SimpleDynamicMetric("Counter", 0l);
  private SimpleDynamicMetric<Long> _unbatchedCounter =
      new SimpleDynamicMetric("UnbatchedCounter", 0l);
  private SimpleDynamicMetric<Long> _totalLatencyCounter =
      new SimpleDynamicMetric("LatencyCounter", 0l);

  private HistogramDynamicMetric _latencyGauge = new HistogramDynamicMetric("LatencyGauge",
      _metricRegistry.histogram(getMetricRegistryNamePrefix() + "LatencyGauge"));

  public HelixCallbackMonitor(InstanceType type, String key, HelixConstants.ChangeType changeType)
      throws JMException {
    _changeType = changeType;
    _sensorName = String
        .format("%s.%s.%s.%s", MonitorDomainNames.HelixCallback.name(), type.name(), key,
            changeType.name());

    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_counter);
    attributeList.add(_unbatchedCounter);
    attributeList.add(_totalLatencyCounter);
    attributeList.add(_latencyGauge);
    register(attributeList, MBEAN_DESCRIPTION, MonitorDomainNames.HelixCallback.name(),
        MONITOR_TYPE, type.name(), MONITOR_KEY, key, MONITOR_CHANGE_TYPE, changeType.name());
  }

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  public HelixConstants.ChangeType getChangeType() {
    return _changeType;
  }

  public void increaseCallbackCounters(long time) {
    _counter.updateValue(_counter.getValue() + 1);
    _totalLatencyCounter.updateValue(_totalLatencyCounter.getValue() + time);
    _latencyGauge.updateValue(time);
  }

  public void increaseCallbackUnbatchedCounters() {
    _unbatchedCounter.updateValue(_unbatchedCounter.getValue() + 1);
  }
}
