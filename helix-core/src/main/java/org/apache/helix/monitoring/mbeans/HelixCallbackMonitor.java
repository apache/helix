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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.helix.HelixConstants;
import org.apache.helix.InstanceType;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

import javax.management.JMException;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;

public class HelixCallbackMonitor {
  public static final String MONITOR_TYPE = "Type";
  public static final String MONITOR_KEY = "Key";
  public static final String MONITOR_CHANGE_TYPE = "Change";
  private static final String MBEAN_DESCRIPTION = "Helix Callback Monitor";

  private static final MetricRegistry _metricRegistry = new MetricRegistry();

  private DynamicMBeanProvider _dynamicMBeanProvider;
  private ObjectName _objectName;
  private final InstanceType _instanceType;
  private final String _key;
  private final HelixConstants.ChangeType _changeType;

  private SimpleDynamicMetric<Long> _counter = new SimpleDynamicMetric("Counter", 0l);
  private SimpleDynamicMetric<Long> _unbatchedCounter =
      new SimpleDynamicMetric("UnbatchedCounter", 0l);
  private SimpleDynamicMetric<Long> _totalLatencyCounter =
      new SimpleDynamicMetric("LatencyCounter", 0l);

  private HistogramDynamicMetric _latencyGauge = new HistogramDynamicMetric("LatencyGauge",
      _metricRegistry.histogram(toString() + "LatencyGauge"));

  public HelixCallbackMonitor(InstanceType type, String key, HelixConstants.ChangeType changeType)
      throws JMException {
    _instanceType = type;
    _key = key;
    _changeType = changeType;

    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_counter);
    attributeList.add(_unbatchedCounter);
    attributeList.add(_totalLatencyCounter);
    attributeList.add(_latencyGauge);

    _dynamicMBeanProvider = new DynamicMBeanProvider(String
        .format("%s.%s.%s.%s", MonitorDomainNames.HelixCallback.name(), _instanceType.name(), _key,
            _changeType.name()), MBEAN_DESCRIPTION, attributeList);

    register(type, key, changeType);
  }

  private void register(InstanceType type, String key, HelixConstants.ChangeType changeType)
      throws JMException {
    _objectName = MBeanRegistrar
        .register(_dynamicMBeanProvider, MonitorDomainNames.HelixCallback.name(), MONITOR_TYPE,
            type.name(), MONITOR_KEY, key, MONITOR_CHANGE_TYPE, changeType.name());
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

  /**
   * After unregistered, the MBean can't be registered again, a new monitor has be to created.
   */
  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
    _metricRegistry.removeMatching(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.startsWith(toString());
      }
    });
  }
}
