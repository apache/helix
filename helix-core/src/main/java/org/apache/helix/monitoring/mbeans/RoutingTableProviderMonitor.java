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
import org.apache.helix.PropertyType;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RoutingTableProviderMonitor extends DynamicMBeanProvider {
  public static final String DATA_TYPE_KEY = "DataType";
  public static final String CLUSTER_KEY = "Cluster";
  public static final String DEFAULT = "DEFAULT";

  private static final String MBEAN_DESCRIPTION = "Helix RoutingTableProvider Monitor";
  private final String _sensorName;
  private final PropertyType _propertyType;
  private final String _clusterName;

  private SimpleDynamicMetric<Long> _callbackCounter;
  private SimpleDynamicMetric<Long> _eventQueueSizeGauge;
  private SimpleDynamicMetric<Long> _dataRefreshCounter;
  private HistogramDynamicMetric _dataRefreshLatencyGauge;
  private HistogramDynamicMetric _statePropLatencyGauge;

  public RoutingTableProviderMonitor(final PropertyType propertyType, String clusterName) {
    _propertyType = propertyType;
    _clusterName = clusterName == null ? DEFAULT : clusterName;

    // Don't put instanceName into sensor name. This detail information is in the MBean name already.
    _sensorName = String
        .format("%s.%s.%s", MonitorDomainNames.RoutingTableProvider.name(), _clusterName,
            _propertyType.name());

    _dataRefreshLatencyGauge = new HistogramDynamicMetric("DataRefreshLatencyGauge", new Histogram(
        new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
    _callbackCounter = new SimpleDynamicMetric("CallbackCounter", 0l);
    _eventQueueSizeGauge = new SimpleDynamicMetric("EventQueueSizeGauge", 0l);
    _dataRefreshCounter = new SimpleDynamicMetric("DataRefreshCounter", 0l);
    if (propertyType.equals(PropertyType.CURRENTSTATES)) {
      _statePropLatencyGauge = new HistogramDynamicMetric("StatePropagationLatencyGauge",
          new Histogram(
              new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
    }
  }

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  private ObjectName getMBeanName() throws MalformedObjectNameException {
    return new ObjectName(String
        .format("%s:%s=%s,%s=%s", MonitorDomainNames.RoutingTableProvider.name(), CLUSTER_KEY,
            _clusterName, DATA_TYPE_KEY, _propertyType.name()));
  }

  public void increaseCallbackCounters(long currentQueueSize) {
    _callbackCounter.updateValue(_callbackCounter.getValue() + 1);
    _eventQueueSizeGauge.updateValue(currentQueueSize);
  }

  public void increaseDataRefreshCounters(long startTime) {
    _dataRefreshCounter.updateValue(_dataRefreshCounter.getValue() + 1);
    _dataRefreshLatencyGauge.updateValue(System.currentTimeMillis() - startTime);
  }

  public void recordStatePropagationLatency(long latency) {
    if (_statePropLatencyGauge != null) {
      _statePropLatencyGauge.updateValue(latency);
    }
  }

  @Override
  public RoutingTableProviderMonitor register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_dataRefreshLatencyGauge);
    attributeList.add(_callbackCounter);
    attributeList.add(_eventQueueSizeGauge);
    attributeList.add(_dataRefreshCounter);
    if (_statePropLatencyGauge != null) {
      attributeList.add(_statePropLatencyGauge);
    }

    doRegister(attributeList, MBEAN_DESCRIPTION, getMBeanName());
    return this;
  }
}
