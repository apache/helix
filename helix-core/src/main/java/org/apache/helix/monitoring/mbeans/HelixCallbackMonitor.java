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
import org.apache.helix.HelixConstants;
import org.apache.helix.InstanceType;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

import javax.management.JMException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.List;

public class HelixCallbackMonitor extends DynamicMBeanProvider {
  public static final String MONITOR_TYPE = "Type";
  public static final String MONITOR_KEY = "Key";
  public static final String MONITOR_CHANGE_TYPE = "Change";

  private static final String MBEAN_DESCRIPTION = "Helix Callback Monitor";
  private final String _sensorName;
  private final HelixConstants.ChangeType _changeType;
  private final InstanceType _type;
  private final String _clusterName;
  private final String _instanceName;

  private SimpleDynamicMetric<Long> _counter;
  private SimpleDynamicMetric<Long> _unbatchedCounter;
  private SimpleDynamicMetric<Long> _totalLatencyCounter;

  private HistogramDynamicMetric _latencyGauge;

  public HelixCallbackMonitor(InstanceType type, String clusterName, String instanceName,
      HelixConstants.ChangeType changeType) throws JMException {
    _changeType = changeType;
    _type = type;
    _clusterName = clusterName;
    _instanceName = instanceName;

    // Don't put instanceName into sensor name. This detail information is in the MBean name already.
    _sensorName = String
        .format("%s.%s.%s.%s", MonitorDomainNames.HelixCallback.name(), type.name(), clusterName,
            changeType.name());

    _latencyGauge = new HistogramDynamicMetric("LatencyGauge", new Histogram(
        new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
    _totalLatencyCounter = new SimpleDynamicMetric("LatencyCounter", 0l);
    _unbatchedCounter = new SimpleDynamicMetric("UnbatchedCounter", 0l);
    _counter = new SimpleDynamicMetric("Counter", 0l);
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

  @Override
  public HelixCallbackMonitor register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_counter);
    attributeList.add(_unbatchedCounter);
    attributeList.add(_totalLatencyCounter);
    attributeList.add(_latencyGauge);
    doRegister(attributeList, MBEAN_DESCRIPTION, MonitorDomainNames.HelixCallback.name(),
        MONITOR_TYPE, _type.name(), MONITOR_KEY,
        _clusterName + (_instanceName == null ? "" : "." + _instanceName), MONITOR_CHANGE_TYPE,
        _changeType.name());
    return this;
  }
}
