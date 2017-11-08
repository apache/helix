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

import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;

public class ZkClientPathMonitor extends DynamicMBeanProvider {
  public static final String MONITOR_PATH = "PATH";
  private static final String MBEAN_DESCRIPTION = "Helix Zookeeper Client Monitor";
  private final String _sensorName;
  private final String _type;
  private final String _key;
  private final String _instanceName;
  private final PredefinedPath _path;

  protected enum PredefinedPath {
    IdealStates(".*/IDEALSTATES/.*"),
    Instances(".*/INSTANCES/.*"),
    Configs(".*/CONFIGS/.*"),
    Controller(".*/CONTROLLER/.*"),
    ExternalView(".*/EXTERNALVIEW/.*"),
    LiveInstances(".*/LIVEINSTANCES/.*"),
    PropertyStore(".*/PROPERTYSTORE/.*"),
    CurrentStates(".*/CURRENTSTATES/.*"),
    Messages(".*/MESSAGES/.*"),
    Root(".*");

    private final String _matchString;

    PredefinedPath(String matchString) {
      _matchString = matchString;
    }

    public boolean match(String path) {
      return path.matches(this._matchString);
    }
  }

  private SimpleDynamicMetric<Long> _readCounter = new SimpleDynamicMetric("ReadCounter", 0l);
  private SimpleDynamicMetric<Long> _writeCounter = new SimpleDynamicMetric("WriteCounter", 0l);
  private SimpleDynamicMetric<Long> _readBytesCounter =
      new SimpleDynamicMetric("ReadBytesCounter", 0l);
  private SimpleDynamicMetric<Long> _writeBytesCounter =
      new SimpleDynamicMetric("WriteBytesCounter", 0l);
  private SimpleDynamicMetric<Long> _readFailureCounter =
      new SimpleDynamicMetric("ReadFailureCounter", 0l);
  private SimpleDynamicMetric<Long> _writeFailureCounter =
      new SimpleDynamicMetric("WriteFailureCounter", 0l);
  private SimpleDynamicMetric<Long> _readTotalLatencyCounter =
      new SimpleDynamicMetric("ReadTotalLatencyCounter", 0l);
  private SimpleDynamicMetric<Long> _writeTotalLatencyCounter =
      new SimpleDynamicMetric("WriteTotalLatencyCounter", 0l);

  private HistogramDynamicMetric _readLatencyGauge = new HistogramDynamicMetric("ReadLatencyGauge",
      _metricRegistry.histogram(getMetricName("ReadLatencyGauge")));
  private HistogramDynamicMetric _writeLatencyGauge =
      new HistogramDynamicMetric("WriteLatencyGauge",
          _metricRegistry.histogram(getMetricName("WriteLatencyGauge")));
  private HistogramDynamicMetric _readBytesGauge = new HistogramDynamicMetric("ReadBytesGauge",
      _metricRegistry.histogram(getMetricName("ReadBytesGauge")));
  private HistogramDynamicMetric _writeBytesGauge = new HistogramDynamicMetric("WriteBytesGauge",
      _metricRegistry.histogram(getMetricName("WriteBytesGauge")));

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  public ZkClientPathMonitor(PredefinedPath path, String monitorType, String monitorKey,
      String monitorInstanceName) {
    _type = monitorType;
    _key = monitorKey;
    _instanceName = monitorInstanceName;
    _path = path;
    _sensorName = String
        .format("%s.%s.%s.%s", MonitorDomainNames.HelixZkClient.name(), monitorType, monitorKey,
            path.name());
  }

  public ZkClientPathMonitor register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_readCounter);
    attributeList.add(_writeCounter);
    attributeList.add(_readBytesCounter);
    attributeList.add(_writeBytesCounter);
    attributeList.add(_readFailureCounter);
    attributeList.add(_writeFailureCounter);
    attributeList.add(_readTotalLatencyCounter);
    attributeList.add(_writeTotalLatencyCounter);
    attributeList.add(_readLatencyGauge);
    attributeList.add(_writeLatencyGauge);
    attributeList.add(_readBytesGauge);
    attributeList.add(_writeBytesGauge);

    ObjectName objectName = new ObjectName(String.format("%s,%s=%s",
        ZkClientMonitor.getObjectName(_type, _key, _instanceName).toString(),
        MONITOR_PATH, _path.name()));
    doRegister(attributeList, MBEAN_DESCRIPTION, objectName);

    return this;
  }

  protected void record(int bytes, long latencyMilliSec, boolean isFailure, boolean isRead) {
    if (isFailure) {
      increaseFailureCounter(isRead);
    } else {
      increaseCounter(isRead);
      increaseTotalLatency(isRead, latencyMilliSec);
      if (bytes > 0) {
        increaseBytesCounter(isRead, bytes);
      }
    }
  }

  private void increaseFailureCounter(boolean isRead) {
    if (isRead) {
      _readFailureCounter.updateValue(_readFailureCounter.getValue() + 1);
    } else {
      _writeFailureCounter.updateValue(_writeFailureCounter.getValue() + 1);
    }
  }

  private void increaseCounter(boolean isRead) {
    if (isRead) {
      _readCounter.updateValue(_readCounter.getValue() + 1);
    } else {
      _writeCounter.updateValue(_writeCounter.getValue() + 1);
    }
  }

  private void increaseBytesCounter(boolean isRead, int bytes) {
    if (isRead) {
      _readBytesCounter.updateValue(_readBytesCounter.getValue() + bytes);
      _readBytesGauge.updateValue((long) bytes);
    } else {
      _writeBytesCounter.updateValue(_writeBytesCounter.getValue() + bytes);
      _writeBytesGauge.updateValue((long) bytes);
    }
  }

  private void increaseTotalLatency(boolean isRead, long latencyDelta) {
    if (isRead) {
      _readTotalLatencyCounter.updateValue(_readTotalLatencyCounter.getValue() + latencyDelta);
      _readLatencyGauge.updateValue(latencyDelta);
    } else {
      _writeTotalLatencyCounter.updateValue(_writeTotalLatencyCounter.getValue() + latencyDelta);
      _writeLatencyGauge.updateValue(latencyDelta);
    }
  }
}
