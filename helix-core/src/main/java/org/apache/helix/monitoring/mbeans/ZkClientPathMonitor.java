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

import javax.management.JMException;
import javax.management.ObjectName;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.helix.HelixException;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZkClientPathMonitor {
  public static final String MONITOR_PATH = "PATH";
  private static final String MBEAN_DESCRIPTION = "Helix Zookeeper Client Monitor";

  private static final MetricRegistry _metricRegistry = new MetricRegistry();

  private DynamicMBeanProvider _dynamicMBeanProvider;
  private ObjectName _objectName;
  private String _monitorType;
  private String _monitorKey;
  private String _path;

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
      _metricRegistry.histogram(toString() + "ReadLatencyGauge"));
  private HistogramDynamicMetric _writeLatencyGauge =
      new HistogramDynamicMetric("WriteLatencyGauge",
          _metricRegistry.histogram(toString() + "WriteLatencyGauge"));
  private HistogramDynamicMetric _readBytesGauge = new HistogramDynamicMetric("ReadBytesGauge",
      _metricRegistry.histogram(toString() + "ReadBytesGauge"));
  private HistogramDynamicMetric _writeBytesGauge = new HistogramDynamicMetric("WriteBytesGauge",
      _metricRegistry.histogram(toString() + "WriteBytesGauge"));

  protected ZkClientPathMonitor(String path, String monitorType, String monitorKey)
      throws JMException {
    if (monitorKey == null || monitorKey.isEmpty() || monitorType == null || monitorType
        .isEmpty()) {
      throw new HelixException("Cannot create ZkClientMonitor without monitor key and type.");
    }

    _monitorType = monitorType;
    _monitorKey = monitorKey;
    _path = path;

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

    _dynamicMBeanProvider = new DynamicMBeanProvider(String
        .format("%s.%s.%s.%s", MonitorDomainNames.HelixZkClient.name(), _monitorType, _monitorKey,
            _path), MBEAN_DESCRIPTION, attributeList);

    regitster(path, monitorType, monitorKey);
  }

  private void regitster(String path, String monitorType, String monitorKey) throws JMException {
    _objectName = MBeanRegistrar
        .register(_dynamicMBeanProvider, MonitorDomainNames.HelixZkClient.name(),
            ZkClientMonitor.MONITOR_TYPE, monitorType, ZkClientMonitor.MONITOR_KEY, monitorKey,
            MONITOR_PATH, path);
  }

  /**
   * After unregistered, the MBean can't be registered again, a new monitor has be to created.
   */
  protected void unregister() {
    MBeanRegistrar.unregister(_objectName);
    _metricRegistry.removeMatching(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.startsWith(toString());
      }
    });
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
