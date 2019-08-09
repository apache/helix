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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.apache.helix.util.HelixUtil;

public class ZkClientPathMonitor extends DynamicMBeanProvider {
  public static final String MONITOR_PATH = "PATH";
  private final String _sensorName;
  private final String _type;
  private final String _key;
  private final String _instanceName;
  private final PredefinedPath _path;

  public enum PredefinedPath {
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

  public enum PredefinedMetricDomains {
    WriteTotalLatencyCounter,
    ReadTotalLatencyCounter,
    WriteFailureCounter,
    ReadFailureCounter,
    WriteBytesCounter,
    ReadBytesCounter,
    WriteCounter,
    ReadCounter,
    ReadLatencyGauge,
    WriteLatencyGauge,
    ReadBytesGauge,
    WriteBytesGauge,
    /*
     * The latency between a ZK data change happening in the server side and the client side getting notification.
     */
    DataPropagationLatencyGuage
  }

  private SimpleDynamicMetric<Long> _readCounter;
  private SimpleDynamicMetric<Long> _writeCounter;
  private SimpleDynamicMetric<Long> _readBytesCounter;
  private SimpleDynamicMetric<Long> _writeBytesCounter;
  private SimpleDynamicMetric<Long> _readFailureCounter;
  private SimpleDynamicMetric<Long> _writeFailureCounter;
  private SimpleDynamicMetric<Long> _readTotalLatencyCounter;
  private SimpleDynamicMetric<Long> _writeTotalLatencyCounter;

  private HistogramDynamicMetric _readLatencyGauge;
  private HistogramDynamicMetric _writeLatencyGauge;
  private HistogramDynamicMetric _readBytesGauge;
  private HistogramDynamicMetric _writeBytesGauge;
  private HistogramDynamicMetric _dataPropagationLatencyGauge;

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

    _writeTotalLatencyCounter =
        new SimpleDynamicMetric(PredefinedMetricDomains.WriteTotalLatencyCounter.name(), 0l);
    _readTotalLatencyCounter =
        new SimpleDynamicMetric(PredefinedMetricDomains.ReadTotalLatencyCounter.name(), 0l);
    _writeFailureCounter =
        new SimpleDynamicMetric(PredefinedMetricDomains.WriteFailureCounter.name(), 0l);
    _readFailureCounter =
        new SimpleDynamicMetric(PredefinedMetricDomains.ReadFailureCounter.name(), 0l);
    _writeBytesCounter =
        new SimpleDynamicMetric(PredefinedMetricDomains.WriteBytesCounter.name(), 0l);
    _readBytesCounter =
        new SimpleDynamicMetric(PredefinedMetricDomains.ReadBytesCounter.name(), 0l);
    _writeCounter = new SimpleDynamicMetric(PredefinedMetricDomains.WriteCounter.name(), 0l);
    _readCounter = new SimpleDynamicMetric(PredefinedMetricDomains.ReadCounter.name(), 0l);

    _readLatencyGauge = new HistogramDynamicMetric(PredefinedMetricDomains.ReadLatencyGauge.name(),
        new Histogram(new SlidingTimeWindowArrayReservoir(HelixUtil
            .getSystemPropertyAsLong(RESET_INTERVAL_SYSTEM_PROPERTY_KEY, DEFAULT_RESET_INTERVAL_MS),
            TimeUnit.MILLISECONDS)));
    _writeLatencyGauge =
        new HistogramDynamicMetric(PredefinedMetricDomains.WriteLatencyGauge.name(), new Histogram(
            new SlidingTimeWindowArrayReservoir(HelixUtil
                .getSystemPropertyAsLong(RESET_INTERVAL_SYSTEM_PROPERTY_KEY,
                    DEFAULT_RESET_INTERVAL_MS), TimeUnit.MILLISECONDS)));
    _readBytesGauge = new HistogramDynamicMetric(PredefinedMetricDomains.ReadBytesGauge.name(),
        new Histogram(new SlidingTimeWindowArrayReservoir(HelixUtil
            .getSystemPropertyAsLong(RESET_INTERVAL_SYSTEM_PROPERTY_KEY, DEFAULT_RESET_INTERVAL_MS),
            TimeUnit.MILLISECONDS)));
    _writeBytesGauge = new HistogramDynamicMetric(PredefinedMetricDomains.WriteBytesGauge.name(),
        new Histogram(new SlidingTimeWindowArrayReservoir(HelixUtil
            .getSystemPropertyAsLong(RESET_INTERVAL_SYSTEM_PROPERTY_KEY, DEFAULT_RESET_INTERVAL_MS),
            TimeUnit.MILLISECONDS)));
    _dataPropagationLatencyGauge =
        new HistogramDynamicMetric(PredefinedMetricDomains.DataPropagationLatencyGuage.name(),
            new Histogram(new SlidingTimeWindowArrayReservoir(HelixUtil
                .getSystemPropertyAsLong(RESET_INTERVAL_SYSTEM_PROPERTY_KEY,
                    DEFAULT_RESET_INTERVAL_MS), TimeUnit.MILLISECONDS)));
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
    attributeList.add(_dataPropagationLatencyGauge);

    ObjectName objectName = new ObjectName(String
        .format("%s,%s=%s", ZkClientMonitor.getObjectName(_type, _key, _instanceName).toString(),
            MONITOR_PATH, _path.name()));
    doRegister(attributeList, ZkClientMonitor.MBEAN_DESCRIPTION, objectName);

    return this;
  }

  protected synchronized void record(int bytes, long latencyMilliSec, boolean isFailure,
      boolean isRead) {
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

  public void recordDataPropagationLatency(long latency) {
    _dataPropagationLatencyGauge.updateValue(latency);
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
