package org.apache.helix.zookeeper.zkclient.metric;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.apache.helix.monitoring.mbeans.exception.MetricException;
import org.apache.helix.zookeeper.zkclient.ZkEventThread;


public class ZkClientMonitor extends DynamicMBeanProvider {

  public static final String MONITOR_TYPE = "Type";
  public static final String MONITOR_KEY = "Key";
  protected static final String MBEAN_DESCRIPTION = "Helix Zookeeper Client Monitor";

  public enum AccessType {
    READ, WRITE
  }

  private String _sensorName;
  private String _monitorType;
  private String _monitorKey;
  private String _monitorInstanceName;
  private boolean _monitorRootOnly;

  private SimpleDynamicMetric<Long> _stateChangeEventCounter;
  private SimpleDynamicMetric<Long> _expiredSessionCounter;
  private SimpleDynamicMetric<Long> _dataChangeEventCounter;
  private SimpleDynamicMetric<Long> _outstandingRequestGauge;
  private SimpleDynamicMetric<Long> _znodeCompressCounter;

  private ZkThreadMetric _zkEventThreadMetric;

  private Map<ZkClientPathMonitor.PredefinedPath, ZkClientPathMonitor> _zkClientPathMonitorMap =
      new ConcurrentHashMap<>();

  public ZkClientMonitor(String monitorType, String monitorKey, String monitorInstanceName,
      boolean monitorRootOnly, ZkEventThread zkEventThread) {
    if (monitorKey == null || monitorKey.isEmpty() || monitorType == null || monitorType
        .isEmpty()) {
      throw new MetricException("Cannot create ZkClientMonitor without monitor key and type.");
    }

    _sensorName =
        String.format("%s.%s.%s", MonitorDomainNames.HelixZkClient.name(), monitorType, monitorKey);
    _monitorType = monitorType;
    _monitorKey = monitorKey;
    _monitorInstanceName = monitorInstanceName;
    _monitorRootOnly = monitorRootOnly;

    _stateChangeEventCounter = new SimpleDynamicMetric<>("StateChangeEventCounter", 0L);
    _expiredSessionCounter = new SimpleDynamicMetric<>("ExpiredSessionCounter", 0L);
    _dataChangeEventCounter = new SimpleDynamicMetric<>("DataChangeEventCounter", 0L);
    _outstandingRequestGauge = new SimpleDynamicMetric<>("OutstandingRequestGauge", 0L);
    _znodeCompressCounter = new SimpleDynamicMetric<>("CompressedZnodeWriteCounter", 0L);

    if (zkEventThread != null) {
      boolean result = setAndInitZkEventThreadMonitor(zkEventThread);
      if (!result) {
        _logger.error("register zkEventThreadMonitor failed due to an existing one.");
      }
    }

    for (ZkClientPathMonitor.PredefinedPath path : ZkClientPathMonitor.PredefinedPath.values()) {
      // If monitor root path only, check if the current path is Root.
      // Otherwise, add monitors for every path.
      if (!_monitorRootOnly || path.equals(ZkClientPathMonitor.PredefinedPath.Root)) {
        _zkClientPathMonitorMap.put(path,
            new ZkClientPathMonitor(path, _monitorType, _monitorKey, _monitorInstanceName)
        );
      }
    }
  }

  public static ObjectName getObjectName(String monitorType, String monitorKey,
      String monitorInstanceName) throws MalformedObjectNameException {
    return MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), MONITOR_TYPE, monitorType,
            MONITOR_KEY,
            (monitorKey + (monitorInstanceName == null ? "" : "." + monitorInstanceName)));
  }

  public synchronized boolean setAndInitZkEventThreadMonitor(ZkEventThread zkEventThread) {
    if (_zkEventThreadMetric == null) {
      _zkEventThreadMetric = new ZkThreadMetric(zkEventThread);
      return true;
    }
    return false;
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_dataChangeEventCounter);
    attributeList.add(_outstandingRequestGauge);
    attributeList.add(_stateChangeEventCounter);
    attributeList.add(_expiredSessionCounter);
    attributeList.add(_znodeCompressCounter);
    if (_zkEventThreadMetric != null) {
      attributeList.add(_zkEventThreadMetric);
    }
    doRegister(attributeList, MBEAN_DESCRIPTION,
        getObjectName(_monitorType, _monitorKey, _monitorInstanceName));
    _zkClientPathMonitorMap.values().stream().forEach( monitor -> {
      if (monitor != null) {
        try {
          monitor.register();
        } catch (JMException e) {
           _logger.error(" {} failed registration", monitor, e);
        }
      }
    });
    return this;
  }

  /**
   * After unregistered, the MBean can't be registered again, a new monitor has be to created.
   */
  public void unregister() {
    super.unregister();
    for (ZkClientPathMonitor zkClientPathMonitor : _zkClientPathMonitorMap.values()) {
      zkClientPathMonitor.unregister();
    }
  }

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  public void increaseStateChangeEventCounter() {
    synchronized (_stateChangeEventCounter) {
      _stateChangeEventCounter.updateValue(_stateChangeEventCounter.getValue() + 1);
    }
  }

  public void increasExpiredSessionCounter() {
    synchronized (_expiredSessionCounter) {
      _expiredSessionCounter.updateValue(_expiredSessionCounter.getValue() + 1);
    }
  }

  public void increaseDataChangeEventCounter() {
    synchronized (_dataChangeEventCounter) {
      _dataChangeEventCounter.updateValue(_dataChangeEventCounter.getValue() + 1);
    }
  }

  public void increaseOutstandingRequestGauge() {
    synchronized (_outstandingRequestGauge) {
      _outstandingRequestGauge.updateValue(_outstandingRequestGauge.getValue() + 1);
    }
  }

  public void decreaseOutstandingRequestGauge() {
    synchronized (_outstandingRequestGauge) {
      _outstandingRequestGauge.updateValue(_outstandingRequestGauge.getValue() - 1);
    }
  }

  public void increaseZnodeCompressCounter() {
    synchronized (_znodeCompressCounter) {
      _znodeCompressCounter.updateValue(_znodeCompressCounter.getValue() + 1);
    }
  }

  public void recordDataPropagationLatency(String path, long latencyMilliSec) {
    findZkClientPathMonitor(path, (m) -> m.recordDataPropagationLatency(latencyMilliSec));
  }

  private void record(String path, int bytes, long latencyMilliSec, boolean isFailure,
      boolean isRead) {
    findZkClientPathMonitor(path, (m) -> m.record(bytes, latencyMilliSec, isFailure, isRead));
  }

  public void record(String path, int dataSize, long startTimeMilliSec, AccessType accessType) {
    switch (accessType) {
    case READ:
      record(path, dataSize, System.currentTimeMillis() - startTimeMilliSec, false, true);
      return;
    case WRITE:
      record(path, dataSize, System.currentTimeMillis() - startTimeMilliSec, false, false);
      return;
    default:
      return;
    }
  }

  public void recordFailure(String path, AccessType accessType) {
    switch (accessType) {
    case READ:
      record(path, 0, 0, true, true);
      return;
    case WRITE:
      record(path, 0, 0, true, false);
      return;
    default:
      return;
    }
  }

  /**
   * Records metrics for async operations
   */
  private void recordAsync(String path, int bytes, long latencyMilliSec, boolean isFailure,
      AccessType accessType) {
    findZkClientPathMonitor(path, (m) -> m.recordAsync(bytes, latencyMilliSec, isFailure, accessType));
  }

  public void recordAsync(String path, int dataSize, long startTimeMilliSec, AccessType accessType) {
    recordAsync(path, dataSize, System.currentTimeMillis() - startTimeMilliSec, false, accessType);
  }

  public void recordAsyncFailure(String path, AccessType accessType) {
    recordAsync(path, 0, 0, true, accessType);
  }

  private void findZkClientPathMonitor(String path, Consumer<ZkClientPathMonitor> onMatch) {
    if (path == null) {
      return;
    }
    _zkClientPathMonitorMap.forEach((predefinedPath, zkClientPathMonitor) -> {
      if (predefinedPath.match(path)) {
        onMatch.accept(zkClientPathMonitor);
      }
    });
  }

  class ZkThreadMetric extends DynamicMetric<ZkEventThread, ZkEventThread> {
    public ZkThreadMetric(ZkEventThread eventThread) {
      super("ZkEventThead", eventThread);
    }

    @Override
    protected Set<MBeanAttributeInfo> generateAttributeInfos(String metricName,
        ZkEventThread eventThread) {
      Set<MBeanAttributeInfo> attributeInfoSet = new HashSet<>();
      attributeInfoSet.add(new MBeanAttributeInfo("PendingCallbackGauge", Long.TYPE.getName(),
          DEFAULT_ATTRIBUTE_DESCRIPTION, true, false, false));
      attributeInfoSet.add(new MBeanAttributeInfo("TotalCallbackCounter", Long.TYPE.getName(),
          DEFAULT_ATTRIBUTE_DESCRIPTION, true, false, false));
      attributeInfoSet.add(
          new MBeanAttributeInfo("TotalCallbackHandledCounter", Long.TYPE.getName(),
              DEFAULT_ATTRIBUTE_DESCRIPTION, true, false, false));
      return attributeInfoSet;
    }

    @Override
    public Object getAttributeValue(String attributeName) {
      switch (attributeName) {
      case "PendingCallbackGauge":
        return getMetricObject().getPendingEventsCount();
      case "TotalCallbackCounter":
        return getMetricObject().getTotalEventCount();
      case "TotalCallbackHandledCounter":
        return getMetricObject().getTotalHandledEventCount();
      default:
        throw new MetricException("Unknown attribute name: " + attributeName);
      }
    }

    @Override
    public void updateValue(ZkEventThread newEventThread) {
      setMetricObject(newEventThread);
    }
  }
}
