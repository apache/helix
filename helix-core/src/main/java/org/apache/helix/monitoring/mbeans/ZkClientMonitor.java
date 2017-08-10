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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.JMException;
import javax.management.ObjectName;

public class ZkClientMonitor implements ZkClientMonitorMBean {
  private static final long RESET_INTERVAL = 1000 * 60 * 10; // 1 hour
  public static final String MONITOR_TYPE = "Type";
  public static final String MONITOR_KEY = "Key";
  public static final String SESSION_ID_PROPERTY_NAME = "SessionId";
  public static final String CUSTOMIZED_PROPERTY_NAME = "CustomizedKey";

  private ObjectName _objectName;
  private String _monitorType;
  private String _monitorKey;

  private enum PredefinedPath {
    IdealStates(".*/IDEALSTATES/.*"),
    Instances(".*/INSTANCES/.*"),
    Configs(".*/CONFIGS/.*"),
    Controller(".*/CONTROLLER/.*"),
    ExternalView(".*/EXTERNALVIEW/.*"),
    LiveInstances(".*/LIVEINSTANCES/.*"),
    PropertyStore(".*/PROPERTYSTORE/.*"),
    CurrentStates(".*/CURRENTSTATES/.*"),
    Messages(".*/MESSAGES/.*"),
    Default(".*");

    private final String _matchString;

    PredefinedPath(String matchString) {
      _matchString = matchString;
    }

    public boolean match(String path) {
      return path.matches(this._matchString);
    }
  }

  private long _lastResetTime = 0;

  private long _stateChangeEventCounter;
  private long _dataChangeEventCounter;

  private Map<PredefinedPath, Long> _readCounterMap = new ConcurrentHashMap<>();
  private Map<PredefinedPath, Long> _writeCounterMap = new ConcurrentHashMap<>();
  private Map<PredefinedPath, Long> _readBytesCounterMap = new ConcurrentHashMap<>();
  private Map<PredefinedPath, Long> _writeBytesCounterMap = new ConcurrentHashMap<>();
  private Map<PredefinedPath, Long> _readFailureCounterMap = new ConcurrentHashMap<>();
  private Map<PredefinedPath, Long> _writeFailureCounterMap = new ConcurrentHashMap<>();

  private Map<PredefinedPath, Long> _readTotalLatencyMap = new ConcurrentHashMap<>();
  private Map<PredefinedPath, Long> _writeTotalLatencyMap = new ConcurrentHashMap<>();
  private Map<PredefinedPath, Long> _readMaxLatencyMap = new ConcurrentHashMap<>();
  private Map<PredefinedPath, Long> _writeMaxLatencyMap = new ConcurrentHashMap<>();

  public ZkClientMonitor(String monitorType, String monitorKey) throws JMException {
    initCounterMaps();
    _monitorType = monitorType;
    _monitorKey = monitorKey;
    regitster(monitorType, monitorKey);
  }

  private void initCounterMaps() {
    for (PredefinedPath path : PredefinedPath.values()) {
      _readCounterMap.put(path, 0L);
      _writeCounterMap.put(path, 0L);
      _readBytesCounterMap.put(path, 0L);
      _writeBytesCounterMap.put(path, 0L);
      _readTotalLatencyMap.put(path, 0L);
      _writeTotalLatencyMap.put(path, 0L);
      _readMaxLatencyMap.put(path, 0L);
      _writeMaxLatencyMap.put(path, 0L);
      _readFailureCounterMap.put(path, 0L);
      _writeFailureCounterMap.put(path, 0L);
    }
  }

  public void regitster(String monitorType, String monitorKey) throws JMException {
    _objectName = MBeanRegistrar
        .register(this, MonitorDomainNames.HelixZkClient.name(), MONITOR_TYPE, monitorType,
            MONITOR_KEY, monitorKey);
  }

  /**
   * After unregistered, the MBean can't be registered again, a new monitor has be to created.
   */
  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
  }

  @Override public String getSensorName() {
    return String
        .format("%s.%s.%s", MonitorDomainNames.HelixZkClient.name(), _monitorType, _monitorKey);
  }

  public void increaseStateChangeEventCounter() {
    _stateChangeEventCounter++;
  }

  @Override
  public long getStateChangeEventCounter() {
    return _stateChangeEventCounter;
  }

  public void increaseDataChangeEventCounter() {
    _dataChangeEventCounter++;
  }

  @Override
  public long getDataChangeEventCounter() {
    return _dataChangeEventCounter;
  }

  private void record(String path, int bytes, long latencyMilliSec, boolean isFailure,
      boolean isRead) {
    Map<PredefinedPath, Long> _counterMap = isRead ? _readCounterMap : _writeCounterMap;
    Map<PredefinedPath, Long> _bytesCounterMap =
        isRead ? _readBytesCounterMap : _writeBytesCounterMap;
    Map<PredefinedPath, Long> _failureCounterMap =
        isRead ? _readFailureCounterMap : _writeFailureCounterMap;
    Map<PredefinedPath, Long> _totalLatencyMap =
        isRead ? _readTotalLatencyMap : _writeTotalLatencyMap;
    Map<PredefinedPath, Long> _maxLatencyMap = isRead ? _readMaxLatencyMap : _writeMaxLatencyMap;

    for (PredefinedPath predefinedPath : PredefinedPath.values()) {
      if (predefinedPath.match(path)) {
        if (isFailure) {
          _failureCounterMap.put(predefinedPath, _failureCounterMap.get(predefinedPath) + 1);
        } else {
          _counterMap.put(predefinedPath, _counterMap.get(predefinedPath) + 1);
          _totalLatencyMap
              .put(predefinedPath, _totalLatencyMap.get(predefinedPath) + latencyMilliSec);

          if (_lastResetTime + RESET_INTERVAL <= System.currentTimeMillis() ||
              latencyMilliSec > _maxLatencyMap.get(predefinedPath)) {
            _maxLatencyMap.put(predefinedPath, latencyMilliSec);
            _lastResetTime = System.currentTimeMillis();
          }

          if (bytes > 0) {
            _bytesCounterMap
                .put(predefinedPath, _bytesCounterMap.get(predefinedPath) + bytes);
          }
        }
      }
    }
  }

  public void recordReadFailure(String path) {
    record(path, 0, 0, true, true);
  }

  public void recordRead(String path, int dataSize, long startTimeMilliSec) {
    record(path, dataSize, System.currentTimeMillis() - startTimeMilliSec, false, true);
  }

  public void recordWriteFailure(String path) {
    record(path, 0, 0, true, false);
  }

  public void recordWrite(String path, int dataSize, long startTimeMilliSec) {
    record(path, dataSize, System.currentTimeMillis() - startTimeMilliSec, false, false);
  }

  @Override public long getReadCounter() {
    return _readCounterMap.get(PredefinedPath.Default);
  }

  @Override public long getReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.Default);
  }

  @Override public long getWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.Default);
  }

  @Override public long getWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.Default);
  }

  @Override public long getTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.Default);
  }

  @Override public long getTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.Default);
  }

  @Override public long getMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.Default);
  }

  @Override public long getMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.Default);
  }

  @Override public long getReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.Default);
  }

  @Override public long getWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.Default);
  }

  @Override public long getIdealStatesReadCounter() {
    return _readCounterMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getIdealStatesWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.IdealStates);
  }

  @Override public long getInstancesReadCounter() {
    return _readCounterMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.Instances);
  }

  @Override public long getInstancesWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.Instances);
  }

  @Override public long getConfigsReadCounter() {
    return _readCounterMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.Configs);
  }

  @Override public long getConfigsWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.Configs);
  }

  @Override public long getControllerReadCounter() {
    return _readCounterMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.Controller);
  }

  @Override public long getControllerWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.Controller);
  }

  @Override public long getExternalViewReadCounter() {
    return _readCounterMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getExternalViewWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.ExternalView);
  }

  @Override public long getLiveInstancesReadCounter() {
    return _readCounterMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getLiveInstancesWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.LiveInstances);
  }

  @Override public long getPropertyStoreReadCounter() {
    return _readCounterMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getPropertyStoreWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.PropertyStore);
  }

  @Override public long getCurrentStatesReadCounter() {
    return _readCounterMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getCurrentStatesWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.CurrentStates);
  }

  @Override public long getMessagesReadCounter() {
    return _readCounterMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesWriteCounter() {
    return _writeCounterMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesReadBytesCounter() {
    return _readBytesCounterMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesWriteBytesCounter() {
    return _writeBytesCounterMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesTotalReadLatencyCounter() {
    return _readTotalLatencyMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesTotalWriteLatencyCounter() {
    return _writeTotalLatencyMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesMaxSingleReadLatencyGauge() {
    return _readMaxLatencyMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesMaxSingleWriteLatencyGauge() {
    return _writeMaxLatencyMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesReadFailureCounter() {
    return _readFailureCounterMap.get(PredefinedPath.Messages);
  }

  @Override public long getMessagesWriteFailureCounter() {
    return _writeFailureCounterMap.get(PredefinedPath.Messages);
  }

}
