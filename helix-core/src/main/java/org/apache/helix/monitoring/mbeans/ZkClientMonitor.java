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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class ZkClientMonitor implements ZkClientMonitorMBean {
  public static final String DOMAIN = "ZkClient";
  public static final String TAG = "Tag";
  public static final String DEFAULT_TAG = "default";

  private ObjectName _objectName;

  private enum PredefinedPath {
    IdealStates(".*/IDEALSTATES/.*"),
    Instances(".*/INSTANCES/.*"),
    Configs(".*/CONFIGS/.*"),
    Controller(".*/CONTROLLER/.*"),
    ExternalView(".*/EXTERNALVIEW/.*"),
    LiveInstances(".*/LIVEINSTANCES/.*"),
    PropertyStore(".*/PROPERTYSTORE/.*"),
    CurrentStates(".*/CURRENTSTATES/.*"),
    Messages(".*/MESSAGES/.*");

    private final String _matchString;

    PredefinedPath(String matchString) {
      _matchString = matchString;
    }

    public boolean match(String path) {
      return path.matches(this._matchString);
    }
  }

  private long _stateChangeEventCounter;
  private long _dataChangeEventCounter;

  private long _readCounter;
  private long _writeCounter;
  private long _readBytesCounter;
  private long _writeBytesCounter;
  private Map<PredefinedPath, Long> _readCounterMap = new HashMap<PredefinedPath, Long>();
  private Map<PredefinedPath, Long> _writeCounterMap = new HashMap<PredefinedPath, Long>();
  private Map<PredefinedPath, Long> _readBytesCounterMap = new HashMap<PredefinedPath, Long>();
  private Map<PredefinedPath, Long> _writBytesCounterMap = new HashMap<PredefinedPath, Long>();

  public ZkClientMonitor(String tag) throws JMException {
    tag = tag == null ? DEFAULT_TAG : tag;
    initCounterMaps();
    register(tag);
  }

  private void initCounterMaps() {
    for (PredefinedPath path : PredefinedPath.values()) {
      _readCounterMap.put(path, 0L);
      _writeCounterMap.put(path, 0L);
      _readBytesCounterMap.put(path, 0L);
      _writBytesCounterMap.put(path, 0L);
    }
  }

  public ZkClientMonitor() throws JMException {
    this(DEFAULT_TAG);
  }

  private void register(String tag) throws JMException {
    _objectName = MBeanRegistrar.register(this, DOMAIN, TAG, tag);
  }

  /**
   * After unregistered, the MBean can't be registered again, a new monitor has be to created.
   */
  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
  }

  @Override public String getSensorName() {
    if (_objectName.getKeyProperty(MBeanRegistrar.DUPLICATE) == null) {
      return String.format("%s.%s", DOMAIN, _objectName.getKeyProperty(TAG));
    } else {
      return String.format("%s.%s.%s", DOMAIN, _objectName.getKeyProperty(TAG),
          _objectName.getKeyProperty(MBeanRegistrar.DUPLICATE));
    }
  }

  public void increaseStateChangeEventCounter() {
    _stateChangeEventCounter++;
  }

  @Override public long getStateChangeEventCounter() {
    return _stateChangeEventCounter;
  }

  public void increaseDataChangeEventCounter() {
    _dataChangeEventCounter++;
  }

  @Override public long getDataChangeEventCounter() {
    return _dataChangeEventCounter;
  }

  private void increaseCounters(String path, int bytes, boolean isRead) {
    if (isRead) {
      _readCounter++;
      if (bytes > 0) {
        _readBytesCounter += bytes;
      }
    } else {
      _writeCounter++;
      if (bytes > 0) {
        _writeBytesCounter += bytes;
      }
    }

    for (PredefinedPath predefinedPath : PredefinedPath.values()) {
      if (predefinedPath.match(path)) {
        if (isRead) {
          _readCounterMap.put(predefinedPath, _readCounterMap.get(predefinedPath) + 1);
          if (bytes > 0) {
            _readBytesCounterMap
                .put(predefinedPath, _readBytesCounterMap.get(predefinedPath) + bytes);
          }
        } else {
          _writeCounterMap.put(predefinedPath, _writeCounterMap.get(predefinedPath) + 1);
          if (bytes > 0) {
            _writBytesCounterMap
                .put(predefinedPath, _writBytesCounterMap.get(predefinedPath) + bytes);
          }
        }
      }
    }
  }

  public void increaseReadCounters(String path) {
    increaseReadCounters(path, -1);
  }

  public void increaseReadCounters(String path, int bytes) {
    increaseCounters(path, bytes, true);
  }

  public void increaseWriteCounters(String path) {
    increaseWriteCounters(path, -1);
  }

  public void increaseWriteCounters(String path, int bytes) {
    increaseCounters(path, bytes, false);
  }

  @Override public long getReadCounter() {
    return _readCounter;
  }

  @Override public long getReadBytesCounter() {
    return _readBytesCounter;
  }

  @Override public long getWriteCounter() {
    return _writeCounter;
  }

  @Override public long getWriteBytesCounter() {
    return _writeBytesCounter;
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
    return _writBytesCounterMap.get(PredefinedPath.IdealStates);
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
    return _writBytesCounterMap.get(PredefinedPath.Instances);
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
    return _writBytesCounterMap.get(PredefinedPath.Configs);
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
    return _writBytesCounterMap.get(PredefinedPath.Controller);
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
    return _writBytesCounterMap.get(PredefinedPath.ExternalView);
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
    return _writBytesCounterMap.get(PredefinedPath.LiveInstances);
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
    return _writBytesCounterMap.get(PredefinedPath.PropertyStore);
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
    return _writBytesCounterMap.get(PredefinedPath.CurrentStates);
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
    return _writBytesCounterMap.get(PredefinedPath.Messages);
  }

}
