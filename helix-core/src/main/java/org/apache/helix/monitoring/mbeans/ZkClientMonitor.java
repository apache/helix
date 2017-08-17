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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ZkClientMonitor implements ZkClientMonitorMBean {
  public static final String MONITOR_TYPE = "Type";
  public static final String MONITOR_KEY = "Key";

  public static final String SESSION_ID_PROPERTY_NAME = "SessionId";
  public static final String CUSTOMIZED_PROPERTY_NAME = "CustomizedKey";

  private ObjectName _objectName;
  private String _monitorType;
  private String _monitorKey;

  private long _stateChangeEventCounter;
  private long _dataChangeEventCounter;

  private Map<ZkClientPathMonitor.PredefinedPath, ZkClientPathMonitor> _zkClientPathMonitorMap =
      new ConcurrentHashMap<>();

  public ZkClientMonitor(String monitorType, String monitorKey) throws JMException {
    _monitorType = monitorType;
    _monitorKey = monitorKey;
    regitster(monitorType, monitorKey);

    for (ZkClientPathMonitor.PredefinedPath path : ZkClientPathMonitor.PredefinedPath.values()) {
      _zkClientPathMonitorMap
          .put(path, new ZkClientPathMonitor(path.name(), monitorType, monitorKey));
    }
  }

  private void regitster(String monitorType, String monitorKey) throws JMException {
    _objectName = MBeanRegistrar
        .register(this, MonitorDomainNames.HelixZkClient.name(), MONITOR_TYPE, monitorType,
            MONITOR_KEY, monitorKey);
  }

  /**
   * After unregistered, the MBean can't be registered again, a new monitor has be to created.
   */
  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
    for (ZkClientPathMonitor zkClientPathMonitor : _zkClientPathMonitorMap.values()) {
      zkClientPathMonitor.unregister();
    }
  }

  @Override
  public String getSensorName() {
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
    for (ZkClientPathMonitor.PredefinedPath predefinedPath : ZkClientPathMonitor.PredefinedPath
        .values()) {
      if (predefinedPath.match(path)) {
        ZkClientPathMonitor zkClientPathMonitor = _zkClientPathMonitorMap.get(predefinedPath);
        if (zkClientPathMonitor != null) {
          zkClientPathMonitor.record(bytes, latencyMilliSec, isFailure, isRead);
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
}
