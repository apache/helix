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

import org.apache.helix.HelixException;

import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.manager.zk.zookeeper.ZkEventThread;

public class ZkClientMonitor implements ZkClientMonitorMBean {
  public static final String MONITOR_TYPE = "Type";
  public static final String MONITOR_KEY = "Key";

  public enum AccessType {
    READ,
    WRITE
  }

  private ObjectName _objectName;
  private String _sensorName;

  private long _stateChangeEventCounter;
  private long _dataChangeEventCounter;
  private ZkEventThread _zkEventThread;

  private Map<ZkClientPathMonitor.PredefinedPath, ZkClientPathMonitor> _zkClientPathMonitorMap =
      new ConcurrentHashMap<>();

  public ZkClientMonitor(String monitorType, String monitorKey, String monitorInstanceName,
      boolean monitorRootPathOnly) throws JMException {
    if (monitorKey == null || monitorKey.isEmpty() || monitorType == null || monitorType
        .isEmpty()) {
      throw new HelixException("Cannot create ZkClientMonitor without monitor key and type.");
    }

    _sensorName =
        String.format("%s.%s.%s", MonitorDomainNames.HelixZkClient.name(), monitorType, monitorKey);

    _objectName =
        MBeanRegistrar.register(this, getObjectName(monitorType, monitorKey, monitorInstanceName));

    for (ZkClientPathMonitor.PredefinedPath path : ZkClientPathMonitor.PredefinedPath.values()) {
      // If monitor root path only, check if the current path is Root.
      // Otherwise, add monitors for every path.
      if (!monitorRootPathOnly || path.equals(ZkClientPathMonitor.PredefinedPath.Root)) {
        _zkClientPathMonitorMap.put(path,
            new ZkClientPathMonitor(path, monitorType, monitorKey, monitorInstanceName).register());
      }
    }
  }

  public void setZkEventThread(ZkEventThread zkEventThread) {
    _zkEventThread = zkEventThread;
  }

  protected static ObjectName getObjectName(String monitorType, String monitorKey,
      String monitorInstanceName) throws MalformedObjectNameException {
    return MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), MONITOR_TYPE, monitorType,
            MONITOR_KEY,
            (monitorKey + (monitorInstanceName == null ? "" : "." + monitorInstanceName)));
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
    return _sensorName;
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

  @Override
  public long getPendingCallbackGauge() {
    if (_zkEventThread != null) {
      return _zkEventThread.getPendingEventsCount();
    }

    return -1;
  }

  @Override
  public long getTotalCallbackCounter() {
    if (_zkEventThread != null) {
      return _zkEventThread.getTotalEventCount();
    }

    return -1;
  }

  @Override
  public long getTotalCallbackHandledCounter() {
    if (_zkEventThread != null) {
      return _zkEventThread.getTotalHandledEventCount();
    }

    return -1;
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
}
