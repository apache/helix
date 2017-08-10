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

import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.InstanceType;

public class HelixCallbackMonitor implements HelixCallbackMonitorMBean {
  public static final String MONITOR_TYPE = "Type";
  public static final String MONITOR_KEY = "Key";

  private ObjectName _objectName;
  private InstanceType _instanceType;
  private String _key;

  private long _callbackCounter;
  private long _callbackUnbatchedCounter;
  private long _callbackLatencyCounter;
  private Map<ChangeType, Long> _callbackCounterMap = new ConcurrentHashMap<>();
  private Map<ChangeType, Long> _callbackUnbatchedCounterMap = new ConcurrentHashMap<>();
  private Map<ChangeType, Long> _callbackLatencyCounterMap = new ConcurrentHashMap<>();

  public HelixCallbackMonitor(InstanceType type, String key) throws JMException {
    for (ChangeType changeType : ChangeType.values()) {
      _callbackCounterMap.put(changeType, 0L);
      _callbackUnbatchedCounterMap.put(changeType, 0L);
      _callbackLatencyCounterMap.put(changeType, 0L);
    }
    _instanceType = type;
    _key = key;
    register(type, key);
  }

  private void register(InstanceType type, String key) throws JMException {
    _objectName = MBeanRegistrar
        .register(this, MonitorDomainNames.HelixCallback.name(), MONITOR_TYPE, type.name(),
            MONITOR_KEY, key);
  }

  /**
   * After unregistered, the MBean can't be registered again, a new monitor has be to created.
   */
  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
  }

  public void increaseCallbackUnbatchedCounters(ChangeType type) {
    _callbackUnbatchedCounter++;
    _callbackUnbatchedCounterMap.put(type, _callbackUnbatchedCounterMap.get(type) + 1);
  }

  @Override public String getSensorName() {
    return String.format("%s.%s.%s", MonitorDomainNames.HelixCallback.name(), _instanceType.name(),
        _key);
  }

  public void increaseCallbackCounters(ChangeType type, long time) {
    _callbackCounter++;
    _callbackLatencyCounter += time;
    _callbackCounterMap.put(type, _callbackCounterMap.get(type) + 1);
    _callbackLatencyCounterMap.put(type, _callbackLatencyCounterMap.get(type) + time);
  }

  @Override
  public long getCallbackCounter() {
    return _callbackCounter;
  }

  @Override
  public long getCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounter;
  }

  @Override
  public long getCallbackLatencyCounter() {
    return _callbackLatencyCounter;
  }

  @Override
  public long getIdealStateCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.IDEAL_STATE);
  }
  @Override
  public long getIdealStateCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.IDEAL_STATE);
  }

  @Override
  public long getIdealStateCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.IDEAL_STATE);
  }

  @Override
  public long getInstanceConfigCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.INSTANCE_CONFIG);
  }

  @Override
  public long getInstanceConfigCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.INSTANCE_CONFIG);
  }

  @Override
  public long getInstanceConfigCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.INSTANCE_CONFIG);
  }

  @Override
  public long getConfigCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.CONFIG);
  }

  @Override
  public long getConfigCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.CONFIG);
  }

  @Override
  public long getConfigCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.CONFIG);
  }

  @Override
  public long getLiveInstanceCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.LIVE_INSTANCE);
  }

  @Override
  public long getLiveInstanceCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.LIVE_INSTANCE);
  }

  @Override
  public long getLiveInstanceCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.LIVE_INSTANCE);
  }

  @Override
  public long getCurrentStateCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.CURRENT_STATE);
  }

  @Override
  public long getCurrentStateCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.CURRENT_STATE);
  }

  @Override
  public long getCurrentStateCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.CURRENT_STATE);
  }

  @Override
  public long getMessageCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.MESSAGE);
  }

  @Override public long getMessageCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.MESSAGE);
  }

  @Override
  public long getMessageCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.MESSAGE);
  }

  @Override
  public long getMessagesControllerCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.MESSAGES_CONTROLLER);
  }

  @Override
  public long getMessagesControllerCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.MESSAGES_CONTROLLER);
  }

  @Override
  public long getMessagesControllerCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.MESSAGES_CONTROLLER);
  }

  @Override
  public long getExternalViewCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.EXTERNAL_VIEW);
  }

  @Override
  public long getExternalViewCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.EXTERNAL_VIEW);
  }

  @Override
  public long getExternalViewCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.EXTERNAL_VIEW);
  }

  @Override
  public long getControllerCallbackCounter() {
    return _callbackCounterMap.get(ChangeType.CONTROLLER);
  }

  @Override
  public long getControllerCallbackLatencyCounter() {
    return _callbackLatencyCounterMap.get(ChangeType.CONTROLLER);
  }

  @Override
  public long getControllerCallbackUnbatchedCounter() {
    return _callbackUnbatchedCounterMap.get(ChangeType.CONTROLLER);
  }
}
