package org.apache.helix.mock;

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

import java.util.UUID;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.MockAccessor;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public class MockManager implements HelixManager {
  MockAccessor accessor;

  private final String _clusterName;
  private final String _sessionId;
  String _instanceName;
  ClusterMessagingService _msgSvc;
  private String _version;

  HelixManagerProperties _properties = new HelixManagerProperties();

  public MockManager() {
    this("testCluster-" + Math.random() * 10000 % 999);
  }

  public MockManager(String clusterName) {
    _clusterName = clusterName;
    accessor = new MockAccessor(clusterName);
    _sessionId = UUID.randomUUID().toString();
    _instanceName = "testInstanceName";
    _msgSvc = new MockClusterMessagingService();
  }

  @Override
  public void disconnect() {

  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) {
    // TODO Auto-generated method stub

  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public String getClusterName() {
    return _clusterName;
  }

  @Override
  public String getInstanceName() {
    return _instanceName;
  }

  @Override
  public void connect() {
    // TODO Auto-generated method stub

  }

  @Override
  public String getSessionId() {
    return _sessionId;
  }

  @Override
  public boolean isConnected() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long getLastNotificationTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean removeListener(PropertyKey key, Object listener) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public HelixAdmin getClusterManagmentTool() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    // TODO Auto-generated method stub
    return _msgSvc;
  }

  @Override
  public InstanceType getInstanceType() {
    return InstanceType.PARTICIPANT;
  }

  @Override
  public String getVersion() {
    return _version;
  }

  public void setVersion(String version) {
    _properties.getProperties().put("clustermanager.version", version);
    _version = version;

  }

  @Override
  public StateMachineEngine getStateMachineEngine() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isLeader() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ConfigAccessor getConfigAccessor() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void startTimerTasks() {
    // TODO Auto-generated method stub

  }

  @Override
  public void stopTimerTasks() {
    // TODO Auto-generated method stub
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor() {
    return accessor;
  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback) {
    // TODO Auto-generated method stub
  }

  @Override
  public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener)
      throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener listener,
      HelixConfigScope.ConfigScopeProperty scope) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
    // TODO Auto-generated method stub
  }

  @Override
  public HelixManagerProperties getProperties() {
    // TODO Auto-generated method stub
    return _properties;
  }

  @Override
  public void addControllerMessageListener(MessageListener listener) {
    // TODO Auto-generated method stub
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Long getSessionStartTime() {
    return 0L;
  }

}
