package org.apache.helix.controller.stages;

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

import java.util.Set;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

public class DummyClusterManager implements HelixManager {
  HelixDataAccessor _accessor;
  String _clusterName;
  String _sessionId;

  public DummyClusterManager(String clusterName, HelixDataAccessor accessor) {
    _clusterName = clusterName;
    _accessor = accessor;
    _sessionId = "session_" + clusterName;
  }

  @Override
  public void connect() throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isConnected() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void disconnect() {
    // TODO Auto-generated method stub
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addIdealStateChangeListener(org.apache.helix.IdealStateChangeListener listener) throws Exception {

  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addLiveInstanceChangeListener(org.apache.helix.LiveInstanceChangeListener listener) throws Exception {

  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addMessageListener(org.apache.helix.MessageListener listener, String instanceName) throws Exception {

  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addCurrentStateChangeListener(org.apache.helix.CurrentStateChangeListener listener, String instanceName,
      String sessionId) throws Exception {

  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addTargetExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void addExternalViewChangeListener(org.apache.helix.ExternalViewChangeListener listener) throws Exception {

  }

  @Override
  public void setEnabledControlPipelineTypes(Set<Pipeline.Type> types) {

  }

  @Override
  public boolean removeListener(PropertyKey key, Object listener) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getClusterName() {
    return _clusterName;
  }

  @Override
  public String getMetadataStoreConnectionString() {
    return null;
  }

  @Override
  public String getInstanceName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getSessionId() {
    return _sessionId;
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
  public void addControllerListener(org.apache.helix.ControllerChangeListener listener) {

  }

  @Override
  public HelixAdmin getClusterManagmentTool() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InstanceType getInstanceType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getVersion() {
    // TODO Auto-generated method stub
    return null;
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
    return _accessor;
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
  public void addInstanceConfigChangeListener(org.apache.helix.InstanceConfigChangeListener listener) throws Exception {

  }

  @Override
  public void addResourceConfigChangeListener(ResourceConfigChangeListener listener)
      throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addClusterfigChangeListener(ClusterConfigChangeListener listener)
      throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener listener, ConfigScopeProperty scope)
      throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addConfigChangeListener(org.apache.helix.ScopedConfigChangeListener listener, ConfigScopeProperty scope)
      throws Exception {

  }

  @Override
  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
    // TODO Auto-generated method stub

  }

  @Override
  public HelixManagerProperties getProperties() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addControllerMessageListener(MessageListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void addControllerMessageListener(org.apache.helix.MessageListener listener) {

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
