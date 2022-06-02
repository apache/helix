package org.apache.helix.cloud.event;

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

import java.util.Optional;
import java.util.Set;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixCloudProperty;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.CustomizedStateChangeListener;
import org.apache.helix.api.listeners.CustomizedStateConfigChangeListener;
import org.apache.helix.api.listeners.CustomizedStateRootChangeListener;
import org.apache.helix.api.listeners.CustomizedViewChangeListener;
import org.apache.helix.api.listeners.CustomizedViewRootChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.cloud.event.helix.HelixCloudEventListener;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

public class MockCloudEventAwareHelixManager implements HelixManager {
  private final HelixManagerProperty _helixManagerProperty;
  private CloudEventListener _cloudEventListener;

  /**
   * Use a mock zk helix manager to avoid the need to connect to zk
   * Change the cloud event related logic here every time the real logic is modified in ZKHelixManager
   */
  public MockCloudEventAwareHelixManager(HelixManagerProperty helixManagerProperty) {
    _helixManagerProperty = helixManagerProperty;
    _helixManagerProperty.getHelixCloudProperty().populateFieldsWithCloudConfig(
        new CloudConfig.Builder().setCloudEnabled(true).setCloudProvider(CloudProvider.AZURE)
            .build());
  }

  @Override
  public void connect()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    if (_helixManagerProperty != null) {
      HelixCloudProperty helixCloudProperty = _helixManagerProperty.getHelixCloudProperty();
      if (helixCloudProperty != null && helixCloudProperty.isCloudEventCallbackEnabled()) {
        _cloudEventListener =
            new HelixCloudEventListener(helixCloudProperty.getCloudEventCallbackProperty(), this);
        System.out.println("Using handler: " + helixCloudProperty.getCloudEventHandlerClassName());
        CloudEventHandlerFactory.getInstance(
            _helixManagerProperty.getHelixCloudProperty().getCloudEventHandlerClassName())
            .registerCloudEventListener(_cloudEventListener);
      }
    }
  }

  @Override
  public void disconnect() {
    if (_cloudEventListener != null) {
      try {
        CloudEventHandlerFactory.getInstance(
            _helixManagerProperty.getHelixCloudProperty().getCloudEventHandlerClassName())
            .unregisterCloudEventListener(_cloudEventListener);
      } catch (Exception e) {
        System.out.println("Failed to unregister cloudEventListener." );
        e.printStackTrace();
      }
    }
  }

  @Override
  public boolean isConnected() {
    return false;
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {

  }

  @Override
  public void addIdealStateChangeListener(org.apache.helix.IdealStateChangeListener listener)
      throws Exception {

  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {

  }

  @Override
  public void addLiveInstanceChangeListener(org.apache.helix.LiveInstanceChangeListener listener)
      throws Exception {

  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener) throws Exception {

  }

  @Override
  public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener)
      throws Exception {

  }

  @Override
  public void addInstanceConfigChangeListener(
      org.apache.helix.InstanceConfigChangeListener listener) throws Exception {

  }

  @Override
  public void addResourceConfigChangeListener(ResourceConfigChangeListener listener)
      throws Exception {

  }

  @Override
  public void addCustomizedStateConfigChangeListener(CustomizedStateConfigChangeListener listener)
      throws Exception {

  }

  @Override
  public void addClusterfigChangeListener(ClusterConfigChangeListener listener) throws Exception {

  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener listener,
      HelixConfigScope.ConfigScopeProperty scope) throws Exception {

  }

  @Override
  public void addConfigChangeListener(org.apache.helix.ScopedConfigChangeListener listener,
      HelixConfigScope.ConfigScopeProperty scope) throws Exception {

  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName) throws Exception {

  }

  @Override
  public void addMessageListener(org.apache.helix.MessageListener listener, String instanceName)
      throws Exception {

  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {

  }

  @Override
  public void addCurrentStateChangeListener(org.apache.helix.CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {

  }

  @Override
  public void addTaskCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {

  }

  @Override
  public void addCustomizedStateRootChangeListener(CustomizedStateRootChangeListener listener,
      String instanceName) throws Exception {

  }

  @Override
  public void addCustomizedStateChangeListener(CustomizedStateChangeListener listener,
      String instanceName, String stateName) throws Exception {

  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {

  }

  @Override
  public void addCustomizedViewChangeListener(CustomizedViewChangeListener listener,
      String customizedStateType) throws Exception {

  }

  @Override
  public void addCustomizedViewRootChangeListener(CustomizedViewRootChangeListener listener)
      throws Exception {

  }

  @Override
  public void addTargetExternalViewChangeListener(ExternalViewChangeListener listener)
      throws Exception {

  }

  @Override
  public void addExternalViewChangeListener(org.apache.helix.ExternalViewChangeListener listener)
      throws Exception {

  }

  @Override
  public void addControllerListener(ControllerChangeListener listener) {

  }

  @Override
  public void addControllerListener(org.apache.helix.ControllerChangeListener listener) {

  }

  @Override
  public void addControllerMessageListener(MessageListener listener) {

  }

  @Override
  public void addControllerMessageListener(org.apache.helix.MessageListener listener) {

  }

  @Override
  public void setEnabledControlPipelineTypes(Set<Pipeline.Type> types) {

  }

  @Override
  public boolean removeListener(PropertyKey key, Object listener) {
    return false;
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor() {
    return null;
  }

  @Override
  public ConfigAccessor getConfigAccessor() {
    return null;
  }

  @Override
  public String getClusterName() {
    return null;
  }

  @Override
  public String getMetadataStoreConnectionString() {
    return null;
  }

  @Override
  public String getInstanceName() {
    return null;
  }

  @Override
  public String getSessionId() {
    return null;
  }

  @Override
  public long getLastNotificationTime() {
    return 0;
  }

  @Override
  public HelixAdmin getClusterManagmentTool() {
    return null;
  }

  @Override
  public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    return null;
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    return null;
  }

  @Override
  public InstanceType getInstanceType() {
    return null;
  }

  @Override
  public String getVersion() {
    return null;
  }

  @Override
  public HelixManagerProperties getProperties() {
    return null;
  }

  @Override
  public StateMachineEngine getStateMachineEngine() {
    return null;
  }

  @Override
  public Long getSessionStartTime() {
    return null;
  }

  @Override
  public Optional<String> getSessionIdIfLead() {
    return Optional.empty();
  }

  @Override
  public boolean isLeader() {
    return false;
  }

  @Override
  public void startTimerTasks() {

  }

  @Override
  public void stopTimerTasks() {

  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback) {

  }

  @Override
  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {

  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector() {
    return null;
  }
}