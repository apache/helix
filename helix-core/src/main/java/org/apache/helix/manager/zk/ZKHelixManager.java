package org.apache.helix.manager.zk;

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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixMultiClusterController;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.HelixParticipant;
import org.apache.helix.HelixRole;
import org.apache.helix.HelixService;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.MessageListener;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ScopedConfigChangeListener;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.AdministratorId;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.SpectatorId;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

public class ZKHelixManager implements HelixManager {
  private static Logger LOG = Logger.getLogger(ZKHelixManager.class);

  public static final int FLAPPING_TIME_WINDIOW = 300000; // Default to 300 sec
  public static final int MAX_DISCONNECT_THRESHOLD = 5;
  public static final String ALLOW_PARTICIPANT_AUTO_JOIN = "allowParticipantAutoJoin";

  protected final String _zkAddress;
  protected final HelixRole _role; // keep it protected for test purpose

  public ZKHelixManager(HelixRole role) {
    _role = role;
    _zkAddress = null;
  }

  public ZKHelixManager(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddress) {

    LOG.info("Create a zk-based cluster manager. zkSvr: " + zkAddress + ", clusterName: "
        + clusterName + ", instanceName: " + instanceName + ", type: " + instanceType);

    _zkAddress = zkAddress;
    ClusterId clusterId = ClusterId.from(clusterName);
    if (instanceName == null) {
      try {
        instanceName =
            InetAddress.getLocalHost().getCanonicalHostName() + "-" + instanceType.toString();
      } catch (UnknownHostException e) {
        // can ignore it
        LOG.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e);
        instanceName = "UNKNOWN";
      }
    }

    ZkHelixConnection conn = new ZkHelixConnection(zkAddress);
    conn.connect();

    switch (instanceType) {
    case PARTICIPANT: {
      _role = conn.createParticipant(clusterId, ParticipantId.from(instanceName));
      break;
    }
    case CONTROLLER: {
      _role = conn.createController(clusterId, ControllerId.from(instanceName));
      break;
    }
    case CONTROLLER_PARTICIPANT: {
      _role = conn.createMultiClusterController(clusterId, ControllerId.from(instanceName));
      break;
    }
    case ADMINISTRATOR: {
      _role = new ZkHelixRoleDefaultImpl(conn, clusterId, AdministratorId.from(instanceName));
      break;
    }
    case SPECTATOR: {
      _role = new ZkHelixRoleDefaultImpl(conn, clusterId, SpectatorId.from(instanceName));
      break;
    }
    default:
      throw new IllegalArgumentException("Unrecognized type: " + instanceType);
    }
  }

  @Override
  public void connect() throws Exception {
    HelixConnection conn = (ZkHelixConnection) _role.getConnection();

    if (!conn.isConnected()) {
      conn.connect();
    }

    HelixService service = (HelixService) _role;
    service.start();
  }

  @Override
  public boolean isConnected() {
    return ((HelixService) _role).isStarted();
  }

  @Override
  public void disconnect() {
    HelixService service = (HelixService) _role;
    HelixConnection conn = _role.getConnection();

    service.stop();

    if (conn.isConnected()) {
      conn.disconnect();
    }
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
    _role.getConnection().addIdealStateChangeListener(_role, listener, _role.getClusterId());
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
    _role.getConnection().addLiveInstanceChangeListener(_role, listener, _role.getClusterId());
  }

  @Override
  public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener)
      throws Exception {
    _role.getConnection().addInstanceConfigChangeListener(_role, listener, _role.getClusterId());
  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener listener, ConfigScopeProperty scope)
      throws Exception {
    _role.getConnection().addConfigChangeListener(_role, listener, _role.getClusterId(), scope);
  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName) throws Exception {
    _role.getConnection().addMessageListener(_role, listener, _role.getClusterId(),
        ParticipantId.from(instanceName));
  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {
    _role.getConnection().addCurrentStateChangeListener(_role, listener, _role.getClusterId(),
        ParticipantId.from(instanceName), SessionId.from(sessionId));
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    _role.getConnection().addExternalViewChangeListener(_role, listener, _role.getClusterId());
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener) {
    _role.getConnection().addControllerListener(_role, listener, _role.getClusterId());
  }

  @Override
  public void addControllerMessageListener(MessageListener listener) {
    _role.getConnection().addControllerMessageListener(_role, listener, _role.getClusterId());
  }

  @Override
  public boolean removeListener(PropertyKey key, Object listener) {
    return _role.getConnection().removeListener(_role, listener, key);
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor() {
    return _role.getAccessor();
  }

  @Override
  public ConfigAccessor getConfigAccessor() {
    return _role.getConnection().getConfigAccessor();
  }

  @Override
  public String getClusterName() {
    return _role.getClusterId().stringify();
  }

  @Override
  public String getInstanceName() {
    return _role.getId().stringify();
  }

  @Override
  public String getSessionId() {
    return _role.getConnection().getSessionId().stringify();
  }

  @Override
  public long getLastNotificationTime() {
    // TODO implement this
    return 0;
  }

  @Override
  public HelixAdmin getClusterManagmentTool() {
    return _role.getConnection().createClusterManagementTool();
  }

  @Override
  public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    return (ZkHelixPropertyStore<ZNRecord>) _role.getConnection().createPropertyStore(
        _role.getClusterId());
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    return _role.getMessagingService();
  }

  @Override
  public InstanceType getInstanceType() {
    return _role.getType();
  }

  @Override
  public String getVersion() {
    return _role.getConnection().getHelixVersion();
  }

  @Override
  public HelixManagerProperties getProperties() {
    return _role.getConnection().getHelixProperties();
  }

  @Override
  public StateMachineEngine getStateMachineEngine() {
    StateMachineEngine engine = null;
    switch (_role.getType()) {
    case PARTICIPANT: {
      HelixParticipant participant = (HelixParticipant) _role;
      engine = participant.getStateMachineEngine();
      break;
    }
    case CONTROLLER_PARTICIPANT: {
      HelixMultiClusterController multiClusterController = (HelixMultiClusterController) _role;
      engine = multiClusterController.getStateMachineEngine();
      break;
    }
    default:
      LOG.info("helix manager type: " + _role.getType() + " does NOT have state-machine-engine");
    }

    return engine;
  }

  @Override
  public boolean isLeader() {
    boolean isLeader = false;
    switch (_role.getType()) {
    case CONTROLLER: {
      HelixController controller = (HelixController) _role;
      isLeader = controller.isLeader();
      break;
    }
    case CONTROLLER_PARTICIPANT: {
      HelixMultiClusterController multiClusterController = (HelixMultiClusterController) _role;
      isLeader = multiClusterController.isLeader();
      break;
    }
    default:
      LOG.info("helix manager type: " + _role.getType() + " does NOT support leadership");
    }
    return isLeader;
  }

  @Override
  public void startTimerTasks() {
    switch (getInstanceType()) {
    case CONTROLLER: {
      ((ZkHelixController) _role).startTimerTasks();
      break;
    }
    default:
      throw new IllegalStateException("Cann't start timer tasks for type: " + getInstanceType());
    }
  }

  @Override
  public void stopTimerTasks() {
    switch (getInstanceType()) {
    case CONTROLLER: {
      ((ZkHelixController) _role).stopTimerTasks();
      break;
    }
    default:
      throw new IllegalStateException("Cann't stop timer tasks for type: " + getInstanceType());
    }
  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback) {
    switch (_role.getType()) {
    case PARTICIPANT: {
      HelixParticipant participant = (HelixParticipant) _role;
      participant.addPreConnectCallback(callback);
      break;
    }
    case CONTROLLER_PARTICIPANT: {
      HelixMultiClusterController multiClusterController = (HelixMultiClusterController) _role;
      multiClusterController.addPreConnectCallback(callback);
      break;
    }
    default:
      LOG.info("helix manager type: " + _role.getType()
          + " does NOT support add pre-connect callback");
    }
  }

  @Override
  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
    switch (_role.getType()) {
    case PARTICIPANT: {
      HelixParticipant participant = (HelixParticipant) _role;
      participant.setLiveInstanceInfoProvider(liveInstanceInfoProvider);
      break;
    }
    case CONTROLLER_PARTICIPANT: {
      HelixMultiClusterController multiClusterController = (HelixMultiClusterController) _role;
      multiClusterController.setLiveInstanceInfoProvider(liveInstanceInfoProvider);
      break;
    }
    default:
      LOG.info("helix manager type: " + _role.getType()
          + " does NOT support set additional live instance information");
    }
  }
}
