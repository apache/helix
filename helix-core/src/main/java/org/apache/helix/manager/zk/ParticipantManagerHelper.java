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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.ScheduledTaskStateModelFactory;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

/**
 * helper class for participant-manager
 */
public class ParticipantManagerHelper {
  private static Logger LOG = Logger.getLogger(ParticipantManagerHelper.class);

  final ZkClient _zkclient;
  final HelixManager _manager;
  final PropertyKey.Builder _keyBuilder;
  final String _clusterName;
  final String _instanceName;
  final String _sessionId;
  final int _sessionTimeout;
  final ConfigAccessor _configAccessor;
  final InstanceType _instanceType;
  final HelixAdmin _helixAdmin;
  final ZKHelixDataAccessor _dataAccessor;
  final DefaultMessagingService _messagingService;
  final StateMachineEngine _stateMachineEngine;
  final LiveInstanceInfoProvider _liveInstanceInfoProvider;

  public ParticipantManagerHelper(HelixManager manager, ZkClient zkclient, int sessionTimeout,
      LiveInstanceInfoProvider liveInstanceInfoProvider) {
    _zkclient = zkclient;
    _manager = manager;
    _clusterName = manager.getClusterName();
    _instanceName = manager.getInstanceName();
    _keyBuilder = new PropertyKey.Builder(_clusterName);
    _sessionId = manager.getSessionId();
    _sessionTimeout = sessionTimeout;
    _configAccessor = manager.getConfigAccessor();
    _instanceType = manager.getInstanceType();
    _helixAdmin = manager.getClusterManagmentTool();
    _dataAccessor = (ZKHelixDataAccessor) manager.getHelixDataAccessor();
    _messagingService = (DefaultMessagingService) manager.getMessagingService();
    _stateMachineEngine = manager.getStateMachineEngine();
    _liveInstanceInfoProvider = liveInstanceInfoProvider;
  }

  public void joinCluster() {
    // Read cluster config and see if instance can auto join the cluster
    boolean autoJoin = false;
    try {
      HelixConfigScope scope =
          new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(
              _manager.getClusterName()).build();
      autoJoin =
          Boolean.parseBoolean(_configAccessor.get(scope,
              ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN));
      LOG.info("instance: " + _instanceName + " auto-joining " + _clusterName + " is " + autoJoin);
    } catch (Exception e) {
      // autoJoin is false
    }

    if (!ZKUtil.isInstanceSetup(_zkclient, _clusterName, _instanceName, _instanceType)) {
      if (!autoJoin) {
        throw new HelixException("Initial cluster structure is not set up for instance: "
            + _instanceName + ", instanceType: " + _instanceType);
      } else {
        LOG.info(_instanceName + " is auto-joining cluster: " + _clusterName);
        InstanceConfig instanceConfig = new InstanceConfig(_instanceName);
        String hostName = _instanceName;
        String port = "";
        int lastPos = _instanceName.lastIndexOf("_");
        if (lastPos > 0) {
          hostName = _instanceName.substring(0, lastPos);
          port = _instanceName.substring(lastPos + 1);
        }
        instanceConfig.setHostName(hostName);
        instanceConfig.setPort(port);
        instanceConfig.setInstanceEnabled(true);
        _helixAdmin.addInstance(_clusterName, instanceConfig);
      }
    }
  }

  public void createLiveInstance() {
    String liveInstancePath = _keyBuilder.liveInstance(_instanceName).getPath();
    LiveInstance liveInstance = new LiveInstance(_instanceName);
    liveInstance.setSessionId(_sessionId);
    liveInstance.setHelixVersion(_manager.getVersion());
    liveInstance.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());

    // LiveInstanceInfoProvider liveInstanceInfoProvider = _manager._liveInstanceInfoProvider;
    if (_liveInstanceInfoProvider != null) {
      LOG.info("invoke liveInstanceInfoProvider");
      ZNRecord additionalLiveInstanceInfo =
          _liveInstanceInfoProvider.getAdditionalLiveInstanceInfo();
      if (additionalLiveInstanceInfo != null) {
        additionalLiveInstanceInfo.merge(liveInstance.getRecord());
        ZNRecord mergedLiveInstance = new ZNRecord(additionalLiveInstanceInfo, _instanceName);
        liveInstance = new LiveInstance(mergedLiveInstance);
        LOG.info("instanceName: " + _instanceName + ", mergedLiveInstance: " + liveInstance);
      }
    }

    boolean retry;
    do {
      retry = false;
      try {
        _zkclient.createEphemeral(liveInstancePath, liveInstance.getRecord());
      } catch (ZkNodeExistsException e) {
        LOG.warn("found another instance with same instanceName: " + _instanceName + " in cluster "
            + _clusterName);

        Stat stat = new Stat();
        ZNRecord record = _zkclient.readData(liveInstancePath, stat, true);
        if (record == null) {
          /**
           * live-instance is gone as we check it, retry create live-instance
           */
          retry = true;
        } else {
          String ephemeralOwner = Long.toHexString(stat.getEphemeralOwner());
          if (ephemeralOwner.equals(_sessionId)) {
            /**
             * update sessionId field in live-instance if necessary
             */
            LiveInstance curLiveInstance = new LiveInstance(record);
            if (!curLiveInstance.getSessionId().equals(_sessionId)) {
              /**
               * in last handle-new-session,
               * live-instance is created by new zkconnection with stale session-id inside
               * just update session-id field
               */
              LOG.info("overwriting session-id by ephemeralOwner: " + ephemeralOwner
                  + ", old-sessionId: " + curLiveInstance.getSessionId() + ", new-sessionId: "
                  + _sessionId);

              curLiveInstance.setSessionId(_sessionId);
              _zkclient.writeData(liveInstancePath, curLiveInstance.getRecord());
            }
          } else {
            /**
             * wait for a while, in case previous helix-participant exits unexpectedly
             * and its live-instance still hangs around until session timeout
             */
            try {
              TimeUnit.MILLISECONDS.sleep(_sessionTimeout + 5000);
            } catch (InterruptedException ex) {
              LOG.warn("Sleep interrupted while waiting for previous live-instance to go away.", ex);
            }
            /**
             * give a last try after exit while loop
             */
            retry = true;
            break;
          }
        }
      }
    } while (retry);

    /**
     * give a last shot
     */
    if (retry) {
      try {
        _zkclient.createEphemeral(liveInstancePath, liveInstance.getRecord());
      } catch (Exception e) {
        String errorMessage =
            "instance: " + _instanceName + " already has a live-instance in cluster "
                + _clusterName;
        LOG.error(errorMessage);
        throw new HelixException(errorMessage);
      }
    }
  }

  /**
   * carry over current-states from last sessions
   * set to initial state for current session only when state doesn't exist in current session
   */
  public void carryOverPreviousCurrentState() {
    List<String> sessions = _dataAccessor.getChildNames(_keyBuilder.sessions(_instanceName));

    for (String session : sessions) {
      if (session.equals(_sessionId)) {
        continue;
      }

      List<CurrentState> lastCurStates =
          _dataAccessor.getChildValues(_keyBuilder.currentStates(_instanceName, session));

      for (CurrentState lastCurState : lastCurStates) {
        LOG.info("Carrying over old session: " + session + ", resource: " + lastCurState.getId()
            + " to current session: " + _sessionId);
        String stateModelDefRef = lastCurState.getStateModelDefRef();
        if (stateModelDefRef == null) {
          LOG.error("skip carry-over because previous current state doesn't have a state model definition. previous current-state: "
              + lastCurState);
          continue;
        }
        StateModelDefinition stateModel =
            _dataAccessor.getProperty(_keyBuilder.stateModelDef(stateModelDefRef));

        String curStatePath =
            _keyBuilder.currentState(_instanceName, _sessionId, lastCurState.getResourceName())
                .getPath();
        _dataAccessor.getBaseDataAccessor().update(curStatePath,
            new CurStateCarryOverUpdater(_sessionId, stateModel.getInitialState(), lastCurState),
            AccessOption.PERSISTENT);
      }
    }

    /**
     * remove previous current state parent nodes
     */
    for (String session : sessions) {
      if (session.equals(_sessionId)) {
        continue;
      }

      String path = _keyBuilder.currentStates(_instanceName, session).getPath();
      LOG.info("Removing current states from previous sessions. path: " + path);
      _zkclient.deleteRecursive(path);
    }
  }

  public void setupMsgHandler() throws Exception {
    _messagingService.registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
        _stateMachineEngine);
    _manager.addMessageListener(_messagingService.getExecutor(), _instanceName);

    ScheduledTaskStateModelFactory stStateModelFactory =
        new ScheduledTaskStateModelFactory(_messagingService.getExecutor());
    _stateMachineEngine.registerStateModelFactory(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, stStateModelFactory);
    _messagingService.onConnected();

  }

}
