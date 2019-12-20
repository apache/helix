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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordBucketizer;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.ScheduledTaskStateModelFactory;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle all session related work for a participant.
 */
public class ParticipantManager {
  private static Logger LOG = LoggerFactory.getLogger(ParticipantManager.class);

  final HelixZkClient _zkclient;
  final HelixManager _manager;
  final PropertyKey.Builder _keyBuilder;
  final String _clusterName;
  final String _instanceName;
  final int _sessionTimeout;
  final ConfigAccessor _configAccessor;
  final InstanceType _instanceType;
  final HelixAdmin _helixAdmin;
  final ZKHelixDataAccessor _dataAccessor;
  final DefaultMessagingService _messagingService;
  final StateMachineEngine _stateMachineEngine;
  final LiveInstanceInfoProvider _liveInstanceInfoProvider;
  final List<PreConnectCallback> _preConnectCallbacks;

  private final String _sessionId;

  public ParticipantManager(HelixManager manager, HelixZkClient zkclient, int sessionTimeout,
      LiveInstanceInfoProvider liveInstanceInfoProvider, List<PreConnectCallback> preConnectCallbacks,
      final String sessionId) {
    _zkclient = zkclient;
    _manager = manager;
    _clusterName = manager.getClusterName();
    _instanceName = manager.getInstanceName();
    _keyBuilder = new PropertyKey.Builder(_clusterName);
    _sessionId = sessionId;
    _sessionTimeout = sessionTimeout;
    _configAccessor = manager.getConfigAccessor();
    _instanceType = manager.getInstanceType();
    _helixAdmin = manager.getClusterManagmentTool();
    _dataAccessor = (ZKHelixDataAccessor) manager.getHelixDataAccessor();
    _messagingService = (DefaultMessagingService) manager.getMessagingService();
    _stateMachineEngine = manager.getStateMachineEngine();
    _liveInstanceInfoProvider = liveInstanceInfoProvider;
    _preConnectCallbacks = preConnectCallbacks;
  }

  /**
   * Handles a new session for a participant. The new session's id is passed in when participant
   * manager is created, as it is required in
   * {@link org.apache.helix.manager.zk.zookeeper.ZkClient#createEphemeral(String, Object, String)}
   * to prevent ephemeral node creation from session race condition: ephemeral node is created by an
   * expired or unexpected session.
   *
   * @throws Exception if any exception occurs
   */
  public void handleNewSession() throws Exception {
    // Check participant's session is still valid.
    // If not, stop to handle new session for this participant.
    final String zkClientHexSession = _zkclient.getHexSessionId();
    if (!zkClientHexSession.equals(_sessionId)) {
      throw new HelixException(
          "Failed to handle new session for participant. There is a session mismatch: "
              + "participant manager session = " + _sessionId + ", zk client session = "
              + zkClientHexSession);
    }

    joinCluster();

    /**
     * Invoke PreConnectCallbacks
     */
    for (PreConnectCallback callback : _preConnectCallbacks) {
      callback.onPreConnect();
    }

    // TODO create live instance node after all the init works done --JJ
    // This will help to prevent controller from sending any message prematurely.
    createLiveInstance();
    carryOverPreviousCurrentState();

    /**
     * setup message listener
     */
    setupMsgHandler();
  }

  private void joinCluster() {
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

  private void createLiveInstance() {
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
        _zkclient.createEphemeral(liveInstancePath, liveInstance.getRecord(), _sessionId);
        LOG.info("LiveInstance created, path: " + liveInstancePath + ", sessionId: " + liveInstance.getEphemeralOwner());
      } catch (ZkSessionMismatchedException e) {
        throw new HelixException(
            "Failed to create live instance, path: " + liveInstancePath + ", expected session: "
                + _sessionId, e);
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
          String ephemeralOwner = ZKUtil.toHexSessionId(stat.getEphemeralOwner());
          if (ephemeralOwner.equals(_sessionId)) {
            /**
             * update sessionId field in live-instance if necessary
             */
            LiveInstance curLiveInstance = new LiveInstance(record);
            if (!curLiveInstance.getEphemeralOwner().equals(_sessionId)) {
              /**
               * in last handle-new-session,
               * live-instance is created by new zkconnection with stale session-id inside
               * just update session-id field
               */
              LOG.info("overwriting session-id by ephemeralOwner: " + ephemeralOwner
                  + ", old-sessionId: " + curLiveInstance.getEphemeralOwner() + ", new-sessionId: "
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
        _zkclient.createEphemeral(liveInstancePath, liveInstance.getRecord(), _sessionId);
        LOG.info("LiveInstance created, path: " + liveInstancePath + ", sessionId: " + liveInstance
            .getEphemeralOwner());
      } catch (ZkSessionMismatchedException e) {
        throw new HelixException(
            "Failed to create live instance, path: " + liveInstancePath + ", session: "
                + _sessionId, e);
      } catch (ZkNodeExistsException e) {
        throw new HelixException(
            "Instance name: " + _instanceName + " already has a live-instance in cluster: "
                + _clusterName);
      } catch (Exception e) {
        throw new HelixException(e);
      }
    }

    ParticipantHistory history = getHistory();
    history.reportOnline(_sessionId, _manager.getVersion());
    persistHistory(history);

    if (!liveInstance.getEphemeralOwner().equals(liveInstance.getSessionId())) {
      LOG.warn(
          "Session ID {} (Deprecated) in the znode does not match the Ephemeral Owner session ID {}. Will use the Ephemeral Owner session ID.",
          liveInstance.getSessionId(), liveInstance.getEphemeralOwner());
    }
  }

  /**
   * carry over current-states from last sessions
   * set to initial state for current session only when state doesn't exist in current session
   */
  private void carryOverPreviousCurrentState() {
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

        BaseDataAccessor<ZNRecord> baseAccessor = _dataAccessor.getBaseDataAccessor();
        String curStatePath =
            _keyBuilder.currentState(_instanceName, _sessionId, lastCurState.getResourceName())
                .getPath();

        String initState = stateModel.getInitialState();
        if (lastCurState.getBucketSize() > 0) {
          // update parent node
          ZNRecord metaRecord = new ZNRecord(lastCurState.getId());
          metaRecord.setSimpleFields(lastCurState.getRecord().getSimpleFields());
          DataUpdater<ZNRecord> metaRecordUpdater =
              new CurStateCarryOverUpdater(_sessionId, initState, new CurrentState(metaRecord));
          boolean success =
              baseAccessor.update(curStatePath, metaRecordUpdater, AccessOption.PERSISTENT);
          if (success) {
            // update current state buckets
            ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(lastCurState.getBucketSize());

            Map<String, ZNRecord> map = bucketizer.bucketize(lastCurState.getRecord());
            List<String> paths = new ArrayList<String>();
            List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
            for (String bucketName : map.keySet()) {
              paths.add(curStatePath + "/" + bucketName);
              updaters.add(new CurStateCarryOverUpdater(_sessionId, initState, new CurrentState(map
                  .get(bucketName))));
            }

            baseAccessor.updateChildren(paths, updaters, AccessOption.PERSISTENT);
          }

        } else {
          _dataAccessor.getBaseDataAccessor().update(curStatePath,
              new CurStateCarryOverUpdater(_sessionId, initState, lastCurState),
              AccessOption.PERSISTENT);
        }
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
      _zkclient.deleteRecursively(path);
    }
  }

  private void setupMsgHandler() throws Exception {
    _messagingService.registerMessageHandlerFactory(_stateMachineEngine.getMessageTypes(),
        _stateMachineEngine);
    _manager.addMessageListener(_messagingService.getExecutor(), _instanceName);

    ScheduledTaskStateModelFactory stStateModelFactory = new ScheduledTaskStateModelFactory(_messagingService.getExecutor());
    _stateMachineEngine.registerStateModelFactory(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, stStateModelFactory);
    _messagingService.onConnected();
  }

  private ParticipantHistory getHistory() {
    PropertyKey propertyKey = _keyBuilder.participantHistory(_instanceName);
    ParticipantHistory history = _dataAccessor.getProperty(propertyKey);
    if (history == null) {
      history = new ParticipantHistory(_instanceName);
    }
    return history;
  }

  private void persistHistory(ParticipantHistory history) {
    PropertyKey propertyKey = _keyBuilder.participantHistory(_instanceName);
    if (!_dataAccessor.setProperty(propertyKey, history)) {
      LOG.error("Failed to persist participant history to zk!");
    }
  }

  public void reset() {
  }

  public void disconnect() {
    try {
      ParticipantHistory history = getHistory();
      history.reportOffline();
      persistHistory(history);
    } catch (Exception e) {
      LOG.error("Failed to report participant offline.", e);
    }
    reset();
  }
}
