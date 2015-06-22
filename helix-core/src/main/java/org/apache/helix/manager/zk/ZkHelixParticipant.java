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
import java.util.concurrent.TimeUnit;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixParticipant;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.HelixStateMachineEngine;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.ScheduledTaskStateModelFactory;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class ZkHelixParticipant implements HelixParticipant {
  private static Logger LOG = Logger.getLogger(ZkHelixParticipant.class);

  final ZkHelixConnection _connection;
  final ClusterId _clusterId;
  final ParticipantId _participantId;
  final ZKHelixDataAccessor _accessor;
  final BaseDataAccessor<ZNRecord> _baseAccessor;
  final PropertyKey.Builder _keyBuilder;
  final ConfigAccessor _configAccessor;
  final ClusterAccessor _clusterAccessor;
  final DefaultMessagingService _messagingService;
  final List<PreConnectCallback> _preConnectCallbacks;
  final List<HelixTimerTask> _timerTasks;
  boolean _isStarted;

  /**
   * state-transition message handler factory for helix-participant
   */
  final StateMachineEngine _stateMachineEngine;

  LiveInstanceInfoProvider _liveInstanceInfoProvider;

  public ZkHelixParticipant(ZkHelixConnection connection, ClusterId clusterId,
      ParticipantId participantId) {
    _connection = connection;
    _accessor = (ZKHelixDataAccessor) connection.createDataAccessor(clusterId);
    _baseAccessor = _accessor.getBaseDataAccessor();
    _keyBuilder = _accessor.keyBuilder();
    _clusterAccessor = connection.createClusterAccessor(clusterId);
    _configAccessor = connection.getConfigAccessor();

    _clusterId = clusterId;
    _participantId = participantId;

    _messagingService = (DefaultMessagingService) connection.createMessagingService(this);
    HelixManager manager = new ZKHelixManager(this);
    _stateMachineEngine = new HelixStateMachineEngine(manager);
    _preConnectCallbacks = new ArrayList<PreConnectCallback>();
    _timerTasks = new ArrayList<HelixTimerTask>();

  }

  @Override
  public ClusterId getClusterId() {
    return _clusterId;
  }

  @Override
  public ParticipantId getParticipantId() {
    return _participantId;
  }

  @Override
  public HelixConnection getConnection() {
    return _connection;
  }

  void startTimerTasks() {
    for (HelixTimerTask task : _timerTasks) {
      task.start();
    }
  }

  void stopTimerTasks() {
    for (HelixTimerTask task : _timerTasks) {
      task.stop();
    }
  }

  void reset() {
    /**
     * stop timer tasks, reset all handlers, make sure cleanup completed for previous session,
     * disconnect if cleanup fails
     */
    stopTimerTasks();
    _connection.resetHandlers(this);

    /**
     * clear write-through cache
     */
    _accessor.getBaseDataAccessor().reset();
  }

  private void createLiveInstance() {
    String liveInstancePath = _keyBuilder.liveInstance(_participantId.stringify()).getPath();
    String sessionId = _connection.getSessionId().stringify();
    LiveInstance liveInstance = new LiveInstance(_participantId.stringify());
    liveInstance.setSessionId(sessionId);
    liveInstance.setHelixVersion(_connection.getHelixVersion());
    liveInstance.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());

    if (_liveInstanceInfoProvider != null) {
      LOG.info("Additional live instance information is provided: " + _liveInstanceInfoProvider);
      ZNRecord additionalLiveInstanceInfo =
          _liveInstanceInfoProvider.getAdditionalLiveInstanceInfo();
      if (additionalLiveInstanceInfo != null) {
        additionalLiveInstanceInfo.merge(liveInstance.getRecord());
        ZNRecord mergedLiveInstance =
            new ZNRecord(additionalLiveInstanceInfo, _participantId.stringify());
        liveInstance = new LiveInstance(mergedLiveInstance);
        LOG.info("Participant: " + _participantId + ", mergedLiveInstance: " + liveInstance);
      }
    }

    boolean retry;
    do {
      retry = false;
      boolean success =
          _baseAccessor.create(liveInstancePath, liveInstance.getRecord(), AccessOption.EPHEMERAL);
      if (!success) {
        LOG.warn("found another participant with same name: " + _participantId + " in cluster "
            + _clusterId);

        Stat stat = new Stat();
        ZNRecord record = _baseAccessor.get(liveInstancePath, stat, 0);
        if (record == null) {
          /**
           * live-instance is gone as we check it, retry create live-instance
           */
          retry = true;
        } else {
          String ephemeralOwner = Long.toHexString(stat.getEphemeralOwner());
          if (ephemeralOwner.equals(sessionId)) {
            /**
             * update sessionId field in live-instance if necessary
             */
            LiveInstance curLiveInstance = new LiveInstance(record);
            if (!curLiveInstance.getSessionId().equals(sessionId)) {
              /**
               * in last handle-new-session,
               * live-instance is created by new zkconnection with stale session-id inside
               * just update session-id field
               */
              LOG.info("overwriting session-id by ephemeralOwner: " + ephemeralOwner
                  + ", old-sessionId: " + curLiveInstance.getSessionId() + ", new-sessionId: "
                  + sessionId);

              curLiveInstance.setSessionId(sessionId);
              success =
                  _baseAccessor.set(liveInstancePath, curLiveInstance.getRecord(),
                      stat.getVersion(), AccessOption.EPHEMERAL);
              if (!success) {
                LOG.error("Someone changes sessionId as we update, should not happen");
                throw new HelixException("fail to create live-instance for " + _participantId);
              }
            }
          } else {
            /**
             * wait for a while, in case previous helix-participant exits unexpectedly
             * and its live-instance still hangs around until session timeout
             */
            try {
              TimeUnit.MILLISECONDS.sleep(_connection.getSessionTimeout() + 5000);
            } catch (InterruptedException ex) {
              LOG.warn("Sleep interrupted while waiting for previous live-instance to go away", ex);
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
      boolean success =
          _baseAccessor.create(liveInstancePath, liveInstance.getRecord(), AccessOption.EPHEMERAL);
      if (!success) {
        LOG.error("instance: " + _participantId + " already has a live-instance in cluster "
            + _clusterId);
        throw new HelixException("fail to create live-instance for " + _participantId);
      }
    }
  }

  /**
   * carry over current-states from last sessions
   * set to initial state for current session only when state doesn't exist in current session
   */
  private void carryOverPreviousCurrentState() {
    String sessionId = _connection.getSessionId().stringify();
    String participantName = _participantId.stringify();
    List<String> sessions = _accessor.getChildNames(_keyBuilder.sessions(participantName));

    for (String session : sessions) {
      if (session.equals(sessionId)) {
        continue;
      }

      List<CurrentState> lastCurStates =
          _accessor.getChildValues(_keyBuilder.currentStates(participantName, session));

      for (CurrentState lastCurState : lastCurStates) {
        LOG.info("Carrying over old session: " + session + ", resource: " + lastCurState.getId()
            + " to current session: " + sessionId);
        String stateModelDefRef = lastCurState.getStateModelDefRef();
        if (stateModelDefRef == null) {
          LOG.error("skip carry-over because previous current state doesn't have a state model definition. previous current-state: "
              + lastCurState);
          continue;
        }
        StateModelDefinition stateModel =
            _accessor.getProperty(_keyBuilder.stateModelDef(stateModelDefRef));

        String curStatePath =
            _keyBuilder.currentState(participantName, sessionId, lastCurState.getResourceName())
                .getPath();
        _accessor.getBaseDataAccessor().update(curStatePath,
            new CurStateCarryOverUpdater(sessionId, stateModel.getInitialState(), lastCurState),
            AccessOption.PERSISTENT);
      }
    }

    /**
     * remove previous current state parent nodes
     */
    for (String session : sessions) {
      if (session.equals(sessionId)) {
        continue;
      }

      PropertyKey key = _keyBuilder.currentStates(participantName, session);
      LOG.info("Removing current states from previous sessions. path: " + key.getPath());
      _accessor.removeProperty(key);
    }
  }

  /**
   * Read cluster config and see if instance can auto join the cluster
   */
  private void joinCluster() {
    boolean autoJoin = false;
    try {
      HelixConfigScope scope =
          new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(
              _clusterId.stringify()).build();
      autoJoin =
          Boolean
              .parseBoolean(_configAccessor.get(scope, HelixManager.ALLOW_PARTICIPANT_AUTO_JOIN));
      LOG.info("instance: " + _participantId + " auto-joining " + _clusterId + " is " + autoJoin);
    } catch (Exception e) {
      // autoJoin is false
    }

    if (!ZKUtil.isInstanceSetup(_connection._zkclient, _clusterId.toString(),
        _participantId.toString(), getType())) {
      if (!autoJoin) {
        throw new HelixException("Initial cluster structure is not set up for instance: "
            + _participantId + ", instanceType: " + getType());
      } else {
        LOG.info(_participantId + " is auto-joining cluster: " + _clusterId);
        String participantName = _participantId.stringify();
        String hostName = participantName;
        int port = -1;
        int lastPos = participantName.lastIndexOf("_");
        if (lastPos > 0) {
          hostName = participantName.substring(0, lastPos);
          try {
            port = Integer.parseInt(participantName.substring(lastPos + 1));
          } catch (Exception e) {
            // use port = -1
          }
        }
        ParticipantConfig.Builder builder =
            new ParticipantConfig.Builder(_participantId).hostName(hostName).port(port)
                .enabled(true);
        _clusterAccessor.addParticipant(builder.build());
      }
    }
  }

  private void setupMsgHandler() {
    _messagingService.registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
        _stateMachineEngine);

    /**
     * it's ok to add a listener multiple times, since we check existence in
     * ZkHelixConnection#addXXXListner()
     */
    _connection.addMessageListener(this, _messagingService.getExecutor(), _clusterId,
        _participantId);

    ScheduledTaskStateModelFactory stStateModelFactory =
        new ScheduledTaskStateModelFactory(_messagingService.getExecutor());
    _stateMachineEngine.registerStateModelFactory(StateModelDefId.SchedulerTaskQueue,
        stStateModelFactory);
    _messagingService.onConnected();
  }

  void init() {
    /**
     * from here on, we are dealing with new session
     */
    if (!ZKUtil.isClusterSetup(_clusterId.toString(), _connection._zkclient)) {
      throw new HelixException("Cluster structure is not set up for cluster: " + _clusterId);
    }

    /**
     * auto-join
     */
    joinCluster();

    /**
     * Invoke PreConnectCallbacks
     */
    for (PreConnectCallback callback : _preConnectCallbacks) {
      callback.onPreConnect();
    }

    createLiveInstance();
    carryOverPreviousCurrentState();

    /**
     * setup message listener
     */
    setupMsgHandler();

    /**
     * start health check timer task
     */
    startTimerTasks();

    /**
     * init handlers
     * ok to init message handler and data-accessor twice
     * the second init will be skipped (see CallbackHandler)
     */
    _connection.initHandlers(this);
  }

  @Override
  public void onConnected() {
    reset();
    init();
    _isStarted = true;
  }

  @Override
  public void onDisconnecting() {
    LOG.info("disconnecting " + _participantId + "(" + getType() + ") from " + _clusterId);

    reset();

    /**
     * shall we shutdown thread pool first to avoid reset() being invoked in the middle of state
     * transition?
     */
    _messagingService.getExecutor().shutdown();

    /**
     * remove live instance ephemeral znode
     */
    _accessor.removeProperty(_keyBuilder.liveInstance(_participantId.stringify()));
    _isStarted = false;
  }

  @Override
  public void start() {
    _connection.addConnectionStateListener(this);
    if (_connection.isConnected()) {
      onConnected();
    }
  }

  @Override
  public void stop() {
    _connection.removeConnectionStateListener(this);
    onDisconnecting();
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    return _messagingService;
  }

  @Override
  public StateMachineEngine getStateMachineEngine() {
    return _stateMachineEngine;
  }

  @Override
  public Id getId() {
    return getParticipantId();
  }

  @Override
  public InstanceType getType() {
    return InstanceType.PARTICIPANT;
  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback) {
    LOG.info("Adding preconnect callback: " + callback);
    _preConnectCallbacks.add(callback);
  }

  @Override
  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
    _liveInstanceInfoProvider = liveInstanceInfoProvider;
  }

  @Override
  public HelixDataAccessor getAccessor() {
    return _accessor;
  }

  public ClusterAccessor getClusterAccessor() {
    return _clusterAccessor;
  }

}
