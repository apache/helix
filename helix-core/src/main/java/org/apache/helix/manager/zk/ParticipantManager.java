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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixCloudProperty;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.cloud.CloudInstanceInformation;
import org.apache.helix.api.cloud.CloudInstanceInformationProcessor;
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
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.ZNRecordBucketizer;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.zookeeper.zkclient.exception.ZkSessionMismatchedException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle all session related work for a participant.
 */
public class ParticipantManager {
  private static Logger LOG = LoggerFactory.getLogger(ParticipantManager.class);
  private static final String CLOUD_PROCESSOR_PATH_PREFIX = "org.apache.helix.cloud.";

  final RealmAwareZkClient _zkclient;
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
  final HelixManagerProperty _helixManagerProperty;

  // zk session id should be immutable after participant manager is created. This is to avoid
  // session race condition when handling new session for the participant.
  private final String _sessionId;

  @Deprecated
  public ParticipantManager(HelixManager manager, RealmAwareZkClient zkclient, int sessionTimeout,
      LiveInstanceInfoProvider liveInstanceInfoProvider, List<PreConnectCallback> preConnectCallbacks,
      final String sessionId) {
    this(manager, zkclient, sessionTimeout, liveInstanceInfoProvider, preConnectCallbacks,
        sessionId, null);
  }

  public ParticipantManager(HelixManager manager, RealmAwareZkClient zkclient, int sessionTimeout,
      LiveInstanceInfoProvider liveInstanceInfoProvider,
      List<PreConnectCallback> preConnectCallbacks, final String sessionId,
      HelixManagerProperty helixManagerProperty) {
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
    _helixManagerProperty = helixManagerProperty;
  }

  /**
   * Handles a new session for a participant. The new session's id is passed in when participant
   * manager is created, as it is required to prevent ephemeral node creation from session race
   * condition: ephemeral node is created by an expired or unexpected session.
   * @throws Exception
   */
  public void handleNewSession() throws Exception {
    // Check zk session of this participant is still valid.
    // If not, skip handling new session for this participant.
    final String zkClientHexSession = ZKUtil.toHexSessionId(_zkclient.getSessionId());
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
    // Live instance creation also checks if the expected session is valid or not. Live instance
    // should not be created by an expired zk session.
    createLiveInstance();
    if (shouldCarryOver()) {
      carryOverPreviousCurrentState(_dataAccessor, _instanceName, _sessionId,
          _manager.getStateMachineEngine(), true);
    }
    removePreviousTaskCurrentStates();

    /**
     * setup message listener
     */
    setupMsgHandler();
  }

  private boolean shouldCarryOver() {
    if (_liveInstanceInfoProvider == null
        || _liveInstanceInfoProvider.getAdditionalLiveInstanceInfo() == null) {
      return true;
    }
    String status = _liveInstanceInfoProvider.getAdditionalLiveInstanceInfo()
        .getSimpleField(LiveInstance.LiveInstanceProperty.STATUS.name());
    // If frozen, no carry-over
    return !LiveInstance.LiveInstanceStatus.FROZEN.name().equals(status);
  }

  private void joinCluster() {
    // Read cluster config and see if an instance can auto join or auto register to the cluster
    boolean autoJoin = false;
    boolean autoRegistration = false;

    // Read "allowParticipantAutoJoin" field to see if an instance can auto join to the cluster
    try {
      HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER)
          .forCluster(_manager.getClusterName()).build();
      autoJoin = Boolean
          .parseBoolean(_configAccessor.get(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN));
      LOG.info("instance: " + _instanceName + " auto-joining " + _clusterName + " is " + autoJoin);
    } catch (Exception e) {
      LOG.info("auto join is false for cluster" + _clusterName);
    }

    // Read cloud config and see if an instance can auto register to the cluster
    // Difference between auto join and auto registration is that the latter will also populate the
    // domain information in instance config
    try {
      autoRegistration =
          Boolean.valueOf(_helixManagerProperty.getHelixCloudProperty().getCloudEnabled());
      LOG.info("instance: " + _instanceName + " auto-registering " + _clusterName + " is "
          + autoRegistration);
    } catch (Exception e) {
      LOG.info("auto registration is false for cluster" + _clusterName);
    }

    InstanceConfig instanceConfig;
    if (!ZKUtil.isInstanceSetup(_zkclient, _clusterName, _instanceName, _instanceType)) {
      if (!autoJoin) {
        throw new HelixException("Initial cluster structure is not set up for instance: "
            + _instanceName + ", instanceType: " + _instanceType);
      }
      if (!autoRegistration) {
        LOG.info(_instanceName + " is auto-joining cluster: " + _clusterName);
        instanceConfig = HelixUtil.composeInstanceConfig(_instanceName);
      } else {
        LOG.info(_instanceName + " is auto-registering cluster: " + _clusterName);
        CloudInstanceInformation cloudInstanceInformation = getCloudInstanceInformation();
        String domain = cloudInstanceInformation
            .get(CloudInstanceInformation.CloudInstanceField.FAULT_DOMAIN.name()) + _instanceName;
        instanceConfig = HelixUtil.composeInstanceConfig(_instanceName);
        instanceConfig.setDomain(domain);
      }
      instanceConfig
          .validateTopologySettingInInstanceConfig(_configAccessor.getClusterConfig(_clusterName),
              _instanceName);
      _helixAdmin.addInstance(_clusterName, instanceConfig);
    } else {
      _configAccessor.getInstanceConfig(_clusterName, _instanceName)
          .validateTopologySettingInInstanceConfig(_configAccessor.getClusterConfig(_clusterName),
              _instanceName);
    }
  }

  private CloudInstanceInformation getCloudInstanceInformation() {
    String cloudInstanceInformationProcessorName =
        _helixManagerProperty.getHelixCloudProperty().getCloudInfoProcessorName();
    try {
      // fetch cloud instance information for the instance
      String cloudInstanceInformationProcessorClassName = CLOUD_PROCESSOR_PATH_PREFIX
          + _helixManagerProperty.getHelixCloudProperty().getCloudProvider().toLowerCase() + "."
          + cloudInstanceInformationProcessorName;
      Class processorClass = Class.forName(cloudInstanceInformationProcessorClassName);
      Constructor constructor = processorClass.getConstructor(HelixCloudProperty.class);
      CloudInstanceInformationProcessor processor = (CloudInstanceInformationProcessor) constructor
          .newInstance(_helixManagerProperty.getHelixCloudProperty());
      List<String> responses = processor.fetchCloudInstanceInformation();

      // parse cloud instance information for the participant
      CloudInstanceInformation cloudInstanceInformation =
          processor.parseCloudInstanceInformation(responses);
      return cloudInstanceInformation;
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
        | IllegalAccessException | InvocationTargetException ex) {
      throw new HelixException(
          "Failed to create a new instance for the class: " + cloudInstanceInformationProcessorName,
          ex);
    }
  }

  private void createLiveInstance() {
    String liveInstancePath = _keyBuilder.liveInstance(_instanceName).getPath();
    LiveInstance liveInstance = new LiveInstance(_instanceName);
    liveInstance.setSessionId(_sessionId);
    liveInstance.setHelixVersion(_manager.getVersion());
    liveInstance.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
    liveInstance.setCurrentTaskThreadPoolSize(
        TaskUtil.getTargetThreadPoolSize(_zkclient, _clusterName, _instanceName));

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
        // Zk session ID will be validated in createEphemeral.
        _zkclient.createEphemeral(liveInstancePath, liveInstance.getRecord(), _sessionId);
        LOG.info("LiveInstance created, path: {}, sessionId: {}", liveInstancePath,
            liveInstance.getEphemeralOwner());
      } catch (ZkSessionMismatchedException e) {
        throw new HelixException(
            "Failed to create live instance, path: " + liveInstancePath + ". Caused by: "
                + e.getMessage());
      } catch (ZkNodeExistsException e) {
        LOG.warn("Found another instance with same instance name: {} in cluster: {}", _instanceName,
            _clusterName);

        Stat stat = new Stat();
        ZNRecord record = _zkclient.readData(liveInstancePath, stat, true);
        if (record == null) {
          /**
           * live-instance is gone as we check it, retry create live-instance
           */
          retry = true;
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
    } while (retry);

    /**
     * give a last shot
     */
    if (retry) {
      try {
        // Zk session ID will be validated in createEphemeral.
        _zkclient.createEphemeral(liveInstancePath, liveInstance.getRecord(), _sessionId);
        LOG.info("LiveInstance created, path: {}, sessionId: {}", liveInstancePath,
            liveInstance.getEphemeralOwner());
      } catch (ZkSessionMismatchedException e) {
        throw new HelixException(
            "Failed to create live instance, path: " + liveInstancePath + ". Caused by: "
                + e.getMessage());
      } catch (ZkNodeExistsException e) {
        throw new HelixException("Failed to create live instance because instance: " + _instanceName
            + " already has a live-instance in cluster: " + _clusterName + ". Path is: "
            + liveInstancePath);
      } catch (Exception e) {
        throw new HelixException("Failed to create live instance. " + e.getMessage());
      }
    }

    ParticipantHistory history = getHistory();
    history.reportOnline(_sessionId, _manager.getVersion());
    persistHistory(history, false);
  }

  /**
   * carry over current-states from last sessions
   * set to initial state for current session only when state doesn't exist in current session
   */
  public static synchronized void carryOverPreviousCurrentState(HelixDataAccessor dataAccessor,
      String instanceName, String sessionId, StateMachineEngine stateMachineEngine,
      boolean setToInitState) {
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    List<String> sessions = dataAccessor.getChildNames(keyBuilder.sessions(instanceName));

    for (String session : sessions) {
      if (session.equals(sessionId)) {
        continue;
      }

      // Ignore if any current states in the previous folder cannot be read.
      List<CurrentState> lastCurStates =
          dataAccessor.getChildValues(keyBuilder.currentStates(instanceName, session), false);

      for (CurrentState lastCurState : lastCurStates) {
        LOG.info("Carrying over old session: " + session + ", resource: " + lastCurState.getId()
            + " to current session: " + sessionId + ", setToInitState: " + setToInitState);
        String stateModelDefRef = lastCurState.getStateModelDefRef();
        if (stateModelDefRef == null) {
          LOG.error(
              "skip carry-over because previous current state doesn't have a state model definition. previous current-state: "
                  + lastCurState);
          continue;
        }

        // If the the current state is related to tasks, there is no need to carry it over to new session.
        // Note: this check is not necessary due to TaskCurrentStates, but keep it for backwards compatibility
        if (stateModelDefRef.equals(TaskConstants.STATE_MODEL_NAME)) {
          continue;
        }

        StateModelDefinition stateModelDef =
            dataAccessor.getProperty(keyBuilder.stateModelDef(stateModelDefRef));
        String initState = stateModelDef.getInitialState();
        Map<String, String> partitionExpectedStateMap = new HashMap<>();
        if (setToInitState) {
          lastCurState.getPartitionStateMap().keySet()
              .forEach(partition -> partitionExpectedStateMap.put(partition, initState));
        } else {
          String factoryName = lastCurState.getStateModelFactoryName();
          StateModelFactory<? extends StateModel> stateModelFactory =
              stateMachineEngine.getStateModelFactory(stateModelDefRef, factoryName);
          lastCurState.getPartitionStateMap().keySet().forEach(partition -> {
            StateModel stateModel =
                stateModelFactory.getStateModel(lastCurState.getResourceName(), partition);
            if (stateModel != null) {
              partitionExpectedStateMap.put(partition, stateModel.getCurrentState());
            }
          });
        }

        BaseDataAccessor<ZNRecord> baseAccessor = dataAccessor.getBaseDataAccessor();
        String curStatePath =
            keyBuilder.currentState(instanceName, sessionId, lastCurState.getResourceName())
                .getPath();

        if (lastCurState.getBucketSize() > 0) {
          // update parent node
          ZNRecord metaRecord = new ZNRecord(lastCurState.getId());
          metaRecord.setSimpleFields(lastCurState.getRecord().getSimpleFields());
          DataUpdater<ZNRecord> metaRecordUpdater =
              new CurStateCarryOverUpdater(sessionId, partitionExpectedStateMap, new CurrentState(metaRecord));
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
              updaters.add(new CurStateCarryOverUpdater(sessionId, partitionExpectedStateMap, new CurrentState(map
                  .get(bucketName))));
            }

            baseAccessor.updateChildren(paths, updaters, AccessOption.PERSISTENT);
          }

        } else {
          dataAccessor.getBaseDataAccessor().update(curStatePath,
              new CurStateCarryOverUpdater(sessionId, partitionExpectedStateMap, lastCurState),
              AccessOption.PERSISTENT);
        }
      }
    }

    /**
     * remove previous current state parent nodes
     */
    for (String session : sessions) {
      if (session.equals(sessionId)) {
        continue;
      }

      PropertyKey currentStatesProperty = keyBuilder.currentStates(instanceName, session);
      String path = currentStatesProperty.getPath();
      LOG.info("Removing current states from previous sessions. path: {}", path);
      if (!dataAccessor.removeProperty(currentStatesProperty)) {
        throw new ZkClientException("Failed to delete " + path);
      }
    }
  }

  /**
   * Remove all previous task current state sessions
   */
  private void removePreviousTaskCurrentStates() {
    for (String session : _dataAccessor
        .getChildNames(_keyBuilder.taskCurrentStateSessions(_instanceName))) {
      if (session.equals(_sessionId)) {
        continue;
      }

      String path = _keyBuilder.taskCurrentStates(_instanceName, session).getPath();
      LOG.info("Removing task current states from previous sessions. path: " + path);
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
    return history == null ? new ParticipantHistory(_instanceName) : history;
  }

  private void persistHistory(ParticipantHistory history, boolean skipOnEmptyPath) {
    PropertyKey propertyKey = _keyBuilder.participantHistory(_instanceName);
    boolean result = skipOnEmptyPath
        ? _dataAccessor.updateProperty(
            propertyKey, currentData -> (currentData == null) ? null : history.getRecord(), history)
        : _dataAccessor.setProperty(propertyKey, history);
    if (!result) {
      LOG.error("Failed to persist participant history to zk!");
    }
  }

  public void reset() {
  }

  public void disconnect() {
    try {
      ParticipantHistory history = getHistory();
      history.reportOffline();
      persistHistory(history, true);
    } catch (Exception e) {
      LOG.error("Failed to report participant offline.", e);
    }
    reset();
  }
}
