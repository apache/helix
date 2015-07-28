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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ConstraintId;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
import org.apache.helix.controller.strategy.DefaultTwoStateStrategy;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.RebalanceUtil;
import org.apache.log4j.Logger;

public class ZKHelixAdmin implements HelixAdmin {
  public static final String CONNECTION_TIMEOUT = "helixAdmin.timeOutInSec";
  private final ZkClient _zkClient;
  private final ConfigAccessor _configAccessor;

  private static Logger logger = Logger.getLogger(ZKHelixAdmin.class);

  public ZKHelixAdmin(ZkClient zkClient) {
    _zkClient = zkClient;
    _configAccessor = new ConfigAccessor(zkClient);
  }

  public ZKHelixAdmin(String zkAddress) {
    int timeOutInSec = Integer.parseInt(System.getProperty(CONNECTION_TIMEOUT, "30"));
    _zkClient = new ZkClient(zkAddress, timeOutInSec * 1000);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _zkClient.waitUntilConnected(timeOutInSec, TimeUnit.SECONDS);
    _configAccessor = new ConfigAccessor(_zkClient);
  }

  @Override
  public void addInstance(String clusterName, InstanceConfig instanceConfig) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    String instanceConfigsPath =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString());
    String nodeId = instanceConfig.getId();
    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;

    if (_zkClient.exists(instanceConfigPath)) {
      throw new HelixException("Node " + nodeId + " already exists in cluster " + clusterName);
    }

    ZKUtil.createChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord());

    _zkClient.createPersistent(HelixUtil.getMessagePath(clusterName, nodeId), true);
    _zkClient.createPersistent(HelixUtil.getCurrentStateBasePath(clusterName, nodeId), true);
    _zkClient.createPersistent(HelixUtil.getErrorsPath(clusterName, nodeId), true);
    _zkClient.createPersistent(HelixUtil.getStatusUpdatesPath(clusterName, nodeId), true);
  }

  @Override
  public void dropInstance(String clusterName, InstanceConfig instanceConfig) {
    String instanceConfigsPath =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString());
    String nodeId = instanceConfig.getId();
    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;
    String instancePath = HelixUtil.getInstancePath(clusterName, nodeId);

    if (!_zkClient.exists(instanceConfigPath)) {
      throw new HelixException("Node " + nodeId + " does not exist in config for cluster "
          + clusterName);
    }

    if (!_zkClient.exists(instancePath)) {
      throw new HelixException("Node " + nodeId + " does not exist in instances for cluster "
          + clusterName);
    }

    // delete config path
    ZKUtil.dropChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord());

    // delete instance path
    _zkClient.deleteRecursive(instancePath);
  }

  @Override
  public InstanceConfig getInstanceConfig(String clusterName, String instanceName) {
    String instanceConfigPath =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString(), instanceName);
    if (!_zkClient.exists(instanceConfigPath)) {
      throw new HelixException("instance " + instanceName + " does not exist in cluster "
          + clusterName);
    }

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.instanceConfig(instanceName));
  }

  @Override
  public void enableInstance(final String clusterName, final String instanceName,
      final boolean enabled) {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString(), instanceName);

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);
    if (!baseAccessor.exists(path, 0)) {
      throw new HelixException("Cluster " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
              + ", participant config is null");
        }

        InstanceConfig config = new InstanceConfig(currentData);
        config.setInstanceEnabled(enabled);
        return config.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void enableResource(final String clusterName, final String resourceName,
      final boolean enabled) {
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName, resourceName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);
    if (!baseAccessor.exists(path, 0)) {
      throw new HelixException("Cluster " + clusterName + ", resource: " + resourceName
          + ", ideal-state does not exist");
    }
    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + clusterName + ", resource: " + resourceName
              + ", ideal-state is null");
        }
        IdealState idealState = new IdealState(currentData);
        idealState.enable(enabled);
        return idealState.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void enablePartition(final boolean enabled, final String clusterName,
      final String instanceName, final String resourceName, final List<String> partitionNames) {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString(), instanceName);

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    // check instanceConfig exists
    if (!baseAccessor.exists(path, 0)) {
      throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    // check resource exists
    String idealStatePath =
        PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName, resourceName);

    ZNRecord idealStateRecord = null;
    try {
      idealStateRecord = baseAccessor.get(idealStatePath, null, 0);
    } catch (ZkNoNodeException e) {
      // OK.
    }

    // check resource exist. warn if not.
    if (idealStateRecord == null) {
      // throw new HelixException("Cluster: " + clusterName + ", resource: " + resourceName
      // + ", ideal state does not exist");
      logger.warn("Disable partitions: " + partitionNames + " but Cluster: " + clusterName
          + ", resource: " + resourceName
          + " does not exists. probably disable it during ERROR->DROPPED transtition");

    } else {
      // check partitions exist. warn if not
      IdealState idealState = new IdealState(idealStateRecord);
      for (String partitionName : partitionNames) {
        if ((idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO && idealState
            .getPreferenceList(PartitionId.from(partitionName)) == null)
            || (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED && idealState
                .getParticipantStateMap(PartitionId.from(partitionName)) == null)) {
          logger.warn("Cluster: " + clusterName + ", resource: " + resourceName + ", partition: "
              + partitionName + ", partition does not exist in ideal state");
        }
      }
    }

    // update participantConfig
    // could not use ZNRecordUpdater since it doesn't do listField merge/subtract
    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
              + ", participant config is null");
        }

        // TODO: merge with InstanceConfig.setInstanceEnabledForPartition
        List<String> list =
            currentData.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
        Set<String> disabledPartitions = new HashSet<String>();
        if (list != null) {
          disabledPartitions.addAll(list);
        }

        if (enabled) {
          disabledPartitions.removeAll(partitionNames);
        } else {
          disabledPartitions.addAll(partitionNames);
        }

        list = new ArrayList<String>(disabledPartitions);
        Collections.sort(list);
        currentData.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(), list);
        return currentData;
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void enableCluster(String clusterName, boolean enabled) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    if (enabled) {
      accessor.removeProperty(keyBuilder.pause());
    } else {
      accessor.createProperty(keyBuilder.pause(), new PauseSignal("pause"));
    }
  }

  @Override
  public void resetPartition(String clusterName, String instanceName, String resourceName,
      List<String> partitionNames) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    // check the instance is alive
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      throw new HelixException("Can't reset state for " + resourceName + "/" + partitionNames
          + " on " + instanceName + ", because " + instanceName + " is not alive");
    }

    // check resource group exists
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    if (idealState == null) {
      throw new HelixException("Can't reset state for " + resourceName + "/" + partitionNames
          + " on " + instanceName + ", because " + resourceName + " is not added");
    }

    // check partition exists in resource group
    Set<String> resetPartitionNames = new HashSet<String>(partitionNames);
    if (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
      Set<String> partitions = new HashSet<String>(idealState.getRecord().getMapFields().keySet());
      if (!partitions.containsAll(resetPartitionNames)) {
        throw new HelixException("Can't reset state for " + resourceName + "/" + partitionNames
            + " on " + instanceName + ", because not all " + partitionNames + " exist");
      }
    } else {
      Set<String> partitions = new HashSet<String>(idealState.getRecord().getListFields().keySet());
      if (!partitions.containsAll(resetPartitionNames)) {
        throw new HelixException("Can't reset state for " + resourceName + "/" + partitionNames
            + " on " + instanceName + ", because not all " + partitionNames + " exist");
      }
    }

    // check partition is in ERROR state
    SessionId sessionId = liveInstance.getTypedSessionId();
    CurrentState curState =
        accessor.getProperty(keyBuilder.currentState(instanceName, sessionId.stringify(),
            resourceName));
    for (String partitionName : resetPartitionNames) {
      if (!curState.getState(partitionName).equals(HelixDefinedState.ERROR.toString())) {
        throw new HelixException("Can't reset state for " + resourceName + "/" + partitionNames
            + " on " + instanceName + ", because not all " + partitionNames + " are in ERROR state");
      }
    }

    // check stateModelDef exists and get initial state
    StateModelDefId stateModelDef = idealState.getStateModelDefId();
    StateModelDefinition stateModel =
        accessor.getProperty(keyBuilder.stateModelDef(stateModelDef.stringify()));
    if (stateModel == null) {
      throw new HelixException("Can't reset state for " + resourceName + "/" + partitionNames
          + " on " + instanceName + ", because " + stateModelDef + " is NOT found");
    }

    // check there is no pending messages for the partitions exist
    List<Message> messages = accessor.getChildValues(keyBuilder.messages(instanceName));
    for (Message message : messages) {
      if (!MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(message.getMsgType())
          || !sessionId.equals(message.getTypedTgtSessionId())
          || !resourceName.equals(message.getResourceId().stringify())
          || !resetPartitionNames.contains(message.getPartitionId().stringify())) {
        continue;
      }

      throw new HelixException("Can't reset state for " + resourceName + "/" + partitionNames
          + " on " + instanceName + ", because a pending message exists: " + message);
    }

    String adminName = null;
    try {
      adminName = InetAddress.getLocalHost().getCanonicalHostName() + "-ADMIN";
    } catch (UnknownHostException e) {
      // can ignore it
      logger.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e);
      adminName = "UNKNOWN";
    }

    List<Message> resetMessages = new ArrayList<Message>();
    List<PropertyKey> messageKeys = new ArrayList<PropertyKey>();
    for (String partitionName : resetPartitionNames) {
      // send ERROR to initialState message
      MessageId msgId = MessageId.from(UUID.randomUUID().toString());
      Message message = new Message(MessageType.STATE_TRANSITION, msgId);
      message.setSrcName(adminName);
      message.setTgtName(instanceName);
      message.setMsgState(MessageState.NEW);
      message.setPartitionId(PartitionId.from(partitionName));
      message.setResourceId(ResourceId.from(resourceName));
      message.setTgtSessionId(sessionId);
      message.setStateModelDef(stateModelDef);
      message.setFromState(State.from(HelixDefinedState.ERROR.toString()));
      message.setToState(stateModel.getTypedInitialState());
      message.setStateModelFactoryId(idealState.getStateModelFactoryId());

      resetMessages.add(message);
      messageKeys.add(keyBuilder.message(instanceName, message.getId()));
    }

    accessor.setChildren(messageKeys, resetMessages);
  }

  @Override
  public void resetInstance(String clusterName, List<String> instanceNames) {
    // TODO: not mp-safe
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    List<ExternalView> extViews = accessor.getChildValues(keyBuilder.externalViews());

    Set<String> resetInstanceNames = new HashSet<String>(instanceNames);
    for (String instanceName : resetInstanceNames) {
      List<String> resetPartitionNames = new ArrayList<String>();
      for (ExternalView extView : extViews) {
        Map<String, Map<String, String>> stateMap = extView.getRecord().getMapFields();
        for (String partitionName : stateMap.keySet()) {
          Map<String, String> instanceStateMap = stateMap.get(partitionName);

          if (instanceStateMap.containsKey(instanceName)
              && instanceStateMap.get(instanceName).equals(HelixDefinedState.ERROR.toString())) {
            resetPartitionNames.add(partitionName);
          }
        }
        resetPartition(clusterName, instanceName, extView.getResourceName(), resetPartitionNames);
      }
    }
  }

  @Override
  public void resetResource(String clusterName, List<String> resourceNames) {
    // TODO: not mp-safe
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    List<ExternalView> extViews = accessor.getChildValues(keyBuilder.externalViews());

    Set<String> resetResourceNames = new HashSet<String>(resourceNames);
    for (ExternalView extView : extViews) {
      if (!resetResourceNames.contains(extView.getResourceName())) {
        continue;
      }

      // instanceName -> list of resetPartitionNames
      Map<String, List<String>> resetPartitionNames = new HashMap<String, List<String>>();

      Map<String, Map<String, String>> stateMap = extView.getRecord().getMapFields();
      for (String partitionName : stateMap.keySet()) {
        Map<String, String> instanceStateMap = stateMap.get(partitionName);
        for (String instanceName : instanceStateMap.keySet()) {
          if (instanceStateMap.get(instanceName).equals(HelixDefinedState.ERROR.toString())) {
            if (!resetPartitionNames.containsKey(instanceName)) {
              resetPartitionNames.put(instanceName, new ArrayList<String>());
            }
            resetPartitionNames.get(instanceName).add(partitionName);
          }
        }
      }

      for (String instanceName : resetPartitionNames.keySet()) {
        resetPartition(clusterName, instanceName, extView.getResourceName(),
            resetPartitionNames.get(instanceName));
      }
    }
  }

  @Override
  public boolean addCluster(String clusterName) {
    return addCluster(clusterName, false);
  }

  @Override
  public boolean addCluster(String clusterName, boolean recreateIfExists) {
    String root = "/" + clusterName;

    if (_zkClient.exists(root)) {
      if (recreateIfExists) {
        logger.warn("Root directory exists.Cleaning the root directory:" + root);
        _zkClient.deleteRecursive(root);
      } else {
        logger.info("Cluster " + clusterName + " already exists");
        return true;
      }
    }
    try {
      _zkClient.createPersistent(root, true);
    } catch (Exception e) {
      // some other process might have created the cluster
      if (_zkClient.exists(root)) {
        return true;
      }
      logger.error("Error creating cluster:" + clusterName, e);
      return false;
    }
    try {
      createZKPaths(clusterName);
    } catch (Exception e) {
      logger.error("Error creating cluster:" + clusterName, e);
      return false;
    }
    logger.info("Created cluster:" + clusterName);
    return true;
  }

  private void createZKPaths(String clusterName) {
    String path;

    // IDEAL STATE
    _zkClient.createPersistent(HelixUtil.getIdealStatePath(clusterName));
    // CONFIGURATIONS
    path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.CLUSTER.toString(), clusterName);
    _zkClient.createPersistent(path, true);
    _zkClient.writeData(path, new ZNRecord(clusterName));
    path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString());
    _zkClient.createPersistent(path);
    path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.RESOURCE.toString());
    _zkClient.createPersistent(path);
    // PROPERTY STORE
    path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    _zkClient.createPersistent(path);
    // LIVE INSTANCES
    _zkClient.createPersistent(HelixUtil.getLiveInstancesPath(clusterName));
    // MEMBER INSTANCES
    _zkClient.createPersistent(HelixUtil.getMemberInstancesPath(clusterName));
    // External view
    _zkClient.createPersistent(HelixUtil.getExternalViewPath(clusterName));
    // State model definition
    _zkClient.createPersistent(HelixUtil.getStateModelDefinitionPath(clusterName));

    // controller
    _zkClient.createPersistent(HelixUtil.getControllerPath(clusterName));
    path = PropertyPathConfig.getPath(PropertyType.HISTORY, clusterName);
    final ZNRecord emptyHistory = new ZNRecord(PropertyType.HISTORY.toString());
    final List<String> emptyList = new ArrayList<String>();
    emptyHistory.setListField(clusterName, emptyList);
    _zkClient.createPersistent(path, emptyHistory);

    path = PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER, clusterName);
    _zkClient.createPersistent(path);

    path = PropertyPathConfig.getPath(PropertyType.STATUSUPDATES_CONTROLLER, clusterName);
    _zkClient.createPersistent(path);

    path = PropertyPathConfig.getPath(PropertyType.ERRORS_CONTROLLER, clusterName);
    _zkClient.createPersistent(path);
  }

  @Override
  public List<String> getInstancesInCluster(String clusterName) {
    String memberInstancesPath = HelixUtil.getMemberInstancesPath(clusterName);
    return _zkClient.getChildren(memberInstancesPath);
  }

  @Override
  public List<String> getInstancesInClusterWithTag(String clusterName, String tag) {
    String memberInstancesPath = HelixUtil.getMemberInstancesPath(clusterName);
    List<String> instances = _zkClient.getChildren(memberInstancesPath);
    List<String> result = new ArrayList<String>();

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    for (String instanceName : instances) {
      InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
      if (config.containsTag(tag)) {
        result.add(instanceName);
      }
    }
    return result;
  }

  @Override
  public void addResource(String clusterName, String resourceName, int partitions,
      String stateModelRef) {
    addResource(clusterName, resourceName, partitions, stateModelRef,
        RebalanceMode.SEMI_AUTO.toString(), 0);
  }

  @Override
  public void addResource(String clusterName, String resourceName, int partitions,
      String stateModelRef, String rebalancerMode) {
    addResource(clusterName, resourceName, partitions, stateModelRef, rebalancerMode, 0);
  }

  @Override
  public void addResource(String clusterName, String resourceName, IdealState idealstate) {
    String stateModelRef = idealstate.getStateModelDefId().stringify();
    String stateModelDefPath =
        PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, clusterName, stateModelRef);
    if (!_zkClient.exists(stateModelDefPath)) {
      throw new HelixException("State model " + stateModelRef
          + " not found in the cluster STATEMODELDEFS path");
    }

    String idealStatePath = HelixUtil.getIdealStatePath(clusterName);
    String resourceIdealStatePath = idealStatePath + "/" + resourceName;
    if (_zkClient.exists(resourceIdealStatePath)) {
      throw new HelixException("Skip the operation. Resource ideal state directory already exists:"
          + resourceIdealStatePath);
    }

    ZKUtil.createChildren(_zkClient, idealStatePath, idealstate.getRecord());
  }

  @Override
  public void addResource(String clusterName, String resourceName, int partitions,
      String stateModelRef, String rebalancerMode, int bucketSize) {
    addResource(clusterName, resourceName, partitions, stateModelRef, rebalancerMode, bucketSize,
        -1);

  }

  @Override
  public void addResource(String clusterName, String resourceName, int partitions,
      String stateModelRef, String rebalancerMode, int bucketSize, int maxPartitionsPerInstance) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    IdealState idealState = new IdealState(resourceName);
    idealState.setNumPartitions(partitions);
    idealState.setStateModelDefId(StateModelDefId.from(stateModelRef));
    RebalanceMode mode =
        idealState.rebalanceModeFromString(rebalancerMode, RebalanceMode.SEMI_AUTO);
    idealState.setRebalanceMode(mode);
    idealState.setReplicas("" + 0);
    idealState.setStateModelFactoryId(StateModelFactoryId
        .from(HelixConstants.DEFAULT_STATE_MODEL_FACTORY));
    if (maxPartitionsPerInstance > 0 && maxPartitionsPerInstance < Integer.MAX_VALUE) {
      idealState.setMaxPartitionsPerInstance(maxPartitionsPerInstance);
    }
    if (bucketSize > 0) {
      idealState.setBucketSize(bucketSize);
    }
    addResource(clusterName, resourceName, idealState);
  }

  @Override
  public List<String> getClusters() {
    List<String> zkToplevelPathes = _zkClient.getChildren("/");
    List<String> result = new ArrayList<String>();
    for (String pathName : zkToplevelPathes) {
      if (ZKUtil.isClusterSetup(pathName, _zkClient)) {
        result.add(pathName);
      }
    }
    return result;
  }

  @Override
  public List<String> getResourcesInCluster(String clusterName) {
    return _zkClient.getChildren(HelixUtil.getIdealStatePath(clusterName));
  }

  @Override
  public IdealState getResourceIdealState(String clusterName, String resourceName) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.idealStates(resourceName));
  }

  @Override
  public void setResourceIdealState(String clusterName, String resourceName, IdealState idealState) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
  }

  @Override
  public ExternalView getResourceExternalView(String clusterName, String resourceName) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.externalView(resourceName));
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDef,
      StateModelDefinition stateModel) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    String stateModelDefPath = HelixUtil.getStateModelDefinitionPath(clusterName);
    String stateModelPath = stateModelDefPath + "/" + stateModelDef;
    if (_zkClient.exists(stateModelPath)) {
      logger.warn("Skip the operation.State Model directory exists:" + stateModelPath);
      throw new HelixException("State model path " + stateModelPath + " already exists.");
    }

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef(stateModel.getId()), stateModel);
  }

  @Override
  public void dropStateModelDef(String clusterName, String stateModelDef) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    String stateModelDefPath = HelixUtil.getStateModelDefinitionPath(clusterName);
    String stateModelPath = stateModelDefPath + "/" + stateModelDef;
    if (!_zkClient.exists(stateModelPath)) {
      logger.warn("Skip the operation.State Model directory does not exist:" + stateModelPath);
      return;
    }
    _zkClient.deleteRecursive(stateModelPath);
    if (_zkClient.exists(stateModelPath)) {
      logger.warn("Unable to delete State Model directory:" + stateModelPath);
      throw new HelixException("State model path " + stateModelPath + " not deleted");
    }
  }

  @Override
  public void dropResource(String clusterName, String resourceName) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.removeProperty(keyBuilder.idealStates(resourceName));
    accessor.removeProperty(keyBuilder.resourceConfig(resourceName));
  }

  @Override
  public List<String> getStateModelDefs(String clusterName) {
    return _zkClient.getChildren(HelixUtil.getStateModelDefinitionPath(clusterName));
  }

  @Override
  public StateModelDefinition getStateModelDef(String clusterName, String stateModelName) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.stateModelDef(stateModelName));
  }

  @Override
  public void dropCluster(String clusterName) {
    logger.info("Deleting cluster " + clusterName);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    String root = "/" + clusterName;
    if (accessor.getChildNames(keyBuilder.liveInstances()).size() > 0) {
      throw new HelixException(
          "There are still live instances in the cluster, shut them down first.");
    }

    if (accessor.getProperty(keyBuilder.controllerLeader()) != null) {
      throw new HelixException("There are still LEADER in the cluster, shut them down first.");
    }

    _zkClient.deleteRecursive(root);
  }

  @Override
  public void addClusterToGrandCluster(String clusterName, String grandCluster) {
    if (!ZKUtil.isClusterSetup(grandCluster, _zkClient)) {
      throw new HelixException("Grand cluster " + grandCluster + " is not setup yet");
    }

    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("Cluster " + clusterName + " is not setup yet");
    }

    IdealState idealState = new IdealState(clusterName);

    idealState.setNumPartitions(1);
    idealState.setStateModelDefId(StateModelDefId.from("LeaderStandby"));

    List<String> controllers = getInstancesInCluster(grandCluster);
    if (controllers.size() == 0) {
      throw new HelixException("Grand cluster " + grandCluster + " has no instances");
    }
    idealState.setReplicas(Integer.toString(controllers.size()));
    Collections.shuffle(controllers);
    idealState.getRecord().setListField(clusterName, controllers);
    idealState.setPartitionState(clusterName, controllers.get(0), "LEADER");
    for (int i = 1; i < controllers.size(); i++) {
      idealState.setPartitionState(clusterName, controllers.get(i), "STANDBY");
    }

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(grandCluster, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor
        .setProperty(keyBuilder.idealStates(idealState.getResourceId().stringify()), idealState);
  }

  @Override
  public void setConfig(HelixConfigScope scope, Map<String, String> properties) {
    _configAccessor.set(scope, properties);
  }

  @Override
  public Map<String, String> getConfig(HelixConfigScope scope, List<String> keys) {
    return _configAccessor.get(scope, keys);
  }

  @Override
  public List<String> getConfigKeys(HelixConfigScope scope) {
    return _configAccessor.getKeys(scope);
  }

  @Override
  public void removeConfig(HelixConfigScope scope, List<String> keys) {
    _configAccessor.remove(scope, keys);
  }

  @Override
  public void rebalance(String clusterName, String resourceName, int replica) {
    rebalance(clusterName, resourceName, replica, resourceName, "");
  }

  @Override
  public void rebalance(String clusterName, String resourceName, int replica, String keyPrefix,
      String group) {
    List<String> instanceNames = new LinkedList<String>();
    if (keyPrefix == null || keyPrefix.length() == 0) {
      keyPrefix = resourceName;
    }
    if (group != null && group.length() > 0) {
      instanceNames = getInstancesInClusterWithTag(clusterName, group);
    }
    if (instanceNames.size() == 0) {
      logger.info("No tags found for resource " + resourceName + ", use all instances");
      instanceNames = getInstancesInCluster(clusterName);
      group = "";
    } else {
      logger.info("Found instances with tag for " + resourceName + " " + instanceNames);
    }
    rebalance(clusterName, resourceName, replica, keyPrefix, instanceNames, group);
  }

  @Override
  public void rebalance(String clusterName, String resourceName, int replica, List<String> instances) {
    rebalance(clusterName, resourceName, replica, resourceName, instances, "");
  }

  void rebalance(String clusterName, String resourceName, int replica, String keyPrefix,
      List<String> instanceNames, String groupId) {
    // ensure we get the same idealState with the same set of instances
    Collections.sort(instanceNames);

    IdealState idealState = getResourceIdealState(clusterName, resourceName);
    if (idealState == null) {
      throw new HelixException("Resource: " + resourceName + " has NOT been added yet");
    }

    if (groupId != null && groupId.length() > 0) {
      idealState.setInstanceGroupTag(groupId);
    }
    idealState.setReplicas(Integer.toString(replica));
    int partitions = idealState.getNumPartitions();
    String stateModelName = idealState.getStateModelDefId().stringify();
    StateModelDefinition stateModDef = getStateModelDef(clusterName, stateModelName);

    if (stateModDef == null) {
      throw new HelixException("cannot find state model: " + stateModelName);
    }
    // StateModelDefinition def = new StateModelDefinition(stateModDef);

    List<String> statePriorityList = stateModDef.getStatesPriorityList();

    String masterStateValue = null;
    String slaveStateValue = null;
    replica--;

    for (String state : statePriorityList) {
      String count = stateModDef.getNumInstancesPerState(state);
      if (count.equals("1")) {
        if (masterStateValue != null) {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        masterStateValue = state;
      } else if (count.equalsIgnoreCase("R")) {
        if (slaveStateValue != null) {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        slaveStateValue = state;
      } else if (count.equalsIgnoreCase("N")) {
        if (!(masterStateValue == null && slaveStateValue == null)) {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        replica = instanceNames.size() - 1;
        masterStateValue = slaveStateValue = state;
      }
    }
    if (masterStateValue == null && slaveStateValue == null) {
      throw new HelixException("Invalid or unsupported state model definition");
    }

    if (masterStateValue == null) {
      masterStateValue = slaveStateValue;
    }
    if (idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO
        || idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
      ZNRecord newIdealState =
          DefaultTwoStateStrategy.calculateIdealState(instanceNames, partitions, replica,
              keyPrefix, masterStateValue, slaveStateValue);

      // for now keep mapField in SEMI_AUTO mode and remove listField in CUSTOMIZED mode
      if (idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
        idealState.getRecord().setListFields(newIdealState.getListFields());
        idealState.getRecord().setMapFields(newIdealState.getMapFields());
      }
      if (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
        idealState.getRecord().setMapFields(newIdealState.getMapFields());
      }
    } else {
      for (int i = 0; i < partitions; i++) {
        String partitionName = keyPrefix + "_" + i;
        idealState.getRecord().setMapField(partitionName, new HashMap<String, String>());
        idealState.getRecord().setListField(partitionName, new ArrayList<String>());
      }
    }
    setResourceIdealState(clusterName, resourceName, idealState);
  }

  @Override
  public void addIdealState(String clusterName, String resourceName, String idealStateFile)
      throws IOException {
    ZNRecord idealStateRecord =
        (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(idealStateFile)));
    if (idealStateRecord.getId() == null || !idealStateRecord.getId().equals(resourceName)) {
      throw new IllegalArgumentException("ideal state must have same id as resource name");
    }
    setResourceIdealState(clusterName, resourceName, new IdealState(idealStateRecord));
  }

  private static byte[] readFile(String filePath) throws IOException {
    File file = new File(filePath);

    int size = (int) file.length();
    byte[] bytes = new byte[size];
    DataInputStream dis = null;
    try {
      dis = new DataInputStream(new FileInputStream(file));
      int read = 0;
      int numRead = 0;
      while (read < bytes.length && (numRead = dis.read(bytes, read, bytes.length - read)) >= 0) {
        read = read + numRead;
      }
      return bytes;
    } finally {
      if (dis != null) {
        dis.close();
      }
    }
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDefName,
      String stateModelDefFile) throws IOException {
    ZNRecord record =
        (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(stateModelDefFile)));
    if (record == null || record.getId() == null || !record.getId().equals(stateModelDefName)) {
      throw new IllegalArgumentException(
          "state model definition must have same id as state model def name");
    }
    addStateModelDef(clusterName, stateModelDefName, new StateModelDefinition(record));

  }

  @Override
  public void setConstraint(String clusterName, final ConstraintType constraintType,
      final String constraintId, final ConstraintItem constraintItem) {
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    Builder keyBuilder = new Builder(clusterName);
    String path = keyBuilder.constraint(constraintType.toString()).getPath();

    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        ClusterConstraints constraints =
            currentData == null ? new ClusterConstraints(constraintType) : new ClusterConstraints(
                currentData);

        constraints.addConstraintItem(ConstraintId.from(constraintId), constraintItem);
        return constraints.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void removeConstraint(String clusterName, final ConstraintType constraintType,
      final String constraintId) {
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    Builder keyBuilder = new Builder(clusterName);
    String path = keyBuilder.constraint(constraintType.toString()).getPath();

    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData != null) {
          ClusterConstraints constraints = new ClusterConstraints(currentData);

          constraints.removeConstraintItem(ConstraintId.from(constraintId));
          return constraints.getRecord();
        }
        return null;
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public ClusterConstraints getConstraints(String clusterName, ConstraintType constraintType) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));

    Builder keyBuilder = new Builder(clusterName);
    return accessor.getProperty(keyBuilder.constraint(constraintType.toString()));
  }

  /**
   * Takes the existing idealstate as input and computes newIdealState such that
   * the partition movement is minimized. The partitions are redistributed among the instances
   * provided.
   * @param clusterName
   * @param currentIdealState
   * @param instanceNames
   * @return
   */
  @Override
  public void rebalance(String clusterName, IdealState currentIdealState, List<String> instanceNames) {
    Set<String> activeInstances = new HashSet<String>();
    for (PartitionId partition : currentIdealState.getPartitionIdSet()) {
      activeInstances.addAll(IdealState.stringListFromPreferenceList(currentIdealState
          .getPreferenceList(partition)));
    }
    instanceNames.removeAll(activeInstances);
    Map<String, Object> previousIdealState =
        RebalanceUtil.buildInternalIdealState(currentIdealState);

    Map<String, Object> balancedRecord =
        DefaultTwoStateStrategy.calculateNextIdealState(instanceNames, previousIdealState);
    StateModelDefinition stateModDef =
        this.getStateModelDef(clusterName, currentIdealState.getStateModelDefId().stringify());

    if (stateModDef == null) {
      throw new HelixException("cannot find state model: " + currentIdealState.getStateModelDefId());
    }
    String[] states = RebalanceUtil.parseStates(clusterName, stateModDef);

    ZNRecord newIdealStateRecord =
        DefaultTwoStateStrategy.convertToZNRecord(balancedRecord, currentIdealState.getResourceId()
            .stringify(), states[0], states[1]);
    Set<String> partitionSet = new HashSet<String>();
    partitionSet.addAll(newIdealStateRecord.getMapFields().keySet());
    partitionSet.addAll(newIdealStateRecord.getListFields().keySet());

    Map<String, String> reversePartitionIndex =
        (Map<String, String>) balancedRecord.get("reversePartitionIndex");
    for (String partition : partitionSet) {
      if (reversePartitionIndex.containsKey(partition)) {
        String originPartitionName = reversePartitionIndex.get(partition);
        if (partition.equals(originPartitionName)) {
          continue;
        }
        newIdealStateRecord.getMapFields().put(originPartitionName,
            newIdealStateRecord.getMapField(partition));
        newIdealStateRecord.getMapFields().remove(partition);

        newIdealStateRecord.getListFields().put(originPartitionName,
            newIdealStateRecord.getListField(partition));
        newIdealStateRecord.getListFields().remove(partition);
      }
    }

    newIdealStateRecord.getSimpleFields().putAll(currentIdealState.getRecord().getSimpleFields());
    IdealState newIdealState = new IdealState(newIdealStateRecord);
    setResourceIdealState(clusterName, newIdealStateRecord.getId(), newIdealState);
  }

  @Override
  public void addInstanceTag(String clusterName, String instanceName, String tag) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    if (!ZKUtil.isInstanceSetup(_zkClient, clusterName, instanceName, InstanceType.PARTICIPANT)) {
      throw new HelixException("cluster " + clusterName + " instance " + instanceName
          + " is not setup yet");
    }
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    config.addTag(tag);
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), config);
  }

  @Override
  public void removeInstanceTag(String clusterName, String instanceName, String tag) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    if (!ZKUtil.isInstanceSetup(_zkClient, clusterName, instanceName, InstanceType.PARTICIPANT)) {
      throw new HelixException("cluster " + clusterName + " instance " + instanceName
          + " is not setup yet");
    }
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    config.removeTag(tag);
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), config);
  }

  @Override
  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
    }
  }

}
