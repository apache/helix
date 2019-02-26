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
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkException;
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
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.RebalanceUtil;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKHelixAdmin implements HelixAdmin {
  public static final String CONNECTION_TIMEOUT = "helixAdmin.timeOutInSec";
  private static final String MAINTENANCE_ZNODE_ID = "maintenance";

  private final HelixZkClient _zkClient;
  private final ConfigAccessor _configAccessor;

  private static Logger logger = LoggerFactory.getLogger(ZKHelixAdmin.class);

  public ZKHelixAdmin(HelixZkClient zkClient) {
    _zkClient = zkClient;
    _configAccessor = new ConfigAccessor(zkClient);
  }

  public ZKHelixAdmin(String zkAddress) {
    int timeOutInSec = Integer.parseInt(System.getProperty(CONNECTION_TIMEOUT, "30"));
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer())
        .setConnectInitTimeout(timeOutInSec * 1000);
    _zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress), clientConfig);
    _zkClient.waitUntilConnected(timeOutInSec, TimeUnit.SECONDS);
    _configAccessor = new ConfigAccessor(_zkClient);
  }

  @Override
  public void addInstance(String clusterName, InstanceConfig instanceConfig) {
    logger.info("Add instance {} to cluster {}.", instanceConfig.getInstanceName(), clusterName);
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    String instanceConfigsPath = PropertyPathBuilder.instanceConfig(clusterName);
    String nodeId = instanceConfig.getId();
    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;

    if (_zkClient.exists(instanceConfigPath)) {
      throw new HelixException("Node " + nodeId + " already exists in cluster " + clusterName);
    }

    ZKUtil.createChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord());

    _zkClient.createPersistent(PropertyPathBuilder.instanceMessage(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceCurrentState(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceError(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceStatusUpdate(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceHistory(clusterName, nodeId), true);
  }

  @Override
  public void dropInstance(String clusterName, InstanceConfig instanceConfig) {
    logger.info("Drop instance {} from cluster {}.", instanceConfig.getInstanceName(), clusterName);
    String instanceName = instanceConfig.getInstanceName();

    String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
    if (!_zkClient.exists(instanceConfigPath)) {
      throw new HelixException(
          "Node " + instanceName + " does not exist in config for cluster " + clusterName);
    }

    String instancePath = PropertyPathBuilder.instance(clusterName, instanceName);
    if (!_zkClient.exists(instancePath)) {
      throw new HelixException(
          "Node " + instanceName + " does not exist in instances for cluster " + clusterName);
    }

    String liveInstancePath = PropertyPathBuilder.liveInstance(clusterName, instanceName);
    if (_zkClient.exists(liveInstancePath)) {
      throw new HelixException(
          "Node " + instanceName + " is still alive for cluster " + clusterName + ", can't drop.");
    }

    // delete config path
    String instanceConfigsPath = PropertyPathBuilder.instanceConfig(clusterName);
    ZKUtil.dropChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord());

    // delete instance path
    int retryCnt = 0;
    while (true) {
      try {
        _zkClient.deleteRecursively(instancePath);
        return;
      } catch (HelixException e) {
        if (retryCnt < 3 && e.getCause() instanceof ZkException && e.getCause()
            .getCause() instanceof KeeperException.NotEmptyException) {
          // Racing condition with controller's persisting node history, retryable.
          // We don't need to backoff here as this racing condition only happens once (controller
          // does not repeatedly write instance history)
          logger.warn("Retrying dropping instance {} with exception {}",
              instanceConfig.getInstanceName(), e.getCause().getMessage());
          retryCnt ++;
        } else {
          logger.error("Failed to drop instance {} (not retryable).",
              instanceConfig.getInstanceName(), e.getCause());
          throw e;
        }
      }
    }
  }

  @Override
  public InstanceConfig getInstanceConfig(String clusterName, String instanceName) {
    logger.info("Get instance config for instance {} from cluster {}.", instanceName, clusterName);
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
    if (!_zkClient.exists(instanceConfigPath)) {
      throw new HelixException(
          "instance" + instanceName + " does not exist in cluster " + clusterName);
    }

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.instanceConfig(instanceName));
  }

  @Override
  public boolean setInstanceConfig(String clusterName, String instanceName,
      InstanceConfig newInstanceConfig) {
    logger.info("Set instance config for instance {} to cluster {} with new InstanceConfig {}.",
        instanceName, clusterName,
        newInstanceConfig == null ? "NULL" : newInstanceConfig.toString());
    String instanceConfigPath = PropertyPathBuilder.getPath(PropertyType.CONFIGS, clusterName,
        HelixConfigScope.ConfigScopeProperty.PARTICIPANT.toString(), instanceName);
    if (!_zkClient.exists(instanceConfigPath)) {
      throw new HelixException(
          "instance" + instanceName + " does not exist in cluster " + clusterName);
    }

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    PropertyKey instanceConfigPropertyKey = accessor.keyBuilder().instanceConfig(instanceName);
    InstanceConfig currentInstanceConfig = accessor.getProperty(instanceConfigPropertyKey);
    if (!newInstanceConfig.getHostName().equals(currentInstanceConfig.getHostName())
        || !newInstanceConfig.getPort().equals(currentInstanceConfig.getPort())) {
      throw new HelixException(
          "Hostname and port cannot be changed, current hostname: " + currentInstanceConfig
              .getHostName() + " and port: " + currentInstanceConfig.getPort()
              + " is different from new hostname: " + newInstanceConfig.getHostName()
              + "and new port: " + newInstanceConfig.getPort());
    }
    return accessor.setProperty(instanceConfigPropertyKey, newInstanceConfig);
  }

  @Override
  public void enableInstance(final String clusterName, final String instanceName,
      final boolean enabled) {
    logger.info("{} instance {} in cluster {}.", enabled ? "Enable" : "Disable", instanceName,
        clusterName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_zkClient);
    enableSingleInstance(clusterName, instanceName, enabled, baseAccessor);
    // TODO: Reenable this after storage node bug fixed.
    // enableBatchInstances(clusterName, Collections.singletonList(instanceName), enabled, baseAccessor);

  }

  @Override
  public void enableInstance(String clusterName, List<String> instances, boolean enabled) {
    // TODO: Reenable this after storage node bug fixed.
    if (true) {
      throw new HelixException("Current batch enable/disable instances are temporarily disabled!");
    }
    logger.info("Batch {} instances {} in cluster {}.", enabled ? "enable" : "disable",
        HelixUtil.serializeByComma(instances), clusterName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_zkClient);
    if (enabled) {
      for (String instance : instances) {
        enableSingleInstance(clusterName, instance, enabled, baseAccessor);
      }
    }
    enableBatchInstances(clusterName, instances, enabled, baseAccessor);
  }

  @Override
  public void enableResource(final String clusterName, final String resourceName,
      final boolean enabled) {
    logger.info("{} resource {} in cluster {}.", enabled ? "Enable" : "Disable", resourceName,
        clusterName);
    String path = PropertyPathBuilder.idealState(clusterName, resourceName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);
    if (!baseAccessor.exists(path, 0)) {
      throw new HelixException("Cluster " + clusterName + ", resource: " + resourceName
          + ", ideal-state does not exist");
    }
    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException(
              "Cluster: " + clusterName + ", resource: " + resourceName + ", ideal-state is null");
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
    logger.info("{} partitions {} for resource {} on instance {} in cluster {}.",
        enabled ? "Enable" : "Disable", HelixUtil.serializeByComma(partitionNames), resourceName,
        instanceName, clusterName);
    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    // check instanceConfig exists
    if (!baseAccessor.exists(path, 0)) {
      throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    // check resource exists
    String idealStatePath = PropertyPathBuilder.idealState(clusterName, resourceName);

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
      logger.warn(
          "Disable partitions: " + partitionNames + " but Cluster: " + clusterName + ", resource: "
              + resourceName
              + " does not exists. probably disable it during ERROR->DROPPED transtition");
    } else {
      // check partitions exist. warn if not
      IdealState idealState = new IdealState(idealStateRecord);
      for (String partitionName : partitionNames) {
        if ((idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO
            && idealState.getPreferenceList(partitionName) == null) || (
            idealState.getRebalanceMode() == RebalanceMode.USER_DEFINED
                && idealState.getPreferenceList(partitionName) == null) || (
            idealState.getRebalanceMode() == RebalanceMode.TASK
                && idealState.getPreferenceList(partitionName) == null) || (
            idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED
                && idealState.getInstanceStateMap(partitionName) == null)) {
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

        InstanceConfig instanceConfig = new InstanceConfig(currentData);
        for (String partitionName : partitionNames) {
          instanceConfig.setInstanceEnabledForPartition(resourceName, partitionName, enabled);
        }

        return instanceConfig.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void enableCluster(String clusterName, boolean enabled) {
    enableCluster(clusterName, enabled, null);
  }

  /**
   * @param clusterName
   * @param enabled
   * @param reason      set additional string description on why the cluster is disabled when
   *                    <code>enabled</code> is false.
   */
  @Override
  public void enableCluster(String clusterName, boolean enabled, String reason) {
    logger.info("{} cluster {} for reason {}.", enabled ? "Enable" : "Disable", clusterName,
        reason == null ? "NULL" : reason);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    if (enabled) {
      accessor.removeProperty(keyBuilder.pause());
    } else {
      PauseSignal pauseSignal = new PauseSignal("pause");
      if (reason != null) {
        pauseSignal.setReason(reason);
      }
      if (!accessor.createPause(pauseSignal)) {
        throw new HelixException("Failed to create pause signal");
      }
    }
  }

  @Override
  @Deprecated
  public void enableMaintenanceMode(String clusterName, boolean enabled) {
    manuallyEnableMaintenanceMode(clusterName, enabled, null, null);
  }

  @Override
  @Deprecated
  public void enableMaintenanceMode(String clusterName, boolean enabled, String reason) {
    manuallyEnableMaintenanceMode(clusterName, enabled, reason, null);
  }

  @Override
  public void autoEnableMaintenanceMode(String clusterName, boolean enabled, String reason,
      MaintenanceSignal.AutoTriggerReason internalReason) {
    processMaintenanceMode(clusterName, enabled, reason, internalReason, null,
        MaintenanceSignal.TriggeringEntity.CONTROLLER);
  }

  @Override
  public void manuallyEnableMaintenanceMode(String clusterName, boolean enabled, String reason,
      Map<String, String> customFields) {
    processMaintenanceMode(clusterName, enabled, reason,
        MaintenanceSignal.AutoTriggerReason.NOT_APPLICABLE, customFields,
        MaintenanceSignal.TriggeringEntity.USER);
  }

  /**
   * Helper method for enabling/disabling maintenance mode.
   * @param clusterName
   * @param enabled
   * @param reason
   * @param internalReason
   * @param customFields
   * @param triggeringEntity
   */
  private void processMaintenanceMode(String clusterName, boolean enabled, String reason,
      MaintenanceSignal.AutoTriggerReason internalReason, Map<String, String> customFields,
      MaintenanceSignal.TriggeringEntity triggeringEntity) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    logger.info("Cluster {} {} {} maintenance mode for reason {}.", clusterName,
        triggeringEntity == MaintenanceSignal.TriggeringEntity.CONTROLLER ? "automatically"
            : "manually",
        enabled ? "enters" : "exits", reason == null ? "NULL" : reason);
    if (!enabled) {
      // Exit maintenance mode
      accessor.removeProperty(keyBuilder.maintenance());
    } else {
      // Enter maintenance mode
      MaintenanceSignal maintenanceSignal = new MaintenanceSignal(MAINTENANCE_ZNODE_ID);
      if (reason != null) {
        maintenanceSignal.setReason(reason);
      }
      maintenanceSignal.setTimestamp(System.currentTimeMillis());
      maintenanceSignal.setTriggeringEntity(triggeringEntity);
      switch (triggeringEntity) {
      case CONTROLLER:
        // autoEnable
        maintenanceSignal.setAutoTriggerReason(internalReason);
        break;
      case USER:
      case UNKNOWN:
        // manuallyEnable
        if (customFields != null && !customFields.isEmpty()) {
          // Enter all custom fields provided by the user
          Map<String, String> simpleFields = maintenanceSignal.getRecord().getSimpleFields();
          for (Map.Entry<String, String> entry : customFields.entrySet()) {
            if (!simpleFields.containsKey(entry.getKey())) {
              simpleFields.put(entry.getKey(), entry.getValue());
            }
          }
        }
        break;
      }
      if (!accessor.createMaintenance(maintenanceSignal)) {
        throw new HelixException("Failed to create maintenance signal");
      }
    }
  }

  @Override
  public void resetPartition(String clusterName, String instanceName, String resourceName,
      List<String> partitionNames) {
    logger.info("Reset partitions {} for resource {} on instance {} in cluster {}.",
        partitionNames == null ? "NULL" : HelixUtil.serializeByComma(partitionNames), resourceName,
        instanceName, clusterName);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    // check the instance is alive
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      throw new HelixException(
          "Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName
              + ", because " + instanceName + " is not alive");
    }

    // check resource group exists
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    if (idealState == null) {
      throw new HelixException(
          "Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName
              + ", because " + resourceName + " is not added");
    }

    // check partition exists in resource group
    Set<String> resetPartitionNames = new HashSet<String>(partitionNames);
    if (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
      Set<String> partitions = new HashSet<String>(idealState.getRecord().getMapFields().keySet());
      if (!partitions.containsAll(resetPartitionNames)) {
        throw new HelixException(
            "Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName
                + ", because not all " + partitionNames + " exist");
      }
    } else {
      Set<String> partitions = new HashSet<String>(idealState.getRecord().getListFields().keySet());
      if (!partitions.containsAll(resetPartitionNames)) {
        throw new HelixException(
            "Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName
                + ", because not all " + partitionNames + " exist");
      }
    }

    // check partition is in ERROR state
    String sessionId = liveInstance.getSessionId();
    CurrentState curState =
        accessor.getProperty(keyBuilder.currentState(instanceName, sessionId, resourceName));
    for (String partitionName : resetPartitionNames) {
      if (!curState.getState(partitionName).equals(HelixDefinedState.ERROR.toString())) {
        throw new HelixException(
            "Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName
                + ", because not all " + partitionNames + " are in ERROR state");
      }
    }

    // check stateModelDef exists and get initial state
    String stateModelDef = idealState.getStateModelDefRef();
    StateModelDefinition stateModel = accessor.getProperty(keyBuilder.stateModelDef(stateModelDef));
    if (stateModel == null) {
      throw new HelixException(
          "Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName
              + ", because " + stateModelDef + " is NOT found");
    }

    // check there is no pending messages for the partitions exist
    List<Message> messages = accessor.getChildValues(keyBuilder.messages(instanceName));
    for (Message message : messages) {
      if (!MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType()) || !sessionId
          .equals(message.getTgtSessionId()) || !resourceName.equals(message.getResourceName())
          || !resetPartitionNames.contains(message.getPartitionName())) {
        continue;
      }

      throw new HelixException(
          "Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName
              + ", because a pending message exists: " + message);
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
      String msgId = UUID.randomUUID().toString();
      Message message = new Message(MessageType.STATE_TRANSITION, msgId);
      message.setSrcName(adminName);
      message.setTgtName(instanceName);
      message.setMsgState(MessageState.NEW);
      message.setPartitionName(partitionName);
      message.setResourceName(resourceName);
      message.setTgtSessionId(sessionId);
      message.setStateModelDef(stateModelDef);
      message.setFromState(HelixDefinedState.ERROR.toString());
      message.setToState(stateModel.getInitialState());
      message.setStateModelFactoryName(idealState.getStateModelFactoryName());

      if (idealState.getResourceGroupName() != null) {
        message.setResourceGroupName(idealState.getResourceGroupName());
      }
      if (idealState.getInstanceGroupTag() != null) {
        message.setResourceTag(idealState.getInstanceGroupTag());
      }

      resetMessages.add(message);
      messageKeys.add(keyBuilder.message(instanceName, message.getId()));
    }

    accessor.setChildren(messageKeys, resetMessages);
  }

  @Override
  public void resetInstance(String clusterName, List<String> instanceNames) {
    // TODO: not mp-safe
    logger.info("Reset instances {} in cluster {}.",
        instanceNames == null ? "NULL" : HelixUtil.serializeByComma(instanceNames), clusterName);
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

          if (instanceStateMap.containsKey(instanceName) && instanceStateMap.get(instanceName)
              .equals(HelixDefinedState.ERROR.toString())) {
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
    logger.info("Reset resources {} in cluster {}.",
        resourceNames == null ? "NULL" : HelixUtil.serializeByComma(resourceNames), clusterName);
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
    logger.info("Add cluster {}.", clusterName);
    String root = "/" + clusterName;

    if (_zkClient.exists(root)) {
      if (recreateIfExists) {
        logger.warn("Root directory exists.Cleaning the root directory:" + root);
        _zkClient.deleteRecursively(root);
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
    _zkClient.createPersistent(PropertyPathBuilder.idealState(clusterName));
    // CONFIGURATIONS
    path = PropertyPathBuilder.clusterConfig(clusterName);
    _zkClient.createPersistent(path, true);
    _zkClient.writeData(path, new ZNRecord(clusterName));
    path = PropertyPathBuilder.instanceConfig(clusterName);
    _zkClient.createPersistent(path);
    path = PropertyPathBuilder.resourceConfig(clusterName);
    _zkClient.createPersistent(path);
    // PROPERTY STORE
    path = PropertyPathBuilder.propertyStore(clusterName);
    _zkClient.createPersistent(path);
    // LIVE INSTANCES
    _zkClient.createPersistent(PropertyPathBuilder.liveInstance(clusterName));
    // MEMBER INSTANCES
    _zkClient.createPersistent(PropertyPathBuilder.instance(clusterName));
    // External view
    _zkClient.createPersistent(PropertyPathBuilder.externalView(clusterName));
    // State model definition
    _zkClient.createPersistent(PropertyPathBuilder.stateModelDef(clusterName));

    // controller
    _zkClient.createPersistent(PropertyPathBuilder.controller(clusterName));
    path = PropertyPathBuilder.controllerHistory(clusterName);
    final ZNRecord emptyHistory = new ZNRecord(PropertyType.HISTORY.toString());
    final List<String> emptyList = new ArrayList<String>();
    emptyHistory.setListField(clusterName, emptyList);
    _zkClient.createPersistent(path, emptyHistory);

    path = PropertyPathBuilder.controllerMessage(clusterName);
    _zkClient.createPersistent(path);

    path = PropertyPathBuilder.controllerStatusUpdate(clusterName);
    _zkClient.createPersistent(path);

    path = PropertyPathBuilder.controllerError(clusterName);
    _zkClient.createPersistent(path);
  }

  @Override
  public List<String> getInstancesInCluster(String clusterName) {
    String memberInstancesPath = PropertyPathBuilder.instance(clusterName);
    return _zkClient.getChildren(memberInstancesPath);
  }

  @Override
  public List<String> getInstancesInClusterWithTag(String clusterName, String tag) {
    String memberInstancesPath = PropertyPathBuilder.instance(clusterName);
    List<String> instances = _zkClient.getChildren(memberInstancesPath);
    List<String> result = new ArrayList<String>();

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    for (String instanceName : instances) {
      InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
      if (config == null) {
        throw new IllegalStateException(String
            .format("Instance %s does not have a config, cluster might be in bad state",
                instanceName));
      }
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
  public void addResource(String clusterName, String resourceName, int partitions,
      String stateModelRef, String rebalancerMode, String rebalanceStrategy) {
    addResource(clusterName, resourceName, partitions, stateModelRef, rebalancerMode,
        rebalanceStrategy, 0, -1);
  }

  @Override
  public void addResource(String clusterName, String resourceName, IdealState idealstate) {
    logger.info("Add resource {} in cluster {}.", resourceName, clusterName);
    String stateModelRef = idealstate.getStateModelDefRef();
    String stateModelDefPath = PropertyPathBuilder.stateModelDef(clusterName, stateModelRef);
    if (!_zkClient.exists(stateModelDefPath)) {
      throw new HelixException(
          "State model " + stateModelRef + " not found in the cluster STATEMODELDEFS path");
    }

    String idealStatePath = PropertyPathBuilder.idealState(clusterName);
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
    addResource(clusterName, resourceName, partitions, stateModelRef, rebalancerMode,
        RebalanceStrategy.DEFAULT_REBALANCE_STRATEGY, bucketSize, maxPartitionsPerInstance);
  }

  @Override
  public void addResource(String clusterName, String resourceName, int partitions,
      String stateModelRef, String rebalancerMode, String rebalanceStrategy, int bucketSize,
      int maxPartitionsPerInstance) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    IdealState idealState = new IdealState(resourceName);
    idealState.setNumPartitions(partitions);
    idealState.setStateModelDefRef(stateModelRef);
    RebalanceMode mode =
        idealState.rebalanceModeFromString(rebalancerMode, RebalanceMode.SEMI_AUTO);
    idealState.setRebalanceMode(mode);
    idealState.setRebalanceStrategy(rebalanceStrategy);
    idealState.setReplicas("" + 0);
    idealState.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
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
    return _zkClient.getChildren(PropertyPathBuilder.idealState(clusterName));
  }

  @Override
  public List<String> getResourcesInClusterWithTag(String clusterName, String tag) {
    List<String> resourcesWithTag = new ArrayList<String>();

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    for (String resourceName : getResourcesInCluster(clusterName)) {
      IdealState is = accessor.getProperty(keyBuilder.idealStates(resourceName));
      if (is != null && is.getInstanceGroupTag() != null && is.getInstanceGroupTag().equals(tag)) {
        resourcesWithTag.add(resourceName);
      }
    }

    return resourcesWithTag;
  }

  @Override
  public IdealState getResourceIdealState(String clusterName, String resourceName) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.idealStates(resourceName));
  }

  @Override
  public void setResourceIdealState(String clusterName, String resourceName,
      IdealState idealState) {
    logger
        .info("Set IdealState for resource {} in cluster {} with new IdealState {}.", resourceName,
            clusterName, idealState == null ? "NULL" : idealState.toString());
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
  }

  /**
   * Partially updates the fields appearing in the given IdealState (input).
   * @param clusterName
   * @param resourceName
   * @param idealState
   */
  @Override
  public void updateIdealState(String clusterName, String resourceName, IdealState idealState) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException(
          "updateIdealState failed. Cluster: " + clusterName + " is NOT setup properly.");
    }
    String zkPath = PropertyPathBuilder.idealState(clusterName, resourceName);
    if (!_zkClient.exists(zkPath)) {
      throw new HelixException(String.format(
          "updateIdealState failed. The IdealState for the given resource does not already exist. Resource name: %s",
          resourceName));
    }
    // Update by way of merge
    ZKUtil.createOrUpdate(_zkClient, zkPath, idealState.getRecord(), true, true);
  }

  /**
   * Selectively removes fields appearing in the given IdealState (input) from the IdealState in ZK.
   * @param clusterName
   * @param resourceName
   * @param idealState
   */
  @Override
  public void removeFromIdealState(String clusterName, String resourceName, IdealState idealState) {
    String zkPath = PropertyPathBuilder.idealState(clusterName, resourceName);
    ZKUtil.subtract(_zkClient, zkPath, idealState.getRecord());
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
    addStateModelDef(clusterName, stateModelDef, stateModel, false);
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDef,
      StateModelDefinition stateModel, boolean recreateIfExists) {
    logger
        .info("Add StateModelDef {} in cluster {} with StateModel {}.", stateModelDef, clusterName,
            stateModel == null ? "NULL" : stateModel.toString());
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    String stateModelDefPath = PropertyPathBuilder.stateModelDef(clusterName);
    String stateModelPath = stateModelDefPath + "/" + stateModelDef;
    if (_zkClient.exists(stateModelPath)) {
      if (recreateIfExists) {
        logger.info(
            "Operation.State Model directory exists:" + stateModelPath + ", remove and recreate.");
        _zkClient.deleteRecursively(stateModelPath);
      } else {
        logger.info("Skip the operation. State Model directory exists:" + stateModelPath);
        return;
      }
    }

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef(stateModelDef), stateModel);
  }

  @Override
  public void dropResource(String clusterName, String resourceName) {
    logger.info("Drop resource {} from cluster {}", resourceName, clusterName);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.removeProperty(keyBuilder.idealStates(resourceName));
    accessor.removeProperty(keyBuilder.resourceConfig(resourceName));
  }

  @Override
  public List<String> getStateModelDefs(String clusterName) {
    return _zkClient.getChildren(PropertyPathBuilder.stateModelDef(clusterName));
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
    logger.info("Deleting cluster {}.", clusterName);
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

    _zkClient.deleteRecursively(root);
  }

  @Override
  public void addClusterToGrandCluster(String clusterName, String grandCluster) {
    logger.info("Add cluster {} to grand cluster {}.", clusterName, grandCluster);
    if (!ZKUtil.isClusterSetup(grandCluster, _zkClient)) {
      throw new HelixException("Grand cluster " + grandCluster + " is not setup yet");
    }

    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("Cluster " + clusterName + " is not setup yet");
    }

    IdealState idealState = new IdealState(clusterName);

    idealState.setNumPartitions(1);
    idealState.setStateModelDefRef("LeaderStandby");

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

    accessor.setProperty(keyBuilder.idealStates(idealState.getResourceName()), idealState);
  }

  @Override
  public void setConfig(HelixConfigScope scope, Map<String, String> properties) {
    logger.info("Set configs with keys ");
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
  public void rebalance(String clusterName, String resourceName, int replica,
      List<String> instances) {
    rebalance(clusterName, resourceName, replica, resourceName, instances, "");
  }

  void rebalance(String clusterName, String resourceName, int replica, String keyPrefix,
      List<String> instanceNames, String groupId) {
    logger.info("Rebalance resource {} with replica {} in cluster {}.", resourceName, replica,
        clusterName);
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
    String stateModelName = idealState.getStateModelDefRef();
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
    if (idealState.getRebalanceMode() != RebalanceMode.FULL_AUTO
        && idealState.getRebalanceMode() != RebalanceMode.USER_DEFINED) {
      ZNRecord newIdealState = DefaultIdealStateCalculator
          .calculateIdealState(instanceNames, partitions, replica, keyPrefix, masterStateValue,
              slaveStateValue);

      // for now keep mapField in SEMI_AUTO mode and remove listField in CUSTOMIZED mode
      if (idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
        idealState.getRecord().setListFields(newIdealState.getListFields());
        // TODO: need consider to remove this.
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
    logger.info("Add IdealState for resource {} to cluster {} by file name {}.", resourceName,
        clusterName, idealStateFile);
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
    addStateModelDef(clusterName, stateModelDefName, new StateModelDefinition(record), false);
  }

  @Override
  public void setConstraint(String clusterName, final ConstraintType constraintType,
      final String constraintId, final ConstraintItem constraintItem) {
    logger.info("Set constraint type {} with constraint id {} for cluster {}.", constraintType,
        constraintId, clusterName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    Builder keyBuilder = new Builder(clusterName);
    String path = keyBuilder.constraint(constraintType.toString()).getPath();

    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        ClusterConstraints constraints = currentData == null ?
            new ClusterConstraints(constraintType) :
            new ClusterConstraints(currentData);

        constraints.addConstraintItem(constraintId, constraintItem);
        return constraints.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void removeConstraint(String clusterName, final ConstraintType constraintType,
      final String constraintId) {
    logger.info("Remove constraint type {} with constraint id {} for cluster {}.", constraintType,
        constraintId, clusterName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    Builder keyBuilder = new Builder(clusterName);
    String path = keyBuilder.constraint(constraintType.toString()).getPath();

    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData != null) {
          ClusterConstraints constraints = new ClusterConstraints(currentData);

          constraints.removeConstraintItem(constraintId);
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
   * Takes the existing idealstate as input and computes newIdealState such that the partition
   * movement is minimized. The partitions are redistributed among the instances provided.
   *
   * @param clusterName
   * @param currentIdealState
   * @param instanceNames
   * @return
   */
  @Override
  public void rebalance(String clusterName, IdealState currentIdealState,
      List<String> instanceNames) {
    logger.info("Rebalance resource {} in cluster {} with IdealState {}.",
        currentIdealState.getResourceName(), clusterName,
        currentIdealState == null ? "NULL" : currentIdealState.toString());
    Set<String> activeInstances = new HashSet<String>();
    for (String partition : currentIdealState.getPartitionSet()) {
      activeInstances.addAll(currentIdealState.getRecord().getListField(partition));
    }
    instanceNames.removeAll(activeInstances);
    Map<String, Object> previousIdealState =
        RebalanceUtil.buildInternalIdealState(currentIdealState);

    Map<String, Object> balancedRecord =
        DefaultIdealStateCalculator.calculateNextIdealState(instanceNames, previousIdealState);
    StateModelDefinition stateModDef =
        this.getStateModelDef(clusterName, currentIdealState.getStateModelDefRef());

    if (stateModDef == null) {
      throw new HelixException(
          "cannot find state model: " + currentIdealState.getStateModelDefRef());
    }
    String[] states = RebalanceUtil.parseStates(clusterName, stateModDef);

    ZNRecord newIdealStateRecord = DefaultIdealStateCalculator
        .convertToZNRecord(balancedRecord, currentIdealState.getResourceName(), states[0],
            states[1]);
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
        newIdealStateRecord.getMapFields()
            .put(originPartitionName, newIdealStateRecord.getMapField(partition));
        newIdealStateRecord.getMapFields().remove(partition);

        newIdealStateRecord.getListFields()
            .put(originPartitionName, newIdealStateRecord.getListField(partition));
        newIdealStateRecord.getListFields().remove(partition);
      }
    }

    newIdealStateRecord.getSimpleFields().putAll(currentIdealState.getRecord().getSimpleFields());
    IdealState newIdealState = new IdealState(newIdealStateRecord);
    setResourceIdealState(clusterName, newIdealStateRecord.getId(), newIdealState);
  }

  @Override
  public void addInstanceTag(String clusterName, String instanceName, String tag) {
    logger
        .info("Add instance tag {} for instance {} in cluster {}.", tag, instanceName, clusterName);
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    if (!ZKUtil.isInstanceSetup(_zkClient, clusterName, instanceName, InstanceType.PARTICIPANT)) {
      throw new HelixException(
          "cluster " + clusterName + " instance " + instanceName + " is not setup yet");
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
    logger.info("Remove instance tag {} for instance {} in cluster {}.", tag, instanceName,
        clusterName);
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    if (!ZKUtil.isInstanceSetup(_zkClient, clusterName, instanceName, InstanceType.PARTICIPANT)) {
      throw new HelixException(
          "cluster " + clusterName + " instance " + instanceName + " is not setup yet");
    }
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    config.removeTag(tag);
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), config);
  }

  @Override
  public void setInstanceZoneId(String clusterName, String instanceName, String zoneId) {
    logger.info("Set instance zoneId {} for instance {} in cluster {}.", zoneId, instanceName,
        clusterName);
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    if (!ZKUtil.isInstanceSetup(_zkClient, clusterName, instanceName, InstanceType.PARTICIPANT)) {
      throw new HelixException(
          "cluster " + clusterName + " instance " + instanceName + " is not setup yet");
    }
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    config.setZoneId(zoneId);
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), config);
  }

  @Override
  public void enableBatchMessageMode(String clusterName, boolean enabled) {
    logger
        .info("{} batch message mode for cluster {}.", enabled ? "Enable" : "Disable", clusterName);
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    ConfigAccessor accessor = new ConfigAccessor(_zkClient);

    ClusterConfig clusterConfig = accessor.getClusterConfig(clusterName);
    clusterConfig.setBatchMessageMode(enabled);
    accessor.setClusterConfig(clusterName, clusterConfig);
  }

  @Override
  public void enableBatchMessageMode(String clusterName, String resourceName, boolean enabled) {
    logger.info("{} batch message mode for resource {} in cluster {}.",
        enabled ? "Enable" : "Disable", resourceName, clusterName);
    // TODO: Change IdealState to ResourceConfig when configs are migrated to ResourceConfig
    IdealState idealState = getResourceIdealState(clusterName, resourceName);
    if (idealState == null) {
      throw new HelixException("Cluster " + clusterName + ", resource: " + resourceName
          + ", ideal-state does not exist");
    }

    idealState.setBatchMessageMode(enabled);
    setResourceIdealState(clusterName, resourceName, idealState);
  }

  private void enableSingleInstance(final String clusterName, final String instanceName,
      final boolean enabled, BaseDataAccessor<ZNRecord> baseAccessor) {
    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);

    if (!baseAccessor.exists(path, 0)) {
      throw new HelixException("Cluster " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    baseAccessor.update(path, new DataUpdater<ZNRecord>()

    {
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

  private void enableBatchInstances(final String clusterName, final List<String> instances,
      final boolean enabled, BaseDataAccessor<ZNRecord> baseAccessor) {
    // TODO : Due to Espresso storage node depends on map field. Current disable the feature now
    // include tests.
    if (true) {
      throw new HelixException("Current batch enable/disable instances are temporarily disabled!");
    }

    String path = PropertyPathBuilder.clusterConfig(clusterName);

    if (!baseAccessor.exists(path, 0)) {
      throw new HelixException("Cluster " + clusterName + ": cluster config does not exist");
    }

    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + clusterName + ": cluster config is null");
        }

        ClusterConfig clusterConfig = new ClusterConfig(currentData);
        Map<String, String> disabledInstances = new TreeMap<>();
        if (clusterConfig.getDisabledInstances() != null) {
          disabledInstances.putAll(clusterConfig.getDisabledInstances());
        }

        if (enabled) {
          disabledInstances.keySet().removeAll(instances);
        } else {
          for (String disabledInstance : instances) {
            if (!disabledInstances.containsKey(disabledInstance)) {
              disabledInstances.put(disabledInstance, String.valueOf(System.currentTimeMillis()));
            }
          }
        }
        clusterConfig.setDisabledInstances(disabledInstances);

        return clusterConfig.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public Map<String, String> getBatchDisabledInstances(String clusterName) {
    ConfigAccessor accessor = new ConfigAccessor(_zkClient);
    return accessor.getClusterConfig(clusterName).getDisabledInstances();
  }

  @Override
  public List<String> getInstancesByDomain(String clusterName, String domain) {
    List<String> instances = new ArrayList<>();
    String path = PropertyPathBuilder.instanceConfig(clusterName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_zkClient);
    List<ZNRecord> znRecords = baseAccessor.getChildren(path, null, 0);
    for (ZNRecord record : znRecords) {
      if (record != null) {
        InstanceConfig instanceConfig = new InstanceConfig(record);
        if (instanceConfig.isInstanceInDomain(domain)) {
          instances.add(instanceConfig.getInstanceName());
        }
      }
    }
    return instances;
  }

  @Override
  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
    }
  }

}
