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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
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
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.api.exceptions.HelixConflictException;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.api.topology.ClusterTopology;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ClusterStatus;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.CustomizedView;
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
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.apache.helix.util.ConfigStringUtil;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.InstanceUtil;
import org.apache.helix.util.RebalanceUtil;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.NetworkUtil;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZKHelixAdmin implements HelixAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(ZKHelixAdmin.class);

  public static final String CONNECTION_TIMEOUT = "helixAdmin.timeOutInSec";
  private static final String MAINTENANCE_ZNODE_ID = "maintenance";
  private static final int DEFAULT_SUPERCLUSTER_REPLICA = 3;
  private static final ImmutableSet<InstanceConstants.InstanceOperation>
      INSTANCE_OPERATION_TO_EXCLUDE_FROM_ASSIGNMENT =
      ImmutableSet.of(InstanceConstants.InstanceOperation.EVACUATE,
          InstanceConstants.InstanceOperation.UNKNOWN);

  private final RealmAwareZkClient _zkClient;
  private final ConfigAccessor _configAccessor;
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;
  // true if ZKHelixAdmin was instantiated with a RealmAwareZkClient, false otherwise
  // This is used for close() to determine how ZKHelixAdmin should close the underlying ZkClient
  private final boolean _usesExternalZkClient;

  private static Logger logger = LoggerFactory.getLogger(ZKHelixAdmin.class);

  /**
   * @deprecated it is recommended to use the builder constructor {@link Builder}
   * instead to avoid having to manually create and maintain a RealmAwareZkClient
   * outside of ZKHelixAdmin.
   *
   * @param zkClient A created RealmAwareZkClient
   */
  @Deprecated
  public ZKHelixAdmin(RealmAwareZkClient zkClient) {
    _zkClient = zkClient;
    _configAccessor = new ConfigAccessor(zkClient);
    _baseDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    _usesExternalZkClient = true;
  }

  /**
   * There are 2 realm-aware modes to connect to ZK:
   * 1. if system property {@link SystemPropertyKeys#MULTI_ZK_ENABLED} is set to <code>"true"</code>
   * , or zkAddress is null, it will connect on multi-realm mode;
   * 2. otherwise, it will connect on single-realm mode to the <code>zkAddress</code> provided.
   *
   * @param zkAddress ZK address
   * @exception HelixException if not able to connect on multi-realm mode
   *
   * @deprecated it is recommended to use the builder constructor {@link Builder}
   */
  @Deprecated
  public ZKHelixAdmin(String zkAddress) {
    int timeOutInSec = Integer.parseInt(System.getProperty(CONNECTION_TIMEOUT, "30"));
    RealmAwareZkClient.RealmAwareZkClientConfig clientConfig =
        new RealmAwareZkClient.RealmAwareZkClientConfig()
            .setConnectInitTimeout(timeOutInSec * 1000L)
            .setZkSerializer(new ZNRecordSerializer());

    RealmAwareZkClient zkClient;

    if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED) || zkAddress == null) {
      try {
        zkClient = new FederatedZkClient(
            new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build(), clientConfig);
      } catch (IllegalStateException | InvalidRoutingDataException e) {
        throw new HelixException("Not able to connect on multi-realm mode.", e);
      }
    } else {
      zkClient = SharedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
              clientConfig.createHelixZkClientConfig());
      zkClient.waitUntilConnected(timeOutInSec, TimeUnit.SECONDS);
    }

    _zkClient = zkClient;
    _configAccessor = new ConfigAccessor(_zkClient);
    _baseDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    _usesExternalZkClient = false;
  }

  private ZKHelixAdmin(RealmAwareZkClient zkClient, boolean usesExternalZkClient) {
    _zkClient = zkClient;
    _configAccessor = new ConfigAccessor(_zkClient);
    _baseDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    _usesExternalZkClient = usesExternalZkClient;
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

    List<InstanceConfig> matchingLogicalIdInstances =
        InstanceUtil.findInstancesWithMatchingLogicalId(_baseDataAccessor, clusterName,
            instanceConfig);
    if (matchingLogicalIdInstances.size() > 1) {
      throw new HelixException(
          "There are already more than one instance with the same logicalId in the cluster: "
              + matchingLogicalIdInstances.stream().map(InstanceConfig::getInstanceName)
              .collect(Collectors.joining(", "))
              + " Please make sure there is at most 2 instance with the same logicalId in the cluster.");
    }

    InstanceConstants.InstanceOperation attemptedInstanceOperation =
        instanceConfig.getInstanceOperation().getOperation();
    try {
      InstanceUtil.validateInstanceOperationTransition(_baseDataAccessor, clusterName,
          instanceConfig,
          InstanceConstants.InstanceOperation.UNKNOWN, attemptedInstanceOperation);
    } catch (HelixException e) {
      instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.UNKNOWN);
      logger.error("Failed to add instance " + instanceConfig.getInstanceName() + " to cluster "
          + clusterName + " with instance operation " + attemptedInstanceOperation
          + ". Setting INSTANCE_OPERATION to " + instanceConfig.getInstanceOperation()
          .getOperation()
          + " instead.", e);
    }

    ZKUtil.createChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord());

    _zkClient.createPersistent(PropertyPathBuilder.instanceMessage(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceCurrentState(clusterName, nodeId), true);
    _zkClient
        .createPersistent(PropertyPathBuilder.instanceTaskCurrentState(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceCustomizedState(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceError(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceStatusUpdate(clusterName, nodeId), true);
    _zkClient.createPersistent(PropertyPathBuilder.instanceHistory(clusterName, nodeId), true);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.participantHistory(nodeId), new ParticipantHistory(nodeId));
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

    dropInstancePathsRecursively(clusterName, instanceName);
  }

  private void dropInstancePathsRecursively(String clusterName, String instanceName) {
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
    String instancePath = PropertyPathBuilder.instance(clusterName, instanceName);
    int retryCnt = 0;
    while (true) {
      try {
        _zkClient.deleteRecursivelyAtomic(Arrays.asList(instancePath, instanceConfigPath));
        return;
      } catch (ZkClientException e) {
        if (retryCnt < 3 && e.getCause() instanceof ZkException && e.getCause()
            .getCause() instanceof KeeperException.NotEmptyException) {
          // Racing condition with controller's persisting node history, retryable.
          // We don't need to backoff here as this racing condition only happens once (controller
          // does not repeatedly write instance history)
          logger.warn("Retrying dropping instance {} with exception {}", instanceName,
              e.getCause().getMessage());
          retryCnt++;
        } else {
          String errorMessage =
              "Failed to drop instance: " + instanceName + ". Retry times: " + retryCnt;
          logger.error(errorMessage, e);
          throw new HelixException(errorMessage, e);
        }
      }
    }
  }

  /**
   * Please note that the purge function should only be called when there is no new instance
   * joining happening in the cluster. The reason is that current implementation is not thread safe,
   * meaning that if the offline instance comes online while the purging is ongoing, race
   * condition may happen, and we may have live instance in the cluster without corresponding
   * instance config.
   * TODO: consider using Helix lock to prevent race condition, and make sure zookeeper is ok
   *  with the extra traffic caused by lock.
   */
  @Override
  public void purgeOfflineInstances(String clusterName, long offlineDuration) {
    List<String> failToPurgeInstances = new ArrayList<>();
    findTimeoutOfflineInstances(clusterName, offlineDuration).forEach(instance -> {
      try {
        purgeInstance(clusterName, instance);
      } catch (HelixException e) {
        failToPurgeInstances.add(instance);
      }
    });
    if (failToPurgeInstances.size() > 0) {
      LOG.error("ZKHelixAdmin::purgeOfflineInstances(): failed to drop the following instances: "
          + failToPurgeInstances);
    }
  }

  private void purgeInstance(String clusterName, String instanceName) {
    logger.info("Purge instance {} from cluster {}.", instanceName, clusterName);
    dropInstancePathsRecursively(clusterName, instanceName);
  }

  @Override
  public InstanceConfig getInstanceConfig(String clusterName, String instanceName) {
    logger.info("Get instance config for instance {} from cluster {}.", instanceName, clusterName);
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
    if (!_zkClient.exists(instanceConfigPath)) {
      throw new HelixException(
          "instance" + instanceName + " does not exist in cluster " + clusterName);
    }

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
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

  @Deprecated
  @Override
  public void enableInstance(final String clusterName, final String instanceName,
      final boolean enabled) {
    enableInstance(clusterName, instanceName, enabled, null, null);
  }

  @Deprecated
  @Override
  public void enableInstance(final String clusterName, final String instanceName,
      final boolean enabled, InstanceConstants.InstanceDisabledType disabledType, String reason) {
    logger.info("{} instance {} in cluster {}.", enabled ? "Enable" : "Disable", instanceName,
        clusterName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_zkClient);

    // Eventually we will have all instances' enable/disable information in clusterConfig. Now we
    // update both instanceConfig and clusterConfig in transition period.
    enableSingleInstance(clusterName, instanceName, enabled, baseAccessor, disabledType, reason);
  }

  @Deprecated
  @Override
  public void enableInstance(String clusterName, List<String> instances, boolean enabled) {
    // TODO: batch enable/disable is breaking backward compatibility on instance enable with older library
    // re-enable once batch enable/disable is ready
    if (true) {
      throw new HelixException("Batch enable/disable is not supported");
    }
    //enableInstance(clusterName, instances, enabled, null, null);
  }

  /**
   * Set the InstanceOperation of an instance in the cluster.
   *
   * @param clusterName       The cluster name
   * @param instanceName      The instance name
   * @param instanceOperation The instance operation
   */
  @Override
  public void setInstanceOperation(String clusterName, String instanceName,
      @Nullable InstanceConstants.InstanceOperation instanceOperation) {
    setInstanceOperation(clusterName, instanceName, instanceOperation, null, false);
  }

  /**
   * Set the instanceOperation of and instance with {@link InstanceConstants.InstanceOperation}.
   *
   * @param clusterName       The cluster name
   * @param instanceName      The instance name
   * @param instanceOperation The instance operation type
   * @param reason            The reason for the operation
   */
  @Override
  public void setInstanceOperation(String clusterName, String instanceName,
      @Nullable InstanceConstants.InstanceOperation instanceOperation, String reason) {
    setInstanceOperation(clusterName, instanceName, instanceOperation, reason, false);
  }

  /**
   * Set the instanceOperation of and instance with {@link InstanceConstants.InstanceOperation}.
   *
   * @param clusterName       The cluster name
   * @param instanceName      The instance name
   * @param instanceOperation The instance operation type
   * @param reason            The reason for the operation
   * @param overrideAll       Whether to override all existing instance operations from all other
   *                          instance operations
   */
  @Override
  public void setInstanceOperation(String clusterName, String instanceName,
      @Nullable InstanceConstants.InstanceOperation instanceOperation, String reason,
      boolean overrideAll) {
    InstanceConfig.InstanceOperation instanceOperationObj =
        new InstanceConfig.InstanceOperation.Builder().setOperation(
            instanceOperation == null ? InstanceConstants.InstanceOperation.ENABLE
                : instanceOperation).setReason(reason).setSource(
            overrideAll ? InstanceConstants.InstanceOperationSource.ADMIN
                : InstanceConstants.InstanceOperationSource.USER).build();
    InstanceUtil.setInstanceOperation(_configAccessor, _baseDataAccessor, clusterName, instanceName,
        instanceOperationObj);
  }

  @Override
  public boolean isEvacuateFinished(String clusterName, String instanceName) {
    InstanceConfig config = getInstanceConfig(clusterName, instanceName);
    if (config == null || config.getInstanceOperation().getOperation() !=
        InstanceConstants.InstanceOperation.EVACUATE ) {
      return false;
    }
    return !instanceHasCurrentStateOrMessage(clusterName, instanceName);
  }

  @Override
  public boolean isInstanceDrained(String clusterName, String instanceName) {
    return !instanceHasCurrentStateOrMessage(clusterName, instanceName);
  }

  /**
   * Check to see if swapping between two instances is ready to be completed. Checks: 1. Both
   * instances must be alive. 2. Both instances must only have one session and not be carrying over
   * from a previous session. 3. Both instances must have no pending messages. 4. Both instances
   * cannot have partitions in the ERROR state 5. SwapIn instance must have correct state for all
   * partitions that are currently assigned to the SwapOut instance.
   * TODO: We may want to make this a public API in the future.
   *
   * @param clusterName         The cluster name
   * @param swapOutInstanceName The instance that is being swapped out
   * @param swapInInstanceName  The instance that is being swapped in
   * @return True if the swap is ready to be completed, false otherwise.
   */
  private boolean canCompleteSwap(String clusterName, String swapOutInstanceName,
      String swapInInstanceName) {
    BaseDataAccessor<ZNRecord> baseAccessor = _baseDataAccessor;
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // 1. Check that swap-in instance is live and enabled.
    LiveInstance swapOutLiveInstance =
        accessor.getProperty(keyBuilder.liveInstance(swapOutInstanceName));
    LiveInstance swapInLiveInstance =
        accessor.getProperty(keyBuilder.liveInstance(swapInInstanceName));
    InstanceConfig swapOutInstanceConfig = getInstanceConfig(clusterName, swapOutInstanceName);
    InstanceConfig swapInInstanceConfig = getInstanceConfig(clusterName, swapInInstanceName);
    if (swapInLiveInstance == null) {
      logger.warn(
          "SwapOutInstance {} is {} + {} and SwapInInstance {} is OFFLINE + {} for cluster {}. Swap will"
              + " not complete unless SwapInInstance instance is ONLINE.",
          swapOutInstanceName, swapOutLiveInstance != null ? "ONLINE" : "OFFLINE",
          swapOutInstanceConfig.getInstanceOperation().getOperation(), swapInInstanceName,
          swapInInstanceConfig.getInstanceOperation().getOperation(), clusterName);
      return false;
    }

    // 2. Check that both instances only have one session and are not carrying any over.
    // count number of sessions under CurrentState folder. If it is carrying over from prv session,
    // then there are > 1 session ZNodes.
    List<String> swapOutSessions = baseAccessor.getChildNames(
        PropertyPathBuilder.instanceCurrentState(clusterName, swapOutInstanceName), 0);
    List<String> swapInSessions = baseAccessor.getChildNames(
        PropertyPathBuilder.instanceCurrentState(clusterName, swapInInstanceName), 0);
    if (swapOutSessions.size() > 1 || swapInSessions.size() > 1) {
      logger.warn(
          "SwapOutInstance {} is carrying over from prev session and SwapInInstance {} is carrying over from prev session for cluster {}."
              + " Swap will not complete unless both instances have only one session.",
          swapOutInstanceName, swapInInstanceName, clusterName);
      return false;
    }

    // 3. Check that the swapOutInstance has no pending messages.
    List<Message> swapOutMessages =
        accessor.getChildValues(keyBuilder.messages(swapOutInstanceName), true);
    int swapOutPendingMessageCount = swapOutMessages != null ? swapOutMessages.size() : 0;
    List<Message> swapInMessages =
        accessor.getChildValues(keyBuilder.messages(swapInInstanceName), true);
    int swapInPendingMessageCount = swapInMessages != null ? swapInMessages.size() : 0;
    if ((swapOutLiveInstance != null && swapOutPendingMessageCount > 0)
        || swapInPendingMessageCount > 0) {
      logger.warn(
          "SwapOutInstance {} has {} pending messages and SwapInInstance {} has {} pending messages for cluster {}."
              + " Swap will not complete unless both SwapOutInstance(only when live)"
              + " and SwapInInstance have no pending messages unless.",
          swapOutInstanceName, swapOutPendingMessageCount, swapInInstanceName,
          swapInPendingMessageCount, clusterName);
      return false;
    }

    // 4. If the swap-out instance is not alive or is disabled, we return true without checking
    // the current states on the swap-in instance.
    if (swapOutLiveInstance == null || swapOutInstanceConfig.getInstanceOperation().getOperation()
        .equals(InstanceConstants.InstanceOperation.DISABLE)) {
      return true;
    }

    // 5. Collect a list of all partitions that have a current state on swapOutInstance
    String swapOutLastActiveSession = swapOutLiveInstance.getEphemeralOwner();
    String swapInActiveSession = swapInLiveInstance.getEphemeralOwner();

    // Iterate over all resources with current states on the swapOutInstance
    List<String> swapOutResources = baseAccessor.getChildNames(
        PropertyPathBuilder.instanceCurrentState(clusterName, swapOutInstanceName,
            swapOutLastActiveSession), 0);
    for (String swapOutResource : swapOutResources) {
      // Get the topState and secondTopStates for the stateModelDef used by the resource.
      IdealState idealState = accessor.getProperty(keyBuilder.idealStates(swapOutResource));
      StateModelDefinition stateModelDefinition =
          accessor.getProperty(keyBuilder.stateModelDef(idealState.getStateModelDefRef()));
      String topState = stateModelDefinition.getTopState();
      Set<String> secondTopStates = stateModelDefinition.getSecondTopStates();

      CurrentState swapOutResourceCurrentState = accessor.getProperty(
          keyBuilder.currentState(swapOutInstanceName, swapOutLastActiveSession, swapOutResource));
      CurrentState swapInResourceCurrentState = accessor.getProperty(
          keyBuilder.currentState(swapInInstanceName, swapInActiveSession, swapOutResource));

      // Check to make sure swapInInstance has a current state for the resource
      if (swapInResourceCurrentState == null) {
        logger.warn(
            "SwapOutInstance {} has current state for resource {} but SwapInInstance {} does not for cluster {}."
                + " Swap will not complete unless both instances have current states for all resources.",
            swapOutInstanceName, swapOutResource, swapInInstanceName, clusterName);
        return false;
      }

      // Iterate over all partitions in the swapOutInstance's current state for the resource
      // and ensure that the swapInInstance has the correct state for the partition.
      for (String partitionName : swapOutResourceCurrentState.getPartitionStateMap().keySet()) {
        String swapOutPartitionState = swapOutResourceCurrentState.getState(partitionName);
        String swapInPartitionState = swapInResourceCurrentState.getState(partitionName);

        // SwapInInstance should have the correct state for the partition.
        // All states should match except for the case where the topState is not ALL_REPLICAS or ALL_CANDIDATE_NODES
        // or the swap-out partition is ERROR state.
        // When the topState is not ALL_REPLICAS or ALL_CANDIDATE_NODES, the swap-in partition should be in a secondTopStates.
        if (!(swapOutPartitionState.equals(HelixDefinedState.ERROR.name()) || (
            topState.equals(swapOutPartitionState) && (
                swapOutPartitionState.equals(swapInPartitionState) ||
                    !ImmutableSet.of(StateModelDefinition.STATE_REPLICA_COUNT_ALL_REPLICAS,
                        StateModelDefinition.STATE_REPLICA_COUNT_ALL_CANDIDATE_NODES).contains(
                        stateModelDefinition.getNumInstancesPerState(
                            stateModelDefinition.getTopState())) && secondTopStates.contains(
                        swapInPartitionState))) || swapOutPartitionState.equals(
            swapInPartitionState))) {
          logger.warn(
              "SwapOutInstance {} has partition {} in {} but SwapInInstance {} has partition {} in state {} for cluster {}."
                  + " Swap will not complete unless SwapInInstance has partition in correct states.",
              swapOutInstanceName, partitionName, swapOutPartitionState, swapInInstanceName,
              partitionName, swapInPartitionState, clusterName);
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public boolean canCompleteSwap(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      logger.warn(
          "Instance {} in cluster {} does not exist. Cannot determine if the swap is complete.",
          instanceName, clusterName);
      return false;
    }

    List<InstanceConfig> swappingInstances =
        InstanceUtil.findInstancesWithMatchingLogicalId(_baseDataAccessor, clusterName,
            instanceConfig);
    if (swappingInstances.size() != 1) {
      logger.warn(
          "Instance {} in cluster {} is not swapping with any other instance. Cannot determine if the swap is complete.",
          instanceName, clusterName);
      return false;
    }

    InstanceConfig swapOutInstanceConfig = !instanceConfig.getInstanceOperation().getOperation()
        .equals(InstanceConstants.InstanceOperation.SWAP_IN)
            ? instanceConfig : swappingInstances.get(0);
    InstanceConfig swapInInstanceConfig = instanceConfig.getInstanceOperation().getOperation()
        .equals(InstanceConstants.InstanceOperation.SWAP_IN) ? instanceConfig
        : swappingInstances.get(0);
    if (swapOutInstanceConfig == null || swapInInstanceConfig == null) {
      logger.warn(
          "Instance {} in cluster {} is not swapping with any other instance. Cannot determine if the swap is complete.",
          instanceName, clusterName);
      return false;
    }

    // Check if the swap is ready to be completed.
    return canCompleteSwap(clusterName, swapOutInstanceConfig.getInstanceName(),
        swapInInstanceConfig.getInstanceName());
  }

  @Override
  public boolean completeSwapIfPossible(String clusterName, String instanceName,
      boolean forceComplete) {
    InstanceConfig instanceConfig = getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      logger.warn(
          "Instance {} in cluster {} does not exist. Cannot determine if the swap is complete.",
          instanceName, clusterName);
      return false;
    }

    List<InstanceConfig> swappingInstances =
        InstanceUtil.findInstancesWithMatchingLogicalId(_baseDataAccessor, clusterName,
            instanceConfig);
    if (swappingInstances.size() != 1) {
      logger.warn(
          "Instance {} in cluster {} is not swapping with any other instance. Cannot determine if the swap is complete.",
          instanceName, clusterName);
      return false;
    }

    InstanceConfig swapOutInstanceConfig = !instanceConfig.getInstanceOperation().getOperation()
        .equals(InstanceConstants.InstanceOperation.SWAP_IN)
            ? instanceConfig : swappingInstances.get(0);
    InstanceConfig swapInInstanceConfig = instanceConfig.getInstanceOperation().getOperation()
        .equals(InstanceConstants.InstanceOperation.SWAP_IN) ? instanceConfig
        : swappingInstances.get(0);
    if (swapOutInstanceConfig == null || swapInInstanceConfig == null) {
      logger.warn(
          "Instance {} in cluster {} is not swapping with any other instance. Cannot determine if the swap is complete.",
          instanceName, clusterName);
      return false;
    }

    // Check if the swap is ready to be completed. If not, return false.
    if (forceComplete || !canCompleteSwap(clusterName, swapOutInstanceConfig.getInstanceName(),
        swapInInstanceConfig.getInstanceName())) {
      return false;
    }

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_zkClient);
    String swapInInstanceConfigPath =
        PropertyPathBuilder.instanceConfig(clusterName, swapInInstanceConfig.getInstanceName());
    String swapOutInstanceConfigPath =
        PropertyPathBuilder.instanceConfig(clusterName, swapOutInstanceConfig.getInstanceName());

    Map<String, DataUpdater<ZNRecord>> updaterMap = new HashMap<>();
    updaterMap.put(swapInInstanceConfigPath, currentData -> {
      if (currentData == null) {
        throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
            + ", SWAP_IN instance config is null");
      }

      InstanceConfig currentSwapOutInstanceConfig =
          getInstanceConfig(clusterName, swapOutInstanceConfig.getInstanceName());
      InstanceConfig config = new InstanceConfig(currentData);
      config.overwriteInstanceConfig(currentSwapOutInstanceConfig);
      // Special handling in case the swap-out instance does not have HELIX_ENABLED or InstanceOperation set.
      return config.getRecord();
    });

    updaterMap.put(swapOutInstanceConfigPath, currentData -> {
      if (currentData == null) {
        throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
            + ", swap out instance config is null");
      }

      InstanceConfig config = new InstanceConfig(currentData);
      config.setInstanceOperation(InstanceConstants.InstanceOperation.UNKNOWN);
      return config.getRecord();
    });

    return baseAccessor.multiSet(updaterMap);
  }

  @Override
  public boolean isReadyForPreparingJoiningCluster(String clusterName, String instanceName) {
    if (!instanceHasCurrentStateOrMessage(clusterName, instanceName)) {
      InstanceConfig config = getInstanceConfig(clusterName, instanceName);
      return config != null && INSTANCE_OPERATION_TO_EXCLUDE_FROM_ASSIGNMENT.contains(
          config.getInstanceOperation().getOperation());
    }
    return false;
  }

  @Override
  public boolean forceKillInstance(String clusterName, String instanceName) {
    return forceKillInstance(clusterName, instanceName, "Force kill instance", null);
  }

  @Override
  public boolean forceKillInstance(String clusterName, String instanceName, String reason,
      InstanceConstants.InstanceOperationSource operationSource) {
    logger.info("Force kill instance {} in cluster {}.", instanceName, clusterName);

    InstanceConfig.InstanceOperation instanceOperationObj = new InstanceConfig.InstanceOperation.Builder()
        .setOperation(InstanceConstants.InstanceOperation.UNKNOWN).setReason(reason)
        .setSource(operationSource).build();
    InstanceConfig instanceConfig = getInstanceConfig(clusterName, instanceName);
    instanceConfig.setInstanceOperation(instanceOperationObj);

    // Set instance operation to unknown and delete live instance in one operation
    List<Op> operations = Arrays.asList(
      Op.setData(PropertyPathBuilder.instanceConfig(clusterName, instanceName),
          _zkClient.serialize(instanceConfig.getRecord(),
          PropertyPathBuilder.instanceConfig(clusterName, instanceName)), -1),
      Op.delete(PropertyPathBuilder.liveInstance(clusterName, instanceName), -1));

    List< OpResult> opResults = _zkClient.multi(operations);
    return opResults.stream().noneMatch(result -> result instanceof OpResult.ErrorResult);
  }

  /**
   * Returns true if instance has more than one session or for online instance if it has messages or
   * FULL_AUTO and CUSTOMIZED resources in current states, false otherwise. For offline instances,
   * returns false if all CUSTOMIZED resources in current states are migrated to other instances
   * in ideal state, true otherwise.
   * @param clusterName
   * @param instanceName
   * @return
   */
  private boolean instanceHasCurrentStateOrMessage(String clusterName,
      String instanceName) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // check the instance is alive
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));

    BaseDataAccessor<ZNRecord> baseAccessor = _baseDataAccessor;
    // count number of sessions under CurrentState folder. If it is carrying over from prv session,
    // then there are > 1 session ZNodes.
    List<String> sessions = baseAccessor.getChildNames(PropertyPathBuilder.instanceCurrentState(clusterName, instanceName), 0);
    if (sessions.isEmpty()) {
      logger.info("Instance {} in cluster {} does not have any session.  The instance can be removed anyway.",
          instanceName, clusterName);
      return false;
    }
    if (sessions.size() > 1) {
      logger.info("Instance {} in cluster {} is carrying over from prev session.", instanceName,
          clusterName);
      return true;
    }

    String sessionId = sessions.get(0);
    List<CurrentState> currentStates = accessor.getChildValues(keyBuilder.currentStates(instanceName, sessionId), true);
    if (currentStates == null || currentStates.isEmpty()) {
      logger.info("Instance {} in cluster {} does not have any current state.",
          instanceName, clusterName);
      return false;
    }

    List<IdealState> idealStates = accessor.getChildValues(keyBuilder.idealStates(), true);
    if (liveInstance == null) {
      boolean customizedResourcesReassigned =
          areAllCustomizedResourcesReassigned(currentStates, idealStates, instanceName);
      logger.info("check for customizedResourcesReassigned for instance {} in cluster {} returned {}",
          instanceName, clusterName, customizedResourcesReassigned);
      return !customizedResourcesReassigned;
    }

    // see if instance has pending message.
    List<String> messages = accessor.getChildNames(keyBuilder.messages(instanceName));
    if (messages != null && !messages.isEmpty()) {
      logger.info("Instance {} in cluster {} has pending messages.", instanceName, clusterName);
      return true;
    }
    // Get set of FULL_AUTO and CUSTOMIZED resources
    Set<String> resources = idealStates != null ? idealStates.stream()
        .filter(idealState -> idealState.getRebalanceMode() == RebalanceMode.FULL_AUTO ||
            idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED)
        .map(IdealState::getResourceName).collect(Collectors.toSet()) : Collections.emptySet();

    return currentStates.stream().map(CurrentState::getResourceName).anyMatch(resources::contains);
  }

  /**
   * Returns true if, for all customized resources present in the given current states every partition is now assigned
   * to a different instance (i.e., not instanceName) in the IdealState.
   *
   * @param currentStates  list of CurrentState objects of instanceName representing the current assignment of
   *                       resources on the instance
   * @param idealStates    list of IdealState objects representing the desired assignment of resources
   * @param instanceName   the instance from which resources are migrated
   * @return
   */
  private boolean areAllCustomizedResourcesReassigned(List<CurrentState> currentStates,
      List<IdealState> idealStates, String instanceName) {
    // Step 1: Create a map of resourceName -> CurrentState
    Map<String, CurrentState> currentStateMap = currentStates.stream()
        .collect(Collectors.toMap(CurrentState::getResourceName, cs -> cs));

    // Step 2: Filter ideal states that are CUSTOMIZED and present in currentStates
    List<IdealState> customizedIdealStates = idealStates.stream()
        .filter(is -> is.getRebalanceMode() == RebalanceMode.CUSTOMIZED &&
            currentStateMap.containsKey(is.getResourceName()))
        .collect(Collectors.toList());

    // Step 3: Check if any partition of any customized resource is still assigned to the instance
    for (IdealState idealState : customizedIdealStates) {
      String resourceName = idealState.getResourceName();
      CurrentState cs = currentStateMap.get(resourceName);

      for (String partition : cs.getPartitionStateMap().keySet()) {
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(partition);
        if (instanceStateMap == null) {
          continue;
        }

        for (String assignedInstance : instanceStateMap.keySet()) {
          if (instanceName.equals(assignedInstance)) {
            return false; // Partition still assigned to the given instance
          }
        }
      }
    }
    return true; // All customized resource partitions have been migrated off this instance
  }

  @Override
  public void enableResource(final String clusterName, final String resourceName, final boolean enabled) {
    logger.info("{} resource {} in cluster {}.", enabled ? "Enable" : "Disable", resourceName, clusterName);
    String path = PropertyPathBuilder.idealState(clusterName, resourceName);
    BaseDataAccessor<ZNRecord> baseAccessor = _baseDataAccessor;
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

    BaseDataAccessor<ZNRecord> baseAccessor = _baseDataAccessor;

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
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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
  public boolean isInMaintenanceMode(String clusterName) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getBaseDataAccessor()
        .exists(keyBuilder.maintenance().getPath(), AccessOption.PERSISTENT);
  }

  @Override
  public void setClusterManagementMode(ClusterManagementModeRequest request) {
    ClusterManagementMode.Type mode = request.getMode();
    String clusterName = request.getClusterName();
    String reason = request.getReason();

    // TODO: support other modes
    switch (mode) {
      case CLUSTER_FREEZE:
        enableClusterPauseMode(clusterName, request.isCancelPendingST(), reason);
        break;
      case NORMAL:
        // If from other modes, should check what mode it is in and call the api accordingly.
        // If we put all mode config in one znode, one generic method is good enough.
        disableClusterPauseMode(clusterName);
        break;
      default:
        throw new IllegalArgumentException("ClusterManagementMode " + mode + " is not supported");
    }
  }

  @Override
  public ClusterManagementMode getClusterManagementMode(String clusterName) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    ClusterStatus status = accessor.getProperty(accessor.keyBuilder().clusterStatus());
    return status == null ? null
        : new ClusterManagementMode(status.getManagementMode(), status.getManagementModeStatus());
  }

  @Override
  public void setPartitionsToError(String clusterName, String instanceName, String resourceName,
      List<String> partitionNames) {
    logger.info("Set partitions {} for resource {} on instance {} in cluster {} to ERROR state.",
        partitionNames == null ? "NULL" : HelixUtil.serializeByComma(partitionNames), resourceName,
        instanceName, clusterName);
    sendStateTransitionMessage(clusterName, instanceName, resourceName, partitionNames,
        StateTransitionType.SET_TO_ERROR);
  }

  private void sendStateTransitionMessage(String clusterName, String instanceName,
      String resourceName, List<String> partitionNames, StateTransitionType stateTransitionType) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // check the instance is alive
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      // check if the instance exists in the cluster
      String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
      throw new HelixException(String.format(
          (_zkClient.exists(instanceConfigPath) ? SetPartitionFailureReason.INSTANCE_NOT_ALIVE
              : SetPartitionFailureReason.INSTANCE_NON_EXISTENT).getMessage(resourceName,
                  partitionNames, instanceName, instanceName, clusterName, stateTransitionType)));
    }

    // check resource exists in ideal state
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    if (idealState == null) {
      throw new HelixException(
          String.format(SetPartitionFailureReason.RESOURCE_NON_EXISTENT.getMessage(resourceName,
              partitionNames, instanceName, resourceName, clusterName, stateTransitionType)));
    }

    // check partition exists in resource
    Set<String> partitionsNames = new HashSet<String>(partitionNames);
    Set<String> partitions = (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED)
        ? idealState.getRecord().getMapFields().keySet()
        : idealState.getRecord().getListFields().keySet();
    if (!partitions.containsAll(partitionsNames)) {
      throw new HelixException(
          String.format(SetPartitionFailureReason.PARTITION_NON_EXISTENT.getMessage(resourceName,
              partitionNames, instanceName, partitionNames.toString(), clusterName, stateTransitionType)));
    }

    // check partition is in ERROR state if reset is set to True
    String sessionId = liveInstance.getEphemeralOwner();
    CurrentState curState =
        accessor.getProperty(keyBuilder.currentState(instanceName, sessionId, resourceName));
    if (stateTransitionType.equals(StateTransitionType.RESET)) {
      for (String partitionName : partitionNames) {
        if (!curState.getState(partitionName).equals(HelixDefinedState.ERROR.toString())) {
          throw new HelixException(String.format(
              SetPartitionFailureReason.PARTITION_NOT_ERROR.getMessage(resourceName, partitionNames,
                  instanceName, partitionNames.toString(), clusterName, stateTransitionType)));
        }
      }
    }

    // check stateModelDef exists
    String stateModelDef = idealState.getStateModelDefRef();
    StateModelDefinition stateModel = accessor.getProperty(keyBuilder.stateModelDef(stateModelDef));
    if (stateModel == null) {
      throw new HelixException(
          String.format(SetPartitionFailureReason.STATE_MODEL_NON_EXISTENT.getMessage(resourceName,
              partitionNames, instanceName, stateModelDef, clusterName, stateTransitionType)));
    }

    // check there is no pending messages for the partitions exist
    List<Message> messages = accessor.getChildValues(keyBuilder.messages(instanceName), true);
    for (Message message : messages) {
      if (!MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType())
          || !sessionId.equals(message.getTgtSessionId())
          || !resourceName.equals(message.getResourceName())
          || !partitionsNames.contains(message.getPartitionName())) {
        continue;
      }

      throw new HelixException(String.format(
          "Can't %s state for %s.%s on %s, because a pending message %s exists for resource %s",
          stateTransitionType.name(), resourceName, partitionNames, instanceName, message,
          message.getResourceName()));
    }

    String adminName = null;
    try {
      adminName = InetAddress.getLocalHost().getCanonicalHostName() + "-ADMIN";
    } catch (UnknownHostException e) {
      logger.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e);
      adminName = "UNKNOWN";
    }

    List<Message> stateTransitionMessages = new ArrayList<Message>();
    List<PropertyKey> messageKeys = new ArrayList<PropertyKey>();
    for (String partitionName : partitionNames) {
      String msgId = UUID.randomUUID().toString();
      Message message = new Message(MessageType.STATE_TRANSITION, msgId);
      message.setSrcName(adminName);
      message.setTgtName(instanceName);
      message.setMsgState(MessageState.NEW);
      message.setPartitionName(partitionName);
      message.setResourceName(resourceName);
      message.setTgtSessionId(sessionId);
      message.setStateModelDef(stateModelDef);
      message.setStateModelFactoryName(idealState.getStateModelFactoryName());
      // if reset == TRUE, send ERROR to initialState message
      // else, send * to ERROR state message
      if (stateTransitionType.equals(StateTransitionType.RESET)) {
        message.setFromState(HelixDefinedState.ERROR.toString());
        message.setToState(stateModel.getInitialState());
      }
      if (stateTransitionType.equals(StateTransitionType.SET_TO_ERROR)) {
        message.setFromState("*");
        message.setToState(HelixDefinedState.ERROR.toString());
      }
      if (idealState.getResourceGroupName() != null) {
        message.setResourceGroupName(idealState.getResourceGroupName());
      }
      if (idealState.getInstanceGroupTag() != null) {
        message.setResourceTag(idealState.getInstanceGroupTag());
      }

      stateTransitionMessages.add(message);
      messageKeys.add(keyBuilder.message(instanceName, message.getId()));
    }

    accessor.setChildren(messageKeys, stateTransitionMessages);
  }

  private void enableClusterPauseMode(String clusterName, boolean cancelPendingST, String reason) {
    String hostname = NetworkUtil.getLocalhostName();
    logger.info(
        "Enable cluster pause mode for cluster: {}. CancelPendingST: {}. Reason: {}. From Host: {}",
        clusterName, cancelPendingST, reason, hostname);

    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(_zkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseDataAccessor);

    if (baseDataAccessor.exists(accessor.keyBuilder().pause().getPath(), AccessOption.PERSISTENT)) {
      throw new HelixConflictException(clusterName + " pause signal already exists");
    }

    // check whether cancellation is enabled
    ClusterConfig config = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    if (cancelPendingST && !config.isStateTransitionCancelEnabled()) {
      throw new HelixConflictException(
          "State transition cancellation not enabled in " + clusterName);
    }

    PauseSignal pauseSignal = new PauseSignal();
    pauseSignal.setClusterPause(true);
    pauseSignal.setCancelPendingST(cancelPendingST);
    pauseSignal.setFromHost(hostname);
    pauseSignal.setTriggerTime(Instant.now().toEpochMilli());
    if (reason != null && !reason.isEmpty()) {
      pauseSignal.setReason(reason);
    }
    // TODO: merge management status signal into one znode to avoid race condition
    if (!accessor.createPause(pauseSignal)) {
      throw new HelixException("Failed to create pause signal");
    }
  }

  private void disableClusterPauseMode(String clusterName) {
    logger.info("Disable cluster pause mode for cluster: {}", clusterName);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    PropertyKey pausePropertyKey = accessor.keyBuilder().pause();
    PauseSignal pauseSignal = accessor.getProperty(pausePropertyKey);
    if (pauseSignal == null || !pauseSignal.isClusterPause()) {
      throw new HelixException("Cluster pause mode is not enabled for cluster " + clusterName);
    }

    if (!accessor.removeProperty(pausePropertyKey)) {
      throw new HelixException("Failed to disable cluster pause mode for cluster: " + clusterName);
    }
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
  private void processMaintenanceMode(String clusterName, final boolean enabled,
      final String reason, final MaintenanceSignal.AutoTriggerReason internalReason,
      final Map<String, String> customFields,
      final MaintenanceSignal.TriggeringEntity triggeringEntity) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    logger.info("Cluster {} {} {} maintenance mode for reason {}.", clusterName,
        triggeringEntity == MaintenanceSignal.TriggeringEntity.CONTROLLER ? "automatically"
            : "manually", enabled ? "enters" : "exits", reason == null ? "NULL" : reason);
    final long currentTime = System.currentTimeMillis();
    if (!enabled) {
      // Exit maintenance mode
      accessor.removeProperty(keyBuilder.maintenance());
    } else {
      // Enter maintenance mode
      MaintenanceSignal maintenanceSignal = new MaintenanceSignal(MAINTENANCE_ZNODE_ID);
      if (reason != null) {
        maintenanceSignal.setReason(reason);
      }
      maintenanceSignal.setTimestamp(currentTime);
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
        throw new HelixException("Failed to create maintenance signal!");
      }
    }

    // Record a MaintenanceSignal history
    if (!accessor.getBaseDataAccessor()
        .update(keyBuilder.controllerLeaderHistory().getPath(),
            (DataUpdater<ZNRecord>) oldRecord -> {
              try {
                if (oldRecord == null) {
                  oldRecord = new ZNRecord(PropertyType.HISTORY.toString());
                }
                return new ControllerHistory(oldRecord)
                    .updateMaintenanceHistory(enabled, reason, currentTime, internalReason,
                        customFields, triggeringEntity);
              } catch (IOException e) {
                logger.error("Failed to update maintenance history! Exception: {}", e);
                return oldRecord;
              }
            }, AccessOption.PERSISTENT)) {
      logger.error("Failed to write maintenance history to ZK!");
    }
  }

  private enum SetPartitionFailureReason {
    INSTANCE_NOT_ALIVE("%s is not alive in cluster %s"),
    INSTANCE_NON_EXISTENT("%s does not exist in cluster %s"),
    RESOURCE_NON_EXISTENT("resource %s is not added to cluster %s"),
    PARTITION_NON_EXISTENT("not all %s exist in cluster %s"),
    PARTITION_NOT_ERROR("%s is NOT found in cluster %s or not in ERROR state"),
    STATE_MODEL_NON_EXISTENT("%s is NOT found in cluster %s");

    private String message;

    SetPartitionFailureReason(String message) {
      this.message = message;
    }

    public String getMessage(String resourceName, List<String> partitionNames, String instanceName,
        String errorStateEntity, String clusterName, StateTransitionType stateTransitionType) {
      return String.format("Can't %s state for %s.%s on %s, because " + message,
          stateTransitionType.name(), resourceName, partitionNames, instanceName, errorStateEntity,
          clusterName);
    }
  }

  private enum StateTransitionType {
    // sets state from ERROR to INIT.
    RESET,
    // sets state from ANY to ERROR.
    SET_TO_ERROR,
    // Unknown StateTransitionType
    UNDEFINED
  }
  @Override
  public void resetPartition(String clusterName, String instanceName, String resourceName,
      List<String> partitionNames) {
    logger.info("Reset partitions {} for resource {} on instance {} in cluster {}.",
        partitionNames == null ? "NULL" : HelixUtil.serializeByComma(partitionNames), resourceName,
        instanceName, clusterName);
    sendStateTransitionMessage(clusterName, instanceName, resourceName, partitionNames, StateTransitionType.RESET);
  }

  @Override
  public void resetInstance(String clusterName, List<String> instanceNames) {
    // TODO: not mp-safe
    logger.info("Reset instances {} in cluster {}.",
        instanceNames == null ? "NULL" : HelixUtil.serializeByComma(instanceNames), clusterName);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<ExternalView> extViews = accessor.getChildValues(keyBuilder.externalViews(), true);

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
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<ExternalView> extViews = accessor.getChildValues(keyBuilder.externalViews(), true);

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
    path = PropertyPathBuilder.customizedStateConfig(clusterName);
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

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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
    List<String> zkToplevelPaths;

    if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED)
        || _zkClient instanceof FederatedZkClient) {
      // If on multi-zk mode, we retrieve cluster information from Metadata Store Directory Service.
      Map<String, List<String>> realmToShardingKeys;
      String routingDataSourceEndpoint =
          _zkClient.getRealmAwareZkConnectionConfig().getRoutingDataSourceEndpoint();
      if (routingDataSourceEndpoint == null || routingDataSourceEndpoint.isEmpty()) {
        // If endpoint is not given explicitly, use HTTP and the endpoint set in System Properties
        realmToShardingKeys = RoutingDataManager.getInstance().getRawRoutingData();
      } else {
        realmToShardingKeys = RoutingDataManager.getInstance().getRawRoutingData(
            RoutingDataReaderType
                .lookUp(_zkClient.getRealmAwareZkConnectionConfig().getRoutingDataSourceType()),
            routingDataSourceEndpoint);
      }

      if (realmToShardingKeys == null || realmToShardingKeys.isEmpty()) {
        return Collections.emptyList();
      }
      // Preceding "/"s are removed: e.g.) "/CLUSTER-SHARDING-KEY" -> "CLUSTER-SHARDING-KEY"
      zkToplevelPaths = realmToShardingKeys.values().stream().flatMap(List::stream)
          .map(shardingKey -> shardingKey.substring(1)).collect(Collectors.toList());
    } else {
      // single-zk mode
      zkToplevelPaths = _zkClient.getChildren("/");
    }

    List<String> result = new ArrayList<>();
    for (String pathName : zkToplevelPaths) {
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

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.idealStates(resourceName));
  }

  @Override
  public void setResourceIdealState(String clusterName, String resourceName,
      IdealState idealState) {
    logger
        .info("Set IdealState for resource {} in cluster {} with new IdealState {}.", resourceName,
            clusterName, idealState == null ? "NULL" : idealState.toString());
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.externalView(resourceName));
  }

  @Override
  public CustomizedView getResourceCustomizedView(String clusterName, String resourceName,
      String customizedStateType) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.customizedView(customizedStateType, resourceName));
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

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef(stateModelDef), stateModel);
  }

  @Override
  public void dropResource(String clusterName, String resourceName) {
    logger.info("Drop resource {} from cluster {}", resourceName, clusterName);
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("Cluster " + clusterName + " is not setup yet");
    }
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    accessor.removeProperty(keyBuilder.idealStates(resourceName));
    accessor.removeProperty(keyBuilder.resourceConfig(resourceName));
  }

  @Override
  public void addCloudConfig(String clusterName, CloudConfig cloudConfig) {
    logger.info("Add CloudConfig to cluster {}, CloudConfig is {}.", clusterName,
        cloudConfig.toString());

    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    CloudConfig.Builder builder = new CloudConfig.Builder(cloudConfig);
    CloudConfig cloudConfigBuilder = builder.build();

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.cloudConfig(), cloudConfigBuilder);
  }

  @Override
  public void removeCloudConfig(String clusterName) {
    logger.info("Remove Cloud Config for cluster {}.", clusterName);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.removeProperty(keyBuilder.cloudConfig());
  }

  @Override
  public ClusterTopology getClusterTopology(String clusterName) {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    String path = PropertyPathBuilder.instanceConfig(clusterName);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_zkClient);
    List<ZNRecord> znRecords = baseAccessor.getChildren(path, null, 0, 0, 0);
    for (ZNRecord record : znRecords) {
      if (record != null) {
        InstanceConfig instanceConfig = new InstanceConfig(record);
        instanceConfigMap.put(instanceConfig.getInstanceName(), instanceConfig);
      }
    }
    path = PropertyPathBuilder.liveInstance(clusterName);
    List<String> liveNodes = baseAccessor.getChildNames(path, 0);
    ConfigAccessor configAccessor = new ConfigAccessor(_zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    return new ClusterTopology(liveNodes, instanceConfigMap, clusterConfig);
  }

  @Override
  public List<String> getStateModelDefs(String clusterName) {
    return _zkClient.getChildren(PropertyPathBuilder.stateModelDef(clusterName));
  }

  @Override
  public StateModelDefinition getStateModelDef(String clusterName, String stateModelName) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.stateModelDef(stateModelName));
  }

  @Override
  public void dropCluster(String clusterName) {
    logger.info("Deleting cluster {}.", clusterName);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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
    idealState.setRebalanceMode(RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    // TODO: Give user an option, say from RestAPI to config the number of replicas.
    idealState.setReplicas(Integer.toString(DEFAULT_SUPERCLUSTER_REPLICA));
    idealState.getRecord().setListField(clusterName, new ArrayList<>());

    List<String> controllers = getInstancesInCluster(grandCluster);
    if (controllers.size() == 0) {
      throw new HelixException("Grand cluster " + grandCluster + " has no instances");
    }

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(grandCluster, new ZkBaseDataAccessor<>(_zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.idealStates(idealState.getResourceName()), idealState);
    LOG.info("Cluster {} has been added to grand cluster {} with rebalance configuration {}.",
        clusterName, grandCluster, idealState.getRecord().getSimpleFields().toString());
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
  public void addCustomizedStateConfig(String clusterName,
      CustomizedStateConfig customizedStateConfig) {
    logger.info(
        "Add CustomizedStateConfig to cluster {}, CustomizedStateConfig is {}",
        clusterName, customizedStateConfig.toString());

    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    CustomizedStateConfig.Builder builder =
        new CustomizedStateConfig.Builder(customizedStateConfig);
    CustomizedStateConfig customizedStateConfigFromBuilder = builder.build();

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateConfig(),
        customizedStateConfigFromBuilder);
  }

  @Override
  public void removeCustomizedStateConfig(String clusterName) {
    logger.info(
        "Remove CustomizedStateConfig from cluster {}.", clusterName);

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.removeProperty(keyBuilder.customizedStateConfig());

  }

  @Override
  public void addTypeToCustomizedStateConfig(String clusterName, String type) {
    logger.info("Add type {} to CustomizedStateConfig of cluster {}", type, clusterName);

    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    CustomizedStateConfig.Builder builder =
        new CustomizedStateConfig.Builder();

    builder.addAggregationEnabledType(type);
    CustomizedStateConfig customizedStateConfigFromBuilder = builder.build();

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    if(!accessor.updateProperty(keyBuilder.customizedStateConfig(),
        customizedStateConfigFromBuilder)) {
      throw new HelixException(
          "Failed to add customized state config type " + type + " to cluster" + clusterName);
    }
  }


  @Override
  public void removeTypeFromCustomizedStateConfig(String clusterName, String type) {
    logger.info("Remove type {} to CustomizedStateConfig of cluster {}", type,
        clusterName);

    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    CustomizedStateConfig.Builder builder = new CustomizedStateConfig.Builder(
        _configAccessor.getCustomizedStateConfig(clusterName));

    if (!builder.getAggregationEnabledTypes().contains(type)) {
      throw new HelixException("Type " + type
          + " is missing from the CustomizedStateConfig of cluster " + clusterName);
    }

    builder.removeAggregationEnabledType(type);
    CustomizedStateConfig customizedStateConfigFromBuilder = builder.build();
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateConfig(),
        customizedStateConfigFromBuilder);
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
  public void onDemandRebalance(String clusterName) {
    BaseDataAccessor<ZNRecord> baseAccessor = _baseDataAccessor;
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
        clusterConfig.setLastOnDemandRebalanceTimestamp(System.currentTimeMillis());
        return clusterConfig.getRecord();
      }
    }, AccessOption.PERSISTENT);
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
      } else if (count.equalsIgnoreCase(StateModelDefinition.STATE_REPLICA_COUNT_ALL_REPLICAS)) {
        if (slaveStateValue != null) {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        slaveStateValue = state;
      } else if (count.equalsIgnoreCase(
          StateModelDefinition.STATE_REPLICA_COUNT_ALL_CANDIDATE_NODES)) {
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

  private static byte[] readFile(String filePath)
      throws IOException {
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
      String stateModelDefFile)
      throws IOException {
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
    BaseDataAccessor<ZNRecord> baseAccessor = _baseDataAccessor;

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    String path = keyBuilder.constraint(constraintType.toString()).getPath();

    baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        ClusterConstraints constraints =
            currentData == null ? new ClusterConstraints(constraintType)
                : new ClusterConstraints(currentData);

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
    BaseDataAccessor<ZNRecord> baseAccessor = _baseDataAccessor;

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
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
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
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
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

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

  @Deprecated
  private void enableSingleInstance(final String clusterName, final String instanceName,
      final boolean enabled, BaseDataAccessor<ZNRecord> baseAccessor,
      InstanceConstants.InstanceDisabledType disabledType, String reason) {
    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);

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
        if (!enabled) {
          // new disabled type and reason will overwrite existing ones.
          config.resetInstanceDisabledTypeAndReason();
          if (reason != null) {
            config.setInstanceDisabledReason(reason);
          }
          if (disabledType != null) {
            config.setInstanceDisabledType(disabledType);
          }
        }
        return config.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  // TODO: Add history ZNode for all batched enabling/disabling histories with metadata.
  @Deprecated
  private void enableBatchInstances(final String clusterName, final List<String> instances,
      final boolean enabled, BaseDataAccessor<ZNRecord> baseAccessor,
      InstanceConstants.InstanceDisabledType disabledType, String reason) {

    // TODO: batch enable/disable is breaking backward compatibility on instance enable with older library
    // re-enable once batch enable/disable is ready
    if (true) {
      throw new HelixException("enableBatchInstances is not supported.");
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
        Map<String, String> disabledInstances = new TreeMap<>(clusterConfig.getDisabledInstances());
        Map<String, String> disabledInstancesWithInfo = new TreeMap<>(clusterConfig.getDisabledInstancesWithInfo());
        if (enabled) {
          disabledInstances.keySet().removeAll(instances);
          disabledInstancesWithInfo.keySet().removeAll(instances);
        } else {
          for (String disabledInstance : instances) {
            // We allow user to override disabledType and reason for an already disabled instance.
            // TODO: we are updating both DISABLED_INSTANCES and DISABLED_INSTANCES_W_INFO for
            // backward compatible. Deprecate DISABLED_INSTANCES in the future.
            // TODO: update the history ZNode
            String timeStamp = String.valueOf(System.currentTimeMillis());
            disabledInstances.put(disabledInstance, timeStamp);
            disabledInstancesWithInfo
                .put(disabledInstance, assembleInstanceBatchedDisabledInfo(disabledType, reason, timeStamp));
          }
        }
        clusterConfig.setDisabledInstances(disabledInstances);
        clusterConfig.setDisabledInstancesWithInfo(disabledInstancesWithInfo);

        return clusterConfig.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  public static String assembleInstanceBatchedDisabledInfo(
      InstanceConstants.InstanceDisabledType disabledType, String reason, String timeStamp) {
    Map<String, String> disableInfo = new TreeMap<>();
    disableInfo.put(ClusterConfig.ClusterConfigProperty.HELIX_ENABLED_DISABLE_TIMESTAMP.toString(),
        timeStamp);
    if (disabledType != null) {
      disableInfo.put(ClusterConfig.ClusterConfigProperty.HELIX_DISABLED_TYPE.toString(),
          disabledType.toString());
    }
    if (reason != null) {
      disableInfo.put(ClusterConfig.ClusterConfigProperty.HELIX_DISABLED_REASON.toString(), reason);
    }
    return ConfigStringUtil.concatenateMapping(disableInfo);
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
    List<ZNRecord> znRecords = baseAccessor.getChildren(path, null, 0, 0, 0);
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

  /**
   * Closes the ZkClient only if it was generated internally.
   */
  @Override
  public void close() {
    if (_zkClient != null && !_usesExternalZkClient) {
      _zkClient.close();
    }
  }

  @Override
  public void finalize() {
    close();
  }

  @Override
  public boolean addResourceWithWeight(String clusterName, IdealState idealState,
      ResourceConfig resourceConfig) {
    // Null checks
    if (clusterName == null || clusterName.isEmpty()) {
      throw new HelixException("Cluster name is null or empty!");
    }
    if (idealState == null || !idealState.isValid()) {
      throw new HelixException("IdealState is null or invalid!");
    }
    if (resourceConfig == null || !resourceConfig.isValid()) {
      // TODO This might be okay because of default weight?
      throw new HelixException("ResourceConfig is null or invalid!");
    }

    // Make sure IdealState and ResourceConfig are for the same resource
    if (!idealState.getResourceName().equals(resourceConfig.getResourceName())) {
      throw new HelixException("Resource names in IdealState and ResourceConfig are different!");
    }

    // Order in which a resource should be added:
    // 1. Validate the weights in ResourceConfig against ClusterConfig
    // Check that all capacity keys in ClusterConfig are set up in every partition in ResourceConfig field
    if (!validateWeightForResourceConfig(_configAccessor.getClusterConfig(clusterName),
        resourceConfig, idealState)) {
      throw new HelixException(String
          .format("Could not add resource %s with weight! Failed to validate the ResourceConfig!",
              idealState.getResourceName()));
    }

    // 2. Add the resourceConfig to ZK
    _configAccessor
        .setResourceConfig(clusterName, resourceConfig.getResourceName(), resourceConfig);

    // 3. Add the idealState to ZK
    setResourceIdealState(clusterName, idealState.getResourceName(), idealState);

    // 4. rebalance the resource
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<String> liveNodes = accessor.getChildNames(keyBuilder.liveInstances());

    rebalance(clusterName, idealState.getResourceName(), idealState.getReplicaCount(liveNodes.size()),
        idealState.getResourceName(), idealState.getInstanceGroupTag());

    return true;
  }

  @Override
  public boolean enableWagedRebalance(String clusterName, List<String> resourceNames) {
    // Null checks
    if (clusterName == null || clusterName.isEmpty()) {
      throw new HelixException("Cluster name is invalid!");
    }
    if (resourceNames == null || resourceNames.isEmpty()) {
      throw new HelixException("Resource name list is invalid!");
    }

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<IdealState> enabledIdealStates = new ArrayList<>();
    List<PropertyKey> enabledIdealStateKeys = new ArrayList<>();
    Set<String> enabledResourceNames = new HashSet<>();

    List<IdealState> idealStates = accessor.getChildValues(keyBuilder.idealStates(), true);
    for (IdealState idealState : idealStates) {
      if (idealState != null && resourceNames.contains(idealState.getResourceName())) {
        idealState.setRebalancerClassName(WagedRebalancer.class.getName());
        idealState.setRebalanceMode(RebalanceMode.FULL_AUTO);
        enabledIdealStates.add(idealState);
        enabledIdealStateKeys.add(keyBuilder.idealStates(idealState.getResourceName()));
        enabledResourceNames.add(idealState.getResourceName());
      }
    }
    List<String> resourcesNotFound =
        resourceNames.stream().filter(resourceName -> !enabledResourceNames.contains(resourceName))
            .collect(Collectors.toList());
    if (!resourcesNotFound.isEmpty()) {
      throw new HelixException(
          String.format("Some resources do not have IdealStates: %s", resourcesNotFound));
    }
    boolean[] success = accessor.setChildren(enabledIdealStateKeys, enabledIdealStates);
    for (boolean s : success) {
      if (!s) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Map<String, Boolean> validateResourcesForWagedRebalance(String clusterName,
      List<String> resourceNames) {
    // Null checks
    if (clusterName == null || clusterName.isEmpty()) {
      throw new HelixException("Cluster name is invalid!");
    }
    if (resourceNames == null || resourceNames.isEmpty()) {
      throw new HelixException("Resource name list is invalid!");
    }

    // Ensure that all instances are valid
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<String> instances = accessor.getChildNames(keyBuilder.instanceConfigs());
    if (validateInstancesForWagedRebalance(clusterName, instances).containsValue(false)) {
      throw new HelixException(String
          .format("Instance capacities haven't been configured properly for cluster %s",
              clusterName));
    }

    Map<String, Boolean> result = new HashMap<>();
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterName);
    for (String resourceName : resourceNames) {
      IdealState idealState = getResourceIdealState(clusterName, resourceName);
      if (idealState == null || !idealState.isValid()) {
        result.put(resourceName, false);
        continue;
      }
      ResourceConfig resourceConfig = _configAccessor.getResourceConfig(clusterName, resourceName);
      result.put(resourceName,
          validateWeightForResourceConfig(clusterConfig, resourceConfig, idealState));
    }
    return result;
  }

  @Override
  public Map<String, Boolean> validateInstancesForWagedRebalance(String clusterName,
      List<String> instanceNames) {
    // Null checks
    if (clusterName == null || clusterName.isEmpty()) {
      throw new HelixException("Cluster name is invalid!");
    }
    if (instanceNames == null || instanceNames.isEmpty()) {
      throw new HelixException("Instance name list is invalid!");
    }

    Map<String, Boolean> result = new HashMap<>();
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterName);
    for (String instanceName : instanceNames) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(clusterName, instanceName);
      if (instanceConfig == null || !instanceConfig.isValid()) {
        result.put(instanceName, false);
        continue;
      }
      WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
      result.put(instanceName, true);
    }

    return result;
  }

  /**
   * Validates ResourceConfig's weight field against the given ClusterConfig.
   * @param clusterConfig
   * @param resourceConfig
   * @param idealState
   * @return true if ResourceConfig has all the required fields. False otherwise.
   */
  private boolean validateWeightForResourceConfig(ClusterConfig clusterConfig,
      ResourceConfig resourceConfig, IdealState idealState) {
    if (resourceConfig == null) {
      if (clusterConfig.getDefaultPartitionWeightMap().isEmpty()) {
        logger.error(
            "ResourceConfig for {} is null, and there are no default weights set in ClusterConfig!",
            idealState.getResourceName());
        return false;
      }
      // If ResourceConfig is null AND the default partition weight map is defined, and the map has all the required keys, we consider this valid since the default weights will be used
      // Need to check the map contains all the required keys
      if (clusterConfig.getDefaultPartitionWeightMap().keySet()
          .containsAll(clusterConfig.getInstanceCapacityKeys())) {
        // Contains all the required keys, so consider it valid since it will use the default weights
        return true;
      }
      logger.error(
          "ResourceConfig for {} is null, and ClusterConfig's default partition weight map doesn't have all the required keys!",
          idealState.getResourceName());
      return false;
    }

    // Parse the entire capacityMap from ResourceConfig
    Map<String, Map<String, Integer>> capacityMap;
    try {
      capacityMap = resourceConfig.getPartitionCapacityMap();
    } catch (IOException ex) {
      logger.error("Invalid partition capacity configuration of resource: {}",
          idealState.getResourceName(), ex);
      return false;
    }

    Set<String> capacityMapSet = new HashSet<>(capacityMap.keySet());
    boolean hasDefaultCapacity = capacityMapSet.contains(ResourceConfig.DEFAULT_PARTITION_KEY);
    // Remove DEFAULT key
    capacityMapSet.remove(ResourceConfig.DEFAULT_PARTITION_KEY);

    // Make sure capacityMap contains all partitions defined in IdealState
    // Here, IdealState has not been rebalanced, so listFields might be null, in which case, we would get an emptyList from getPartitionSet()
    // So check using numPartitions instead
    // This check allows us to fail early on instead of having to loop through all partitions
    if (capacityMapSet.size() != idealState.getNumPartitions() && !hasDefaultCapacity) {
      logger.error(
          "ResourceConfig for {} does not have all partitions defined in PartitionCapacityMap!",
          idealState.getResourceName());
      return false;
    }

    // Loop through all partitions and validate
    capacityMap.keySet().forEach(partitionName -> WagedValidationUtil
        .validateAndGetPartitionCapacity(partitionName, resourceConfig, capacityMap,
            clusterConfig));
    return true;
  }

  public static class Builder extends GenericZkHelixApiBuilder<Builder> {
    public Builder() {
    }

    public ZKHelixAdmin build() {
      validate();
      return new ZKHelixAdmin(
          createZkClient(_realmMode, _realmAwareZkConnectionConfig, _realmAwareZkClientConfig,
              _zkAddress), false);
    }
  }

  private Set<String> findTimeoutOfflineInstances(String clusterName, long offlineDuration) {
    // in case there is no customized timeout value, use the one defined in cluster config
    if (offlineDuration == ClusterConfig.OFFLINE_DURATION_FOR_PURGE_NOT_SET) {
      offlineDuration =
          _configAccessor.getClusterConfig(clusterName).getOfflineDurationForPurge();
      if (offlineDuration == ClusterConfig.OFFLINE_DURATION_FOR_PURGE_NOT_SET) {
        return Collections.emptySet();
      }
    }

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseDataAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<String> instanceConfigNames = accessor.getChildNames(keyBuilder.instanceConfigs());
    List<String> instancePathNames = accessor.getChildNames(keyBuilder.instances());
    List<String> liveNodes = accessor.getChildNames(keyBuilder.liveInstances());

    Set<String> offlineInstanceNames = new HashSet<>(instancePathNames);
    liveNodes.forEach(offlineInstanceNames::remove);
    long finalOfflineDuration = offlineDuration;
    offlineInstanceNames.removeIf(instanceName -> {
      ParticipantHistory participantHistory =
          accessor.getProperty(keyBuilder.participantHistory(instanceName));
      if (participantHistory == null && instanceConfigNames.contains(instanceName)) {
        // this is likely caused by a new instance joining and should not be purged
        return true;
      }
      // If participant history is null without config, a race condition happened and should be
      // cleaned up.
      // Otherwise, if the participant has not been offline for more than the duration, no clean up
      return (participantHistory != null && (
          participantHistory.getLastOfflineTime() == ParticipantHistory.ONLINE
              || System.currentTimeMillis() - participantHistory.getLastOfflineTime()
              < finalOfflineDuration));
    });

    return offlineInstanceNames;
  }
}
