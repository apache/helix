package org.apache.helix;

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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.api.topology.ClusterTopology;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;

/*
 * Helix cluster management
 */
public interface HelixAdmin {
  /**
   * Get a list of clusters under "/"
   * @return a list of cluster names
   */
  List<String> getClusters();

  /**
   * Get a list of instances under a cluster
   * @param clusterName
   * @return a list of instance names
   */
  List<String> getInstancesInCluster(String clusterName);

  /**
   * Get an instance config
   * @param clusterName
   * @param instanceName
   * @return InstanceConfig corresponding to the specified instance
   */
  InstanceConfig getInstanceConfig(String clusterName, String instanceName);

  /**
   * Set the instance config of an existing instance under the given cluster.
   * @param clusterName the name of the cluster to which this instance belongs.
   * @param instanceName the name of this instance.
   * @param instanceConfig the new {@link InstanceConfig} that will replace the current one
   *          associated with this instance.
   * @return true if the operation was successful; false otherwise.
   */
  boolean setInstanceConfig(String clusterName, String instanceName, InstanceConfig instanceConfig);

  /**
   * Get a list of resources in a cluster
   * @param clusterName
   * @return a list of resource names in the cluster
   */
  List<String> getResourcesInCluster(String clusterName);

  /**
   * Get a list of resources in a cluster with a tag
   * @param clusterName
   * @param tag
   */
  List<String> getResourcesInClusterWithTag(String clusterName, String tag);

  /**
   * Add a cluster
   * @param clusterName
   * @return true if successfully created, or if cluster already exists
   */
  boolean addCluster(String clusterName);

  /**
   * Add a cluster
   * @param clusterName
   * @param recreateIfExists If the cluster already exists, it will delete it and recreate
   * @return true if successfully created, or if cluster already exists
   */
  boolean addCluster(String clusterName, boolean recreateIfExists);

  /**
   * Add a cluster and also add this cluster as a resource group in the super cluster
   * @param clusterName
   * @param grandCluster
   */
  void addClusterToGrandCluster(String clusterName, String grandCluster);

  /** Add a CustomizedStateConfig to a cluster
   * @param clusterName
   * @param customizedStateConfig
   */
  void addCustomizedStateConfig(String clusterName,
      CustomizedStateConfig customizedStateConfig);

  /**
   * Remove CustomizedStateConfig from specific cluster
   * @param clusterName
   */
  void removeCustomizedStateConfig(String clusterName);

  /**
   * Add a type to CustomizedStateConfig of specific cluster
   * @param clusterName
   */
  void addTypeToCustomizedStateConfig(String clusterName, String type);

  /**
   * Remove a type from CustomizedStateConfig of specific cluster
   * @param clusterName
   */
  void removeTypeFromCustomizedStateConfig(String clusterName, String type);

  /**
   * Add a resource to a cluster, using the default ideal state mode AUTO
   * @param clusterName
   * @param resourceName
   * @param numPartitions
   * @param stateModelRef
   */
  void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef);

  /**
   * @param clusterName
   * @param resourceName
   * @param idealstate
   */
  void addResource(String clusterName, String resourceName, IdealState idealstate);

  /**
   * Add a resource to a cluster
   * @param clusterName
   * @param resourceName
   * @param numPartitions
   * @param stateModelRef
   * @param rebalancerMode
   */
  void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode);

  /**
   * Add a resource to a cluster
   * @param clusterName
   * @param resourceName
   * @param numPartitions
   * @param stateModelRef
   * @param rebalancerMode
   * @param rebalanceStrategy
   */
  void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode, String rebalanceStrategy);

  /**
   * Add a resource to a cluster, using a bucket size > 1
   * @param clusterName
   * @param resourceName
   * @param numPartitions
   * @param stateModelRef
   * @param rebalancerMode
   * @param bucketSize
   */
  void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode, int bucketSize);

  /**
   * Add a resource to a cluster, using a bucket size > 1
   * @param clusterName
   * @param resourceName
   * @param numPartitions
   * @param stateModelRef
   * @param rebalancerMode
   * @param bucketSize
   * @param maxPartitionsPerInstance
   */
  void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode, int bucketSize, int maxPartitionsPerInstance);

  /**
   * Add a resource to a cluster, using a bucket size > 1
   * @param clusterName
   * @param resourceName
   * @param numPartitions
   * @param stateModelRef
   * @param rebalancerMode
   * @param rebalanceStrategy
   * @param bucketSize
   * @param maxPartitionsPerInstance
   */
  void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode, String rebalanceStrategy, int bucketSize,
      int maxPartitionsPerInstance);

  /**
   * Add an instance to a cluster
   * @param clusterName
   * @param instanceConfig
   */
  void addInstance(String clusterName, InstanceConfig instanceConfig);

  /**
   * Drop an instance from a cluster
   * @param clusterName
   * @param instanceConfig
   */
  void dropInstance(String clusterName, InstanceConfig instanceConfig);

  /**
   * Purge offline instances that have been offline for longer than the offline duration time
   * from a cluster
   * @param clusterName
   * @param offlineDuration if an offline instance has been offline for longer than the set
   *                        offlineDuration, the offline instance becomes eligible for being
   *                        purged/deleted
   */
  void purgeOfflineInstances(String clusterName, long offlineDuration);

  /**
   * Get ideal state for a resource
   * @param clusterName
   * @param resourceName
   * @return
   */
  IdealState getResourceIdealState(String clusterName, String resourceName);

  /**
   * Set ideal state for a resource
   * @param clusterName
   * @param resourceName
   * @param idealState
   */
  void setResourceIdealState(String clusterName, String resourceName, IdealState idealState);

  /**
   * Selectively updates fields for an existing resource's IdealState ZNode.
   * @param clusterName
   * @param resourceName
   * @param idealState
   */
  void updateIdealState(String clusterName, String resourceName, IdealState idealState);

  /**
   * Selectively removes fields for an existing resource's IdealState ZNode.
   * @param clusterName
   * @param resourceName
   * @param idealState
   */
  void removeFromIdealState(String clusterName, String resourceName, IdealState idealState);

  /**
   * Disable or enable an instance
   * @param clusterName
   * @param instanceName
   * @param enabled
   */
  @Deprecated
  void enableInstance(String clusterName, String instanceName, boolean enabled);

  /**
   * @deprecated use {@link #setInstanceOperation(String, String, InstanceConstants.InstanceOperation)}
   * @param clusterName
   * @param instanceName
   * @param enabled
   * @param disabledType disabledType for disable operation. It is ignored when enabled is true.
   *                     Existing disabledType will be over write if instance is in disabled state.
   * @param reason set additional string description on why the instance is disabled when
   *          <code>enabled</code> is false. Existing disabled reason will be over write if instance is in disabled state.
   */
  @Deprecated
  void enableInstance(String clusterName, String instanceName, boolean enabled,
      InstanceConstants.InstanceDisabledType disabledType, String reason);

  /**
   * Batch enable/disable instances in a cluster
   * By default, all the instances are enabled
   * @deprecated use {@link #setInstanceOperation(String, String, InstanceConstants.InstanceOperation)}
   * @param clusterName
   * @param instances
   * @param enabled
   */
  @Deprecated
  void enableInstance(String clusterName, List<String> instances, boolean enabled);

  /**
   * Set the instanceOperation of and instance with {@link InstanceConstants.InstanceOperation}.
   *
   * @param clusterName       The cluster name
   * @param instanceName      The instance name
   * @param instanceOperation The instance operation type
   */
  default void setInstanceOperation(String clusterName, String instanceName,
      InstanceConstants.InstanceOperation instanceOperation) {
    throw new UnsupportedOperationException("setInstanceOperation is not implemented.");
  }

  /**
   * Set the instanceOperation of and instance with {@link InstanceConstants.InstanceOperation}.
   *
   * @param clusterName       The cluster name
   * @param instanceName      The instance name
   * @param instanceOperation The instance operation type
   * @param reason            The reason for the operation
   */
  default void setInstanceOperation(String clusterName, String instanceName,
      InstanceConstants.InstanceOperation instanceOperation, String reason) {
    throw new UnsupportedOperationException("setInstanceOperation is not implemented.");
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
  default void setInstanceOperation(String clusterName, String instanceName,
      InstanceConstants.InstanceOperation instanceOperation, String reason, boolean overrideAll) {
    throw new UnsupportedOperationException("setInstanceOperation is not implemented.");
  }

  /**
   * Disable or enable a resource
   * @param clusterName
   * @param resourceName
   */
  void enableResource(String clusterName, String resourceName, boolean enabled);

  /**
   * Disable or enable a list of partitions on an instance
   * @param enabled
   * @param clusterName
   * @param instanceName
   * @param resourceName
   * @param partitionNames
   */
  void enablePartition(boolean enabled, String clusterName, String instanceName,
      String resourceName, List<String> partitionNames);

  /**
   * Disable or enable a cluster
   * @param clusterName
   * @param enabled
   */
  void enableCluster(String clusterName, boolean enabled);

  /**
   * @param clusterName
   * @param enabled
   * @param reason set additional string description on why the cluster is disabled when
   *          <code>enabled</code> is false.
   */
  void enableCluster(String clusterName, boolean enabled, String reason);

  /**
   * **Deprecated: use autoEnableMaintenanceMode or manuallyEnableMaintenanceMode instead**
   * Enable or disable maintenance mode for a cluster
   * @param clusterName
   * @param enabled
   */
  @Deprecated
  void enableMaintenanceMode(String clusterName, boolean enabled);

  /**
   * **Deprecated: use autoEnableMaintenanceMode or manuallyEnableMaintenanceMode instead**
   * Enable or disable maintenance mode for a cluster
   * @param clusterName
   * @param enabled
   * @param reason
   */
  @Deprecated
  void enableMaintenanceMode(String clusterName, boolean enabled, String reason);

  /**
   * Automatically enable maintenance mode. To be called by the Controller pipeline.
   * @param clusterName
   * @param enabled
   * @param reason
   * @param internalReason
   */
  void autoEnableMaintenanceMode(String clusterName, boolean enabled, String reason,
      MaintenanceSignal.AutoTriggerReason internalReason);

  /**
   * Manually enable maintenance mode. To be called by the REST client that accepts KV mappings as
   * the payload.
   * @param clusterName
   * @param enabled
   * @param reason
   * @param customFields user-specified KV mappings to be stored in the ZNode
   */
  void manuallyEnableMaintenanceMode(String clusterName, boolean enabled, String reason,
      Map<String, String> customFields);

  /**
   * Enable maintenance mode via automation systems (like HelixACM). To be called by automation services.
   * @param clusterName
   * @param enabled
   * @param reason
   * @param customFields user-specified KV mappings to be stored in the ZNode
   */
  void automationEnableMaintenanceMode(String clusterName, boolean enabled, String reason,
      Map<String, String> customFields);

  /**
   * Check specific cluster is in maintenance mode or not
   * @param clusterName the cluster name
   * @return true if in maintenance mode, false otherwise
   */
  boolean isInMaintenanceMode(String clusterName);

  /**
   * Requests to put a cluster into a management mode
   * {@link ClusterManagementMode.Type}. When this method returns,
   * it means the signal has been successfully sent, but it does not mean the cluster has
   * fully entered the mode. Because the cluster can take some time to complete the request.
   * <p>
   * To check the cluster management mode status, call {@link #getClusterManagementMode(String)}.
   *
   * @param request request to set the cluster management mode. {@link ClusterManagementModeRequest}
   */
  void setClusterManagementMode(ClusterManagementModeRequest request);

  /**
   * Gets cluster management status {@link ClusterManagementMode}: what mode the cluster is and
   * whether the cluster has fully reached to that mode.
   *
   * @param clusterName cluster name
   * @return {@link ClusterManagementMode}
   */
  ClusterManagementMode getClusterManagementMode(String clusterName);

  /**
   * Set a list of partitions for an instance to ERROR state from any state.
   * The partitions could be in any state and setPartitionsToError will bring them to ERROR
   * state. ANY to ERROR state transition is required for this.
   * @param clusterName
   * @param instanceName
   * @param resourceName
   * @param partitionNames
   */
  default void setPartitionsToError(String clusterName, String instanceName, String resourceName,
      List<String> partitionNames) {
    throw new UnsupportedOperationException("setPartitionsToError is not implemented.");
  }

  /**
   * Reset a list of partitions in error state for an instance
   * The partitions are assume to be in error state and reset will bring them from error
   * to initial state. An error to initial state transition is required for reset.
   * @param clusterName
   * @param instanceName
   * @param resourceName
   * @param partitionNames
   */
  void resetPartition(String clusterName, String instanceName, String resourceName,
      List<String> partitionNames);

  /**
   * Reset all the partitions in error state for a list of instances
   * @param clusterName
   * @param instanceNames
   */
  void resetInstance(String clusterName, List<String> instanceNames);

  /**
   * Reset all partitions in error state for a list of resources
   * @param clusterName
   * @param resourceNames
   */
  void resetResource(String clusterName, List<String> resourceNames);

  /**
   * Add a state model definition
   * @param clusterName
   * @param stateModelDef
   * @param record
   * @return true if successfully created, or if state model definition already exists
   */
  void addStateModelDef(String clusterName, String stateModelDef, StateModelDefinition record);

  /**
   * Add a state model definition
   * @param clusterName
   * @param stateModelDef
   * @param record
   * @param recreateIfExists If the state definition already exists, it will delete it and recreate
   * @return true if successfully created, or if state model definition already exists
   */
  void addStateModelDef(String clusterName, String stateModelDef, StateModelDefinition record,
      boolean recreateIfExists);

  /**
   * Drop a resource from a cluster
   * @param clusterName
   * @param resourceName
   */
  void dropResource(String clusterName, String resourceName);

  /**
   * Add cloud config to the cluster.
   * @param clusterName
   * @param cloudConfig
   */
  void addCloudConfig(String clusterName, CloudConfig cloudConfig);

  /**
   * Remove the Cloud Config for specific cluster
   * @param clusterName
   */
  void removeCloudConfig(String clusterName);

  /**
   * Get the topology of a specific cluster
   * @param clusterName
   */
  ClusterTopology getClusterTopology(String clusterName);

  /**
   * Get a list of state model definitions in a cluster
   * @param clusterName
   * @return
   */
  List<String> getStateModelDefs(String clusterName);

  /**
   * Get a state model definition in a cluster
   * @param clusterName
   * @param stateModelName
   * @return StateModelDefinition identified by stateModelName
   */
  StateModelDefinition getStateModelDef(String clusterName, String stateModelName);

  /**
   * Get external view for a resource
   * @param clusterName
   * @param resourceName
   * @return ExternalView for the resource
   */
  ExternalView getResourceExternalView(String clusterName, String resourceName);

  /**
   * Get customized view for a resource
   * @param clusterName
   * @param resourceName
   * @return customized for the resource
   */
  CustomizedView getResourceCustomizedView(String clusterName, String resourceName,
      String customizedStateType);

  /**
   * Drop a cluster
   * @param clusterName
   */
  void dropCluster(String clusterName);

  /**
   * Set configuration values
   * @param scope
   * @param properties
   */
  void setConfig(HelixConfigScope scope, Map<String, String> properties);

  /**
   * Remove configuration values
   * @param scope
   * @param keys
   */
  void removeConfig(HelixConfigScope scope, List<String> keys);

  /**
   * Get configuration values
   * @param scope
   * @param keys
   * @return configuration values ordered by the provided keys
   */
  Map<String, String> getConfig(HelixConfigScope scope, List<String> keys);

  /**
   * Get configuration keys
   * @param scope
   * @return keys mapping to valid configuration values
   */
  List<String> getConfigKeys(HelixConfigScope scope);

  /**
   * Rebalance a resource in cluster
   * @param clusterName
   * @param resourceName
   * @param replica
   */
  void rebalance(String clusterName, String resourceName, int replica);

  /**
   * Rebalance a cluster without respecting the delay
   * @param clusterName
   */
  void onDemandRebalance(String clusterName);

  /**
   * Add ideal state using a json format file
   * @param clusterName
   * @param resourceName
   * @param idealStateFile
   * @throws IOException
   */
  void addIdealState(String clusterName, String resourceName, String idealStateFile)
      throws IOException;

  /**
   * Add state model definition using a json format file
   * @param clusterName
   * @param stateModelDefName
   * @param stateModelDefFile
   * @throws IOException error reading the state model definition file
   */
  void addStateModelDef(String clusterName, String stateModelDefName, String stateModelDefFile)
      throws IOException;

  /**
   * Add a constraint item; create if not exist
   * @param clusterName
   * @param constraintType
   * @param constraintId
   * @param constraintItem
   */
  void setConstraint(String clusterName, ConstraintType constraintType, String constraintId,
      ConstraintItem constraintItem);

  /**
   * Remove a constraint item
   * @param clusterName
   * @param constraintType
   * @param constraintId
   */
  void removeConstraint(String clusterName, ConstraintType constraintType, String constraintId);

  /**
   * Get all constraints for a type
   * @param clusterName
   * @param constraintType
   * @return constraints of constraintType
   */
  ClusterConstraints getConstraints(String clusterName, ConstraintType constraintType);

  /**
   * @param clusterName
   * @param currentIdealState
   * @param instanceNames
   */
  void rebalance(String clusterName, IdealState currentIdealState, List<String> instanceNames);

  /**
   * @param clusterName
   * @param resourceName
   * @param replica
   * @param instances
   */
  void rebalance(String clusterName, String resourceName, int replica, List<String> instances);

  /**
   * @param clusterName
   * @param resourceName
   * @param replica
   * @param keyPrefix
   * @param group the group identifier of instances to rebalance
   */
  void rebalance(String clusterName, String resourceName, int replica, String keyPrefix,
      String group);

  /**
   * @param clusterName
   * @param tag
   */
  List<String> getInstancesInClusterWithTag(String clusterName, String tag);

  /**
   * @param clusterName
   * @param instanceName
   * @param tag
   */
  void addInstanceTag(String clusterName, String instanceName, String tag);

  /**
   * @param clusterName
   * @param instanceName
   * @param tag
   */
  void removeInstanceTag(String clusterName, String instanceName, String tag);

  void setInstanceZoneId(String clusterName, String instanceName, String zoneId);

  /**
   * Enable/disable batch message mode for specified cluster.
   * By default batch message mode is disabled.
   * @param clusterName
   * @param enabled
   */
  void enableBatchMessageMode(String clusterName, boolean enabled);

  /**
   * Enable/disable batch message mode for specified resource in a cluster
   * By default batch message mode is disabled.
   * @param clusterName
   * @param resourceName
   * @param enabled
   */
  void enableBatchMessageMode(String clusterName, String resourceName, boolean enabled);

  /**
   * Get batch disabled instance map (disabled instance -> disabled time) in a cluster. It will
   * include disabled instances and instances in disabled zones
   * @param clusterName
   * @return
   */
  Map<String, String> getBatchDisabledInstances(String clusterName);

  /**
   * Get list of instances by domain for a cluster
   * Example : domain could be "helixZoneId=1,rackId=3". All the instances domain contains these
   * two domains will be selected.
   * @param clusterName
   * @return
   */
  List<String> getInstancesByDomain(String clusterName, String domain);

  /**
   * Release resources used in HelixAdmin.
   */
  default void close() {
    System.out.println("Default close() was invoked! No operation was executed.");
  }

  /**
   * Adds a resource with IdealState and ResourceConfig to be rebalanced by WAGED rebalancer with validation.
   * Validation includes the following:
   * 1. Check ResourceConfig has the WEIGHT field
   * 2. Check that all capacity keys from ClusterConfig are set up in the WEIGHT field
   * 3. Check that all ResourceConfig's weightMap fields have all of the capacity keys
   * @param clusterName
   * @param idealState
   * @param resourceConfig
   * @return true if the resource has been added successfully. False otherwise
   */
  boolean addResourceWithWeight(String clusterName, IdealState idealState,
      ResourceConfig resourceConfig);

  /**
   * Batch-enables Waged rebalance for the names of resources given.
   * @param clusterName
   * @param resourceNames
   * @return
   */
  boolean enableWagedRebalance(String clusterName, List<String> resourceNames);

  /**
   * Validates the resources to see if their weight configs have been set properly.
   * Validation includes the following:
   * 1. Check ResourceConfig has the WEIGHT field
   * 2. Check that all capacity keys from ClusterConfig are set up in the WEIGHT field
   * 3. Check that all ResourceConfig's weightMap fields have all of the capacity keys
   * @param resourceNames
   * @return for each resource, true if the weight configs have been set properly, false otherwise
   */
  Map<String, Boolean> validateResourcesForWagedRebalance(String clusterName,
      List<String> resourceNames);

  /**
   * Validates the instances to ensure their weights in InstanceConfigs have been set up properly.
   * Validation includes the following:
   * 1. If default instance capacity is not set, check that the InstanceConfigs have the CAPACITY field
   * 2. Check that all capacity keys defined in ClusterConfig are present in the CAPACITY field
   * @param clusterName
   * @param instancesNames
   * @return
   */
  Map<String, Boolean> validateInstancesForWagedRebalance(String clusterName,
      List<String> instancesNames);

  /**
   * Return if instance operation 'Evacuate' is finished.
   * @param clusterName
   * @param instancesNames
   * @return Return true if there is no current state nor pending message on the instance.
   */
  default boolean isEvacuateFinished(String clusterName, String instancesNames) {
    throw new UnsupportedOperationException("isEvacuateFinished is not implemented.");
  }

  /**
   * Check to see if swapping between two instances can be completed. Either the swapOut or
   * swapIn instance can be passed in.
   * @param clusterName  The cluster name
   * @param instanceName The instance that is being swapped out or swapped in
   * @return True if the swap is ready to be completed, false otherwise.
   */
  default boolean canCompleteSwap(String clusterName, String instanceName) {
    throw new UnsupportedOperationException("canCompleteSwap is not implemented.");
  }

  /**
   * Check to see if swapping between two instances is ready to be completed and complete it if
   * possible. Either the swapOut or swapIn instance can be passed in.
   *
   * @param clusterName  The cluster name
   * @param instanceName The instance that is being swapped out or swapped in
   * @param forceComplete Whether to force complete the swap without checking if it is ready
   * @return True if the swap is ready to be completed and was completed successfully, false
   * otherwise.
   */
  default boolean completeSwapIfPossible(String clusterName, String instanceName,
      boolean forceComplete) {
    throw new UnsupportedOperationException("completeSwapIfPossible is not implemented.");
  }

  /**
   * Return if instance is ready for preparing joining cluster. The instance should have no current state,
   * no pending message and tagged with operation that exclude the instance from Helix assignment.
   * @param clusterName
   * @param instancesNames
   * @return true if the instance is ready for preparing joining cluster.
   */
  default boolean isReadyForPreparingJoiningCluster(String clusterName, String instancesNames) {
    throw new UnsupportedOperationException(
        "isReadyForPreparingJoiningCluster is not implemented.");
  }

  /**
   * WARNING: THIS METHOD WILL FORCEFULLY DROP THE NODE FROM THE EXTERNAL VIEW WITHOUT WAITING FOR IT TO PROCESS
   * ANY DOWNWARD STATE TRANSITIONS. USE WITH CAUTION AS THE NODE WILL KEEP ITS CURRENT STATES LOCALLY AND NOT DOWNWARD
   * ST UNTIL IT RECONNECTS UNDER A NEW SESSION AND RESETS TO THE INITIALSTATE.
   * Attempt to forcefully kill an instance in the cluster. This will remove the instance's
   * LIVEINSTANCES znode, causing the controller to treat the instance as offline. The instance
   * will not receive any state transitions until it rejoins the cluster. If the instance's
   * current ZK session expires, then it will rejoin the cluster automatically.
   */
  default boolean forceKillInstance(String clusterName, String instanceName) {
    throw new UnsupportedOperationException("forceKillInstance is not implemented.");
  }

  default boolean forceKillInstance(String clusterName, String instanceName, String reason,
      InstanceConstants.InstanceOperationSource operationSource) {
    throw new UnsupportedOperationException("forceKillInstance is not implemented.");
  }

}
