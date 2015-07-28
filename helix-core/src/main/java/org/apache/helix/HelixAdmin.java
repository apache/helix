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
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
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
   * Get a list of resources in a cluster
   * @param clusterName
   * @return a list of resource names in the cluster
   */
  List<String> getResourcesInCluster(String clusterName);

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

  /**
   * Add a resource to a cluster, using the default ideal state mode AUTO
   * @param clusterName
   * @param resourceName
   * @param numPartitions
   * @param stateModelRef
   */
  void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef);

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
  void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef, String rebalancerMode);

  /**
   * Add a resource to a cluster, using a bucket size > 1
   * @param clusterName
   * @param resourceName
   * @param numPartitions
   * @param stateModelRef
   * @param rebalancerMode
   * @param bucketSize
   */
  void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef, String rebalancerMode, int bucketSize);

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
  void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef, String rebalancerMode, int bucketSize, int maxPartitionsPerInstance);

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
   * Disable or enable an instance
   * @param clusterName
   * @param instanceName
   * @param enabled
   */
  void enableInstance(String clusterName, String instanceName, boolean enabled);

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
   */
  void addStateModelDef(String clusterName, String stateModelDef, StateModelDefinition record);

  /**
   * Drop a state model definition
   * @param clusterName
   * @param stateModelDef
   */
  void dropStateModelDef(String clusterName, String stateModelDef);

  /**
   * Drop a resource from a cluster
   * @param clusterName
   * @param resourceName
   */
  void dropResource(String clusterName, String resourceName);

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
   * @param instanceNames
   * @param tag
   */
  void addInstanceTag(String clusterName, String instanceName, String tag);

  /**
   * @param clusterName
   * @param instanceNames
   * @param tag
   */
  void removeInstanceTag(String clusterName, String instanceName, String tag);

  /**
   * Release resources
   */
  void close();
}
