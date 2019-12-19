package org.apache.helix.controller.rebalancer.waged.model;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;

/**
 * This util class generates Cluster Model object based on the controller's data cache.
 */
public class ClusterModelProvider {

  private enum RebalanceScopeType {
    // Set the rebalance scope to cover the difference between the current assignment and the
    // Baseline assignment only.
    PARTIAL,
    // Set the rebalance scope to cover all replicas that need relocation based on the cluster
    // changes.
    GLOBAL_BASELINE
  }

  /**
   * Generate a new Cluster Model object according to the current cluster status for partial
   * rebalance. The rebalance scope is configured for recovering the missing replicas that are in
   * the Baseline assignment but not in the current Best possible assignment only.
   * @param dataProvider           The controller's data cache.
   * @param resourceMap            The full list of the resources to be rebalanced. Note that any
   *                               resources that are not in this list will be removed from the
   *                               final assignment.
   * @param activeInstances        The active instances that will be used in the calculation.
   *                               Note this list can be different from the real active node list
   *                               according to the rebalancer logic.
   * @param baselineAssignment     The persisted Baseline assignment.
   * @param bestPossibleAssignment The persisted Best Possible assignment that was generated in the
   *                               previous rebalance.
   * @return
   */
  public static ClusterModel generateClusterModelForPartialRebalance(
      ResourceControllerDataProvider dataProvider, Map<String, Resource> resourceMap,
      Set<String> activeInstances, Map<String, ResourceAssignment> baselineAssignment,
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    return generateClusterModel(dataProvider, resourceMap, activeInstances, Collections.emptyMap(),
        baselineAssignment, bestPossibleAssignment, RebalanceScopeType.PARTIAL);
  }

  /**
   * Generate a new Cluster Model object according to the current cluster status for the Baseline
   * calculation. The rebalance scope is determined according to the cluster changes.
   * @param dataProvider           The controller's data cache.
   * @param resourceMap            The full list of the resources to be rebalanced. Note that any
   *                               resources that are not in this list will be removed from the
   *                               final assignment.
   * @param allInstances           All the instances that will be used in the calculation.
   * @param clusterChanges         All the cluster changes that happened after the previous rebalance.
   * @param baselineAssignment     The previous Baseline assignment.
   * @return the new cluster model
   */
  public static ClusterModel generateClusterModelForBaseline(
      ResourceControllerDataProvider dataProvider, Map<String, Resource> resourceMap,
      Set<String> allInstances, Map<HelixConstants.ChangeType, Set<String>> clusterChanges,
      Map<String, ResourceAssignment> baselineAssignment) {
    return generateClusterModel(dataProvider, resourceMap, allInstances, clusterChanges,
        Collections.emptyMap(), baselineAssignment, RebalanceScopeType.GLOBAL_BASELINE);
  }

  /**
   * Generate a cluster model based on the current state output and data cache. The rebalance scope
   * is configured for recovering the missing replicas only.
   * @param dataProvider           The controller's data cache.
   * @param resourceMap            The full list of the resources to be rebalanced. Note that any
   *                               resources that are not in this list will be removed from the
   *                               final assignment.
   * @param currentStateAssignment The resource assignment built from current state output.
   * @return the new cluster model
   */
  public static ClusterModel generateClusterModelFromExistingAssignment(
      ResourceControllerDataProvider dataProvider, Map<String, Resource> resourceMap,
      Map<String, ResourceAssignment> currentStateAssignment) {
    return generateClusterModel(dataProvider, resourceMap, dataProvider.getEnabledLiveInstances(),
        Collections.emptyMap(), Collections.emptyMap(), currentStateAssignment,
        RebalanceScopeType.GLOBAL_BASELINE);
  }

  /**
   * Generate a new Cluster Model object according to the current cluster status.
   * @param dataProvider           The controller's data cache.
   * @param resourceMap            The full list of the resources to be rebalanced. Note that any
   *                               resources that are not in this list will be removed from the
   *                               final assignment.
   * @param activeInstances        The active instances that will be used in the calculation.
   *                               Note this list can be different from the real active node list
   *                               according to the rebalancer logic.
   * @param clusterChanges         All the cluster changes that happened after the previous rebalance.
   * @param idealAssignment        The ideal assignment.
   * @param currentAssignment      The current assignment that was generated in the previous rebalance.
   * @param scopeType              Specify how to determine the rebalance scope.
   * @return the new cluster model
   */
  private static ClusterModel generateClusterModel(ResourceControllerDataProvider dataProvider,
      Map<String, Resource> resourceMap, Set<String> activeInstances,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges,
      Map<String, ResourceAssignment> idealAssignment,
      Map<String, ResourceAssignment> currentAssignment, RebalanceScopeType scopeType) {
    // Construct all the assignable nodes and initialize with the allocated replicas.
    Set<AssignableNode> assignableNodes =
        parseAllNodes(dataProvider.getClusterConfig(), dataProvider.getInstanceConfigMap(),
            activeInstances);

    // Generate replica objects for all the resource partitions.
    // <resource, replica set>
    Map<String, Set<AssignableReplica>> replicaMap =
        parseAllReplicas(dataProvider, resourceMap, assignableNodes);

    // Check if the replicas need to be reassigned.
    Map<String, Set<AssignableReplica>> allocatedReplicas =
        new HashMap<>(); // <instanceName, replica set>
    Set<AssignableReplica> toBeAssignedReplicas;
    switch (scopeType) {
      case GLOBAL_BASELINE:
        toBeAssignedReplicas = findToBeAssignedReplicasByClusterChanges(replicaMap, activeInstances,
            dataProvider.getLiveInstances().keySet(), clusterChanges, currentAssignment,
            allocatedReplicas);
        break;
      case PARTIAL:
        // Filter to remove the replicas that do not exist in the ideal assignment given but exist
        // in the replicaMap. This is because such replicas are new additions that do not need to be
        // rebalanced right away.
        retainExistingReplicas(replicaMap, idealAssignment);
        toBeAssignedReplicas =
            findToBeAssignedReplicasByComparingWithIdealAssignment(replicaMap, activeInstances,
                idealAssignment, currentAssignment, allocatedReplicas);
        break;
      default:
        throw new HelixException("Unknown rebalance scope type: " + scopeType);
    }

    // Update the allocated replicas to the assignable nodes.
    assignableNodes.parallelStream().forEach(node -> node.assignInitBatch(
        allocatedReplicas.getOrDefault(node.getInstanceName(), Collections.emptySet())));

    // Construct and initialize cluster context.
    ClusterContext context = new ClusterContext(
        replicaMap.values().stream().flatMap(Set::stream).collect(Collectors.toSet()),
        assignableNodes, idealAssignment, currentAssignment);
    // Initial the cluster context with the allocated assignments.
    context.setAssignmentForFaultZoneMap(mapAssignmentToFaultZone(assignableNodes));

    return new ClusterModel(context, toBeAssignedReplicas, assignableNodes);
  }

  // Filter the replicas map so only the replicas that have been allocated in the existing
  // assignmentMap remain in the map.
  private static void retainExistingReplicas(Map<String, Set<AssignableReplica>> replicaMap,
      Map<String, ResourceAssignment> assignmentMap) {
    replicaMap.entrySet().parallelStream().forEach(replicaSetEntry -> {
      // <partition, <state, instances set>>
      Map<String, Map<String, Set<String>>> stateInstanceMap =
          getStateInstanceMap(assignmentMap.get(replicaSetEntry.getKey()));
      // Iterate the replicas of the resource to find the ones that require reallocating.
      Iterator<AssignableReplica> replicaIter = replicaSetEntry.getValue().iterator();
      while (replicaIter.hasNext()) {
        AssignableReplica replica = replicaIter.next();
        Set<String> validInstances =
            stateInstanceMap.getOrDefault(replica.getPartitionName(), Collections.emptyMap())
                .getOrDefault(replica.getReplicaState(), Collections.emptySet());
        if (validInstances.isEmpty()) {
          // Removing the replica if it is not known in the assignment map.
          replicaIter.remove();
        } else {
          // Remove the instance from the state map record after processing so it won't be
          // double-processed as we loop through all replica
          validInstances.remove(validInstances.iterator().next());
        }
      }
    });
  }

  /**
   * Find the minimum set of replicas that need to be reassigned by comparing the current assignment
   * with the ideal assignment.
   * A replica needs to be reassigned or newly assigned if either of the following conditions is true:
   * 1. The partition allocation (the instance the replica is placed on) in the ideal assignment and
   * the current assignment are different. And the allocation in the ideal assignment is valid.
   * So it is worthwhile to move it.
   * 2. The partition allocation is in neither the ideal assignment nor the current assignment. Or
   * those allocations are not valid due to offline or disabled instances.
   * Otherwise, the rebalancer just keeps the current assignment allocation.
   *
   * @param replicaMap             A map contains all the replicas grouped by resource name.
   * @param activeInstances        All the instances that are live and enabled according to the delay rebalance configuration.
   * @param idealAssignment        The ideal assignment.
   * @param currentAssignment      The current assignment that was generated in the previous rebalance.
   * @param allocatedReplicas      A map of <Instance -> replicas> to return the allocated replicas grouped by the target instance name.
   * @return The replicas that need to be reassigned.
   */
  private static Set<AssignableReplica> findToBeAssignedReplicasByComparingWithIdealAssignment(
      Map<String, Set<AssignableReplica>> replicaMap, Set<String> activeInstances,
      Map<String, ResourceAssignment> idealAssignment,
      Map<String, ResourceAssignment> currentAssignment,
      Map<String, Set<AssignableReplica>> allocatedReplicas) {
    Set<AssignableReplica> toBeAssignedReplicas = new HashSet<>();
    // check each resource to identify the allocated replicas and to-be-assigned replicas.
    for (String resourceName : replicaMap.keySet()) {
      // <partition, <state, instances set>>
      Map<String, Map<String, Set<String>>> idealPartitionStateMap =
          getValidStateInstanceMap(idealAssignment.get(resourceName), activeInstances);
      Map<String, Map<String, Set<String>>> currentPartitionStateMap =
          getValidStateInstanceMap(currentAssignment.get(resourceName), activeInstances);
      // Iterate the replicas of the resource to find the ones that require reallocating.
      for (AssignableReplica replica : replicaMap.get(resourceName)) {
        String partitionName = replica.getPartitionName();
        String replicaState = replica.getReplicaState();
        Set<String> idealAllocations =
            idealPartitionStateMap.getOrDefault(partitionName, Collections.emptyMap())
                .getOrDefault(replicaState, Collections.emptySet());
        Set<String> currentAllocations =
            currentPartitionStateMap.getOrDefault(partitionName, Collections.emptyMap())
                .getOrDefault(replicaState, Collections.emptySet());

        // Compare the current assignments with the ideal assignment for the common part.
        List<String> commonAllocations = new ArrayList<>(currentAllocations);
        commonAllocations.retainAll(idealAllocations);
        if (!commonAllocations.isEmpty()) {
          // 1. If the partition is allocated at the same location in both ideal and current
          // assignments, there is no need to reassign it.
          String allocatedInstance = commonAllocations.get(0);
          allocatedReplicas.computeIfAbsent(allocatedInstance, key -> new HashSet<>()).add(replica);
          // Remove the instance from the record to prevent this instance from being processed twice.
          idealAllocations.remove(allocatedInstance);
          currentAllocations.remove(allocatedInstance);
        } else if (!idealAllocations.isEmpty()) {
          // 2. If the partition is allocated at an active instance in the ideal assignment but the
          // same allocation does not exist in the current assignment, try to rebalance the replica
          // or assign it if the replica has not been assigned.
          // There are two possible conditions,
          // * This replica has been newly added and has not been assigned yet, so it appears in
          // the ideal assignment and does not appear in the current assignment.
          // * The allocation of this replica in the ideal assignment has been updated due to a
          // cluster change. For example, new instance is added. So the old allocation in the
          // current assignment might be sub-optimal.
          // In either condition, we add it to toBeAssignedReplicas so that it will get assigned.
          toBeAssignedReplicas.add(replica);
          // Remove the pending allocation from the idealAllocations after processing so that the
          // instance won't be double-processed as we loop through all replicas
          String pendingAllocation = idealAllocations.iterator().next();
          idealAllocations.remove(pendingAllocation);
        } else if (!currentAllocations.isEmpty()) {
          // 3. This replica exists in the current assignment but does not appear or has a valid
          // allocation in the ideal assignment.
          // This means either 1) that the ideal assignment actually has this replica allocated on
          // this instance, but it does not show up because the instance is temporarily offline or
          // disabled (note that all such instances have been filtered out in earlier part of the
          // logic) or that the most recent version of the ideal assignment was not fetched
          // correctly from the assignment metadata store.
          // In either case, the solution is to keep the current assignment. So put this replica
          // with the allocated instance into the allocatedReplicas map.
          String allocatedInstance = currentAllocations.iterator().next();
          allocatedReplicas.computeIfAbsent(allocatedInstance, key -> new HashSet<>()).add(replica);
          // Remove the instance from the record to prevent the same location being processed again.
          currentAllocations.remove(allocatedInstance);
        } else {
          // 4. This replica is not found in either the ideal assignment or the current assignment
          // with a valid allocation. This implies that the replica was newly added but was never
          // assigned in reality or was added so recently that it hasn't shown up in the ideal
          // assignment (because it's calculation takes longer and is asynchronously calculated).
          // In that case, we add it to toBeAssignedReplicas so that it will get assigned as a
          // result of partialRebalance.
          toBeAssignedReplicas.add(replica);
        }
      }
    }
    return toBeAssignedReplicas;
  }

  /**
   * Find the minimum set of replicas that need to be reassigned according to the cluster change.
   * A replica needs to be reassigned if one of the following condition is true:
   * 1. Cluster topology (the cluster config / any instance config) has been updated.
   * 2. The resource config has been updated.
   * 3. If the current assignment does not contain the partition's valid assignment.
   *
   * @param replicaMap             A map contains all the replicas grouped by resource name.
   * @param activeInstances        All the instances that are live and enabled according to the delay rebalance configuration.
   * @param liveInstances          All the instances that are live.
   * @param clusterChanges         A map that contains all the important metadata updates that happened after the previous rebalance.
   * @param currentAssignment      The current replica assignment.
   * @param allocatedReplicas      Return the allocated replicas grouped by the target instance name.
   * @return The replicas that need to be reassigned.
   */
  private static Set<AssignableReplica> findToBeAssignedReplicasByClusterChanges(
      Map<String, Set<AssignableReplica>> replicaMap, Set<String> activeInstances,
      Set<String> liveInstances, Map<HelixConstants.ChangeType, Set<String>> clusterChanges,
      Map<String, ResourceAssignment> currentAssignment,
      Map<String, Set<AssignableReplica>> allocatedReplicas) {
    Set<AssignableReplica> toBeAssignedReplicas = new HashSet<>();

    // A newly connected node = A new LiveInstance znode (or session Id updated) & the
    // corresponding instance is live.
    // TODO: The assumption here is that if the LiveInstance znode is created or it's session Id is
    // TODO: updated, we need to call algorithm for moving some partitions to this new node.
    // TODO: However, if the liveInstance znode is changed because of some other reason, it will be
    // TODO: treated as a newly connected nodes. We need to find a better way to identify which one
    // TODO: is the real newly connected nodes.
    Set<String> newlyConnectedNodes = clusterChanges
        .getOrDefault(HelixConstants.ChangeType.LIVE_INSTANCE, Collections.emptySet());
    newlyConnectedNodes.retainAll(liveInstances);
    if (clusterChanges.containsKey(HelixConstants.ChangeType.CLUSTER_CONFIG) || clusterChanges
        .containsKey(HelixConstants.ChangeType.INSTANCE_CONFIG) || !newlyConnectedNodes.isEmpty()) {
      // 1. If the cluster topology has been modified, need to reassign all replicas.
      // 2. If any node was newly connected, need to rebalance all replicas for the evenness of
      // distribution.
      toBeAssignedReplicas
          .addAll(replicaMap.values().stream().flatMap(Set::stream).collect(Collectors.toSet()));
    } else {
      // check each resource to identify the allocated replicas and to-be-assigned replicas.
      for (Map.Entry<String, Set<AssignableReplica>> replicaMapEntry : replicaMap.entrySet()) {
        String resourceName = replicaMapEntry.getKey();
        Set<AssignableReplica> replicas = replicaMapEntry.getValue();
        // 1. if the resource config/idealstate is changed, need to reassign.
        // 2. if the resource does not appear in the current assignment, need to reassign.
        if (clusterChanges
            .getOrDefault(HelixConstants.ChangeType.RESOURCE_CONFIG, Collections.emptySet())
            .contains(resourceName) || clusterChanges
            .getOrDefault(HelixConstants.ChangeType.IDEAL_STATE, Collections.emptySet())
            .contains(resourceName) || !currentAssignment.containsKey(resourceName)) {
          toBeAssignedReplicas.addAll(replicas);
          continue; // go to check next resource
        } else {
          // check for every replica assignment to identify if the related replicas need to be reassigned.
          // <partition, <state, instances list>>
          Map<String, Map<String, Set<String>>> stateMap =
              getValidStateInstanceMap(currentAssignment.get(resourceName), activeInstances);
          for (AssignableReplica replica : replicas) {
            // Find any ACTIVE instance allocation that has the same state with the replica
            Set<String> validInstances =
                stateMap.getOrDefault(replica.getPartitionName(), Collections.emptyMap())
                    .getOrDefault(replica.getReplicaState(), Collections.emptySet());
            if (validInstances.isEmpty()) {
              // 3. if no such an instance in the current assignment, need to reassign the replica
              toBeAssignedReplicas.add(replica);
              continue; // go to check the next replica
            } else {
              Iterator<String> iter = validInstances.iterator();
              // Remove the instance from the current allocation record after processing so that it
              // won't be double-processed as we loop through all replicas
              String instanceName = iter.next();
              iter.remove();
              // the current assignment for this replica is valid,
              // add to the allocated replica list.
              allocatedReplicas.computeIfAbsent(instanceName, key -> new HashSet<>()).add(replica);
            }
          }
        }
      }
    }
    return toBeAssignedReplicas;
  }

  /**
   * Filter to remove all invalid allocations that are not on the active instances.
   * @param assignment
   * @param activeInstances
   * @return A map of <partition, <state, instances set>> contains the valid state to instance map.
   */
  private static Map<String, Map<String, Set<String>>> getValidStateInstanceMap(
      ResourceAssignment assignment, Set<String> activeInstances) {
    Map<String, Map<String, Set<String>>> stateInstanceMap = getStateInstanceMap(assignment);
    stateInstanceMap.values().stream().forEach(stateMap -> stateMap.values().stream()
        .forEach(instanceSet -> instanceSet.retainAll(activeInstances)));
    return stateInstanceMap;
  }

  // <partition, <state, instances set>>
  private static Map<String, Map<String, Set<String>>> getStateInstanceMap(
      ResourceAssignment assignment) {
    if (assignment == null) {
      return Collections.emptyMap();
    }
    return assignment.getMappedPartitions().stream()
        .collect(Collectors.toMap(partition -> partition.getPartitionName(), partition -> {
          Map<String, Set<String>> stateInstanceMap = new HashMap<>();
          assignment.getReplicaMap(partition).entrySet().stream().forEach(
              stateMapEntry -> stateInstanceMap
                  .computeIfAbsent(stateMapEntry.getValue(), key -> new HashSet<>())
                  .add(stateMapEntry.getKey()));
          return stateInstanceMap;
        }));
  }

  /**
   * Parse all the nodes that can be assigned replicas based on the configurations.
   *
   * @param clusterConfig     The cluster configuration.
   * @param instanceConfigMap A map of all the instance configuration.
   *                          If any active instance has no configuration, it will be ignored.
   * @param activeInstances   All the instances that are online and enabled.
   * @return A map of assignable node set, <InstanceName, node set>.
   */
  private static Set<AssignableNode> parseAllNodes(ClusterConfig clusterConfig,
      Map<String, InstanceConfig> instanceConfigMap, Set<String> activeInstances) {
    return activeInstances.parallelStream()
        .filter(instance -> instanceConfigMap.containsKey(instance)).map(
            instanceName -> new AssignableNode(clusterConfig, instanceConfigMap.get(instanceName),
                instanceName)).collect(Collectors.toSet());
  }

  /**
   * Parse all the replicas that need to be reallocated from the cluster data cache.
   *
   * @param dataProvider The cluster status cache that contains the current cluster status.
   * @param resourceMap  All the valid resources that are managed by the rebalancer.
   * @param assignableNodes All the active assignable nodes.
   * @return A map of assignable replica set, <ResourceName, replica set>.
   */
  private static Map<String, Set<AssignableReplica>> parseAllReplicas(
      ResourceControllerDataProvider dataProvider, Map<String, Resource> resourceMap,
      Set<AssignableNode> assignableNodes) {
    ClusterConfig clusterConfig = dataProvider.getClusterConfig();
    int activeFaultZoneCount = assignableNodes.stream().map(node -> node.getFaultZone())
        .collect(Collectors.toSet()).size();
    return resourceMap.keySet().parallelStream().map(resourceName -> {
      ResourceConfig resourceConfig = dataProvider.getResourceConfig(resourceName);
      if (resourceConfig == null) {
        resourceConfig = new ResourceConfig(resourceName);
      }
      IdealState is = dataProvider.getIdealState(resourceName);
      if (is == null) {
        throw new HelixException(
            "Cannot find the resource ideal state for resource: " + resourceName);
      }
      String defName = is.getStateModelDefRef();
      StateModelDefinition def = dataProvider.getStateModelDef(defName);
      if (def == null) {
        throw new IllegalArgumentException(String
            .format("Cannot find state model definition %s for resource %s.",
                is.getStateModelDefRef(), resourceName));
      }
      Map<String, Integer> stateCountMap =
          def.getStateCountMap(activeFaultZoneCount, is.getReplicaCount(assignableNodes.size()));
      mergeIdealStateWithResourceConfig(resourceConfig, is);
      Set<AssignableReplica> replicas = new HashSet<>();
      for (String partition : is.getPartitionSet()) {
        for (Map.Entry<String, Integer> entry : stateCountMap.entrySet()) {
          String state = entry.getKey();
          for (int i = 0; i < entry.getValue(); i++) {
            replicas.add(new AssignableReplica(clusterConfig, resourceConfig, partition, state,
                def.getStatePriorityMap().get(state)));
          }
        }
      }
      return new HashMap.SimpleEntry<>(resourceName, replicas);
    }).collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
  }

  /**
   * For backward compatibility, propagate the critical simple fields from the IdealState to
   * the Resource Config.
   * Eventually, Resource Config should be the only metadata node that contains the required information.
   */
  private static void mergeIdealStateWithResourceConfig(ResourceConfig resourceConfig,
      final IdealState idealState) {
    // Note that the config fields get updated in this method shall be fully compatible with ones in the IdealState.
    // 1. The fields shall have exactly the same meaning.
    // 2. The value shall be exactly compatible, no additional calculation involved.
    // 3. Resource Config items have a high priority.
    // This is to ensure the resource config is not polluted after the merge.
    if (null == resourceConfig.getRecord()
        .getSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name())) {
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
              idealState.getInstanceGroupTag());
    }
    if (null == resourceConfig.getRecord()
        .getSimpleField(ResourceConfig.ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name())) {
      resourceConfig.getRecord()
          .setIntField(ResourceConfig.ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(),
              idealState.getMaxPartitionsPerInstance());
    }
  }

  /**
   * @return A map contains the assignments for each fault zone. <fault zone, <resource, set of partitions>>
   */
  private static Map<String, Map<String, Set<String>>> mapAssignmentToFaultZone(
      Set<AssignableNode> assignableNodes) {
    Map<String, Map<String, Set<String>>> faultZoneAssignmentMap = new HashMap<>();
    assignableNodes.stream().forEach(node -> {
      for (Map.Entry<String, Set<String>> resourceMap : node.getAssignedPartitionsMap()
          .entrySet()) {
        faultZoneAssignmentMap.computeIfAbsent(node.getFaultZone(), k -> new HashMap<>())
            .computeIfAbsent(resourceMap.getKey(), k -> new HashSet<>())
            .addAll(resourceMap.getValue());
      }
    });
    return faultZoneAssignmentMap;
  }
}
