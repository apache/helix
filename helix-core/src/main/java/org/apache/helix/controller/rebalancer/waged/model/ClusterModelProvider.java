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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
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

  /**
   * @param dataProvider           The controller's data cache.
   * @param resourceMap            The full list of the resources to be rebalanced. Note that any
   *                               resources that are not in this list will be removed from the
   *                               final assignment.
   * @param activeInstances        The active instances that will be used in the calculation.
   *                               Note this list can be different from the real active node list
   *                               according to the rebalancer logic.
   * @param clusterChanges         All the cluster changes that happened after the previous rebalance.
   * @param baselineAssignment     The persisted Baseline assignment.
   * @param bestPossibleAssignment The persisted Best Possible assignment that was generated in the
   *                               previous rebalance.
   * @return Generate a new Cluster Model object according to the current cluster status.
   */
  public static ClusterModel generateClusterModel(ResourceControllerDataProvider dataProvider,
      Map<String, Resource> resourceMap, Set<String> activeInstances,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges,
      Map<String, ResourceAssignment> baselineAssignment,
      Map<String, ResourceAssignment> bestPossibleAssignment) {
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
    Set<AssignableReplica> toBeAssignedReplicas =
        findToBeAssignedReplicas(replicaMap, clusterChanges, activeInstances,
            bestPossibleAssignment, allocatedReplicas);

    // Update the allocated replicas to the assignable nodes.
    assignableNodes.stream().forEach(node -> node.assignInitBatch(
        allocatedReplicas.getOrDefault(node.getInstanceName(), Collections.emptySet())));

    // Construct and initialize cluster context.
    ClusterContext context = new ClusterContext(
        replicaMap.values().stream().flatMap(Set::stream).collect(Collectors.toSet()),
        activeInstances.size(), baselineAssignment, bestPossibleAssignment);
    // Initial the cluster context with the allocated assignments.
    context.setAssignmentForFaultZoneMap(mapAssignmentToFaultZone(assignableNodes));

    return new ClusterModel(context, toBeAssignedReplicas, assignableNodes);
  }

  /**
   * Find the minimum set of replicas that need to be reassigned.
   * A replica needs to be reassigned if one of the following condition is true:
   * 1. Cluster topology (the cluster config / any instance config) has been updated.
   * 2. The baseline assignment has been updated.
   * 3. The resource config has been updated.
   * 4. The resource idealstate has been updated. TODO remove this condition when all resource configurations are migrated to resource config.
   * 5. If the current best possible assignment does not contain the partition's valid assignment.
   *
   * @param replicaMap             A map contains all the replicas grouped by resource name.
   * @param clusterChanges         A map contains all the important metadata updates that happened after the previous rebalance.
   * @param activeInstances        All the instances that are alive and enabled.
   * @param bestPossibleAssignment The current best possible assignment.
   * @param allocatedReplicas      Return the allocated replicas grouped by the target instance name.
   * @return The replicas that need to be reassigned.
   */
  private static Set<AssignableReplica> findToBeAssignedReplicas(
      Map<String, Set<AssignableReplica>> replicaMap,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Set<String> activeInstances,
      Map<String, ResourceAssignment> bestPossibleAssignment,
      Map<String, Set<AssignableReplica>> allocatedReplicas) {
    Set<AssignableReplica> toBeAssignedReplicas = new HashSet<>();
    if (clusterChanges.containsKey(HelixConstants.ChangeType.CLUSTER_CONFIG) || clusterChanges
        .containsKey(HelixConstants.ChangeType.INSTANCE_CONFIG)) {
      // If the cluster topology has been modified, need to reassign all replicas
      toBeAssignedReplicas
          .addAll(replicaMap.values().stream().flatMap(Set::stream).collect(Collectors.toSet()));
    } else {
      // check each resource to identify the allocated replicas and to-be-assigned replicas.
      for (String resourceName : replicaMap.keySet()) {
        Set<AssignableReplica> replicas = replicaMap.get(resourceName);
        // 1. if the resource config/idealstate is changed, need to reassign.
        // 2. if the resource does appear in the best possible assignment, need to reassign.
        if (clusterChanges
            .getOrDefault(HelixConstants.ChangeType.RESOURCE_CONFIG, Collections.emptySet())
            .contains(resourceName) || clusterChanges
            .getOrDefault(HelixConstants.ChangeType.IDEAL_STATE, Collections.emptySet())
            .contains(resourceName) || !bestPossibleAssignment.containsKey(resourceName)) {
          toBeAssignedReplicas.addAll(replicas);
          continue; // go to check next resource
        } else {
          // check for every best possible assignments to identify if the related replicas need to reassign.
          ResourceAssignment assignment = bestPossibleAssignment.get(resourceName);
          // <partition, <instance, state>>
          Map<String, Map<String, String>> stateMap = assignment.getMappedPartitions().stream()
              .collect(Collectors.toMap(partition -> partition.getPartitionName(),
                  partition -> new HashMap<>(assignment.getReplicaMap(partition))));
          for (AssignableReplica replica : replicas) {
            // Find any ACTIVE instance allocation that has the same state with the replica
            Optional<Map.Entry<String, String>> instanceNameOptional =
                stateMap.getOrDefault(replica.getPartitionName(), Collections.emptyMap()).entrySet()
                    .stream().filter(instanceStateMap ->
                    instanceStateMap.getValue().equals(replica.getReplicaState()) && activeInstances
                        .contains(instanceStateMap.getKey())).findAny();
            // 3. if no such an instance in the bestPossible assignment, need to reassign the replica
            if (!instanceNameOptional.isPresent()) {
              toBeAssignedReplicas.add(replica);
              continue; // go to check the next replica
            } else {
              String instanceName = instanceNameOptional.get().getKey();
              // * cleanup the best possible state map record,
              // * so the selected instance won't be picked up again for the another replica check
              stateMap.getOrDefault(replica.getPartitionName(), Collections.emptyMap())
                  .remove(instanceName);
              // the current best possible assignment for this replica is valid,
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
   * Parse all the nodes that can be assigned replicas based on the configurations.
   *
   * @param clusterConfig     The cluster configuration.
   * @param instanceConfigMap A map of all the instance configuration.
   * @param activeInstances   All the instances that are online and enabled.
   * @return A map of assignable node set, <InstanceName, node set>.
   */
  private static Set<AssignableNode> parseAllNodes(ClusterConfig clusterConfig,
      Map<String, InstanceConfig> instanceConfigMap, Set<String> activeInstances) {
    return activeInstances.stream().map(
        instanceName -> new AssignableNode(clusterConfig, instanceConfigMap.get(instanceName),
            instanceName))
        .collect(Collectors.toSet());
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
    Map<String, Set<AssignableReplica>> totalReplicaMap = new HashMap<>();
    ClusterConfig clusterConfig = dataProvider.getClusterConfig();

    for (String resourceName : resourceMap.keySet()) {
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

      int activeFaultZoneCount =
          assignableNodes.stream().map(node -> node.getFaultZone()).collect(Collectors.toSet())
              .size();
      Map<String, Integer> stateCountMap =
          def.getStateCountMap(activeFaultZoneCount, is.getReplicaCount(assignableNodes.size()));

      for (String partition : is.getPartitionSet()) {
        for (Map.Entry<String, Integer> entry : stateCountMap.entrySet()) {
          String state = entry.getKey();
          for (int i = 0; i < entry.getValue(); i++) {
            mergeIdealStateWithResourceConfig(resourceConfig, is);
            totalReplicaMap.computeIfAbsent(resourceName, key -> new HashSet<>()).add(
                new AssignableReplica(clusterConfig, resourceConfig, partition, state,
                    def.getStatePriorityMap().get(state)));
          }
        }
      }
    }
    return totalReplicaMap;
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
