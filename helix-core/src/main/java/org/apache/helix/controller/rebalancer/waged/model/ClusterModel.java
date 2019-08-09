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

import org.apache.helix.HelixException;
import org.apache.helix.model.ResourceAssignment;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class wraps the required input for the rebalance algorithm.
 */
public class ClusterModel {
  private final ClusterContext _clusterContext;
  // Map to track all the assignable replications. <Resource Name, Set<Replicas>>
  private final Map<String, Set<AssignableReplica>> _assignableReplicaMap;
  // The index to find the replication information with a certain state. <Resource, <Key(resource_partition_state), Replica>>
  // Note that the identical replicas are deduped in the index.
  private final Map<String, Map<String, AssignableReplica>> _assignableReplicaIndex;
  private final Map<String, AssignableNode> _assignableNodeMap;

  // Records about the previous assignment
  // <ResourceName, ResourceAssignment contains the baseline assignment>
  private final Map<String, ResourceAssignment> _baselineAssignment;
  // <ResourceName, ResourceAssignment contains the best possible assignment>
  private final Map<String, ResourceAssignment> _bestPossibleAssignment;

  /**
   * @param clusterContext         The initialized cluster context.
   * @param assignableReplicas     The replicas to be assigned.
   *                               Note that the replicas in this list shall not be included while initializing the context and assignable nodes.
   * @param assignableNodes        The active instances.
   * @param baselineAssignment     The recorded baseline assignment.
   * @param bestPossibleAssignment The current best possible assignment.
   */
  ClusterModel(ClusterContext clusterContext, Set<AssignableReplica> assignableReplicas,
      Set<AssignableNode> assignableNodes, Map<String, ResourceAssignment> baselineAssignment,
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    _clusterContext = clusterContext;

    // Save all the to be assigned replication
    _assignableReplicaMap = assignableReplicas.stream()
        .collect(Collectors.groupingBy(AssignableReplica::getResourceName, Collectors.toSet()));

    // Index all the replicas to be assigned. Dedup the replica if two instances have the same resource/partition/state
    _assignableReplicaIndex = assignableReplicas.stream().collect(Collectors
        .groupingBy(AssignableReplica::getResourceName, Collectors
            .toMap(AssignableReplica::toString, replica -> replica,
                (oldValue, newValue) -> oldValue)));

    _assignableNodeMap = assignableNodes.stream()
        .collect(Collectors.toMap(AssignableNode::getInstanceName, node -> node));

    _baselineAssignment = baselineAssignment;
    _bestPossibleAssignment = bestPossibleAssignment;
  }

  public ClusterContext getContext() {
    return _clusterContext;
  }

  public Map<String, AssignableNode> getAssignableNodes() {
    return _assignableNodeMap;
  }

  public Map<String, Set<AssignableReplica>> getAssignableReplicaMap() {
    return _assignableReplicaMap;
  }

  public Map<String, ResourceAssignment> getBaseline() {
    return _baselineAssignment;
  }

  public Map<String, ResourceAssignment> getBestPossibleAssignment() {
    return _bestPossibleAssignment;
  }

  /**
   * Assign the given replica to the specified instance and record the assignment in the cluster model.
   * The cluster usage information will be updated accordingly.
   *
   * @param resourceName
   * @param partitionName
   * @param state
   * @param instanceName
   */
  public void assign(String resourceName, String partitionName, String state, String instanceName) {
    AssignableNode node = locateAssignableNode(instanceName);
    AssignableReplica replica = locateAssignableReplica(resourceName, partitionName, state);

    node.assign(replica);
    _clusterContext.addPartitionToFaultZone(node.getFaultZone(), resourceName, partitionName);
  }

  /**
   * Revert the proposed assignment from the cluster model.
   * The cluster usage information will be updated accordingly.
   *
   * @param resourceName
   * @param partitionName
   * @param state
   * @param instanceName
   */
  public void release(String resourceName, String partitionName, String state,
      String instanceName) {
    AssignableNode node = locateAssignableNode(instanceName);
    AssignableReplica replica = locateAssignableReplica(resourceName, partitionName, state);

    node.release(replica);
    _clusterContext.removePartitionFromFaultZone(node.getFaultZone(), resourceName, partitionName);
  }

  private AssignableNode locateAssignableNode(String instanceName) {
    AssignableNode node = _assignableNodeMap.get(instanceName);
    if (node == null) {
      throw new HelixException("Cannot find the instance: " + instanceName);
    }
    return node;
  }

  private AssignableReplica locateAssignableReplica(String resourceName, String partitionName,
      String state) {
    AssignableReplica sampleReplica =
        _assignableReplicaIndex.getOrDefault(resourceName, Collections.emptyMap())
            .get(AssignableReplica.generateReplicaKey(resourceName, partitionName, state));
    if (sampleReplica == null) {
      throw new HelixException(String
          .format("Cannot find the replication with resource name %s, partition name %s, state %s.",
              resourceName, partitionName, state));
    }
    return sampleReplica;
  }
}
