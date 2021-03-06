package org.apache.helix.controller.rebalancer.waged.constraints;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ResourceAssignment;


/**
 * The algorithm is based on a given set of constraints
 * - HardConstraint: Approve or deny the assignment given its condition, any assignment cannot
 * bypass any "hard constraint"
 * - SoftConstraint: Evaluate the assignment by points/rewards/scores, a higher point means a better
 * assignment
 * The goal is to accumulate the most points(rewards) from "soft constraints" while avoiding any
 * "hard constraints"
 */
class ConstraintBasedAlgorithm implements RebalanceAlgorithm {
  private final List<HardConstraint> _hardConstraints;
  private final Map<SoftConstraint, Float> _softConstraints;

  ConstraintBasedAlgorithm(List<HardConstraint> hardConstraints,
      Map<SoftConstraint, Float> softConstraints) {
    _hardConstraints = hardConstraints;
    _softConstraints = softConstraints;
  }

  @Override
  public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
    OptimalAssignment optimalAssignment = new OptimalAssignment();

    List<AssignableNode> nodes = new ArrayList<>(clusterModel.getAssignableNodes().values());
    Set<String> busyInstances =
        getBusyInstances(clusterModel.getContext().getBestPossibleAssignment().values());
    List<AssignableReplica> allReplicas = clusterModel.getAssignableReplicaMap().values().stream()
        .flatMap(replicas -> replicas.stream()).collect(Collectors.toList());

    // Prioritize the replicas so the input is stable and optimized for the greedy algorithm.
    // For the other algorithm implementation, this additional sorting might be unnecessary.
    PriorityQueue<Triple<AssignableReplica, AssignableNode, Float>> replicaPriorityQueue =
        new PriorityQueue<>(new AssignmentReplicaComparator(clusterModel, allReplicas));
    // A tracking map to support updating the replica-instance impact queue after each assignment.
    Map<String, PriorityQueue<Pair<AssignableNode, Float>>> replicaImpactTrackingMap =
        new HashMap<>();
    initializeReplicaPriorityQueue(nodes, allReplicas, replicaPriorityQueue,
        replicaImpactTrackingMap);

    while (!replicaPriorityQueue.isEmpty()) {
      AssignableReplica replica = replicaPriorityQueue.poll().getLeft();
      Optional<AssignableNode> maybeBestNode =
          getNodeWithHighestPoints(replica, nodes, clusterModel.getContext(), busyInstances,
              optimalAssignment);
      // stop immediately if any replica cannot find best assignable node
      if (!maybeBestNode.isPresent() || optimalAssignment.hasAnyFailure()) {
        String errorMessage = String.format(
            "Unable to find any available candidate node for partition %s; Fail reasons: %s",
            replica.getPartitionName(), optimalAssignment.getFailures());
        throw new HelixRebalanceException(errorMessage,
            HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      }
      AssignableNode bestNode = maybeBestNode.get();
      // Assign the replica and update the cluster model.
      clusterModel
          .assign(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(),
              bestNode.getInstanceName());
      UpdateReplicaPriorityQueue(replicaPriorityQueue, replicaImpactTrackingMap, bestNode);
    }
    optimalAssignment.updateAssignments(clusterModel);
    return optimalAssignment;
  }

  private Optional<AssignableNode> getNodeWithHighestPoints(AssignableReplica replica,
      List<AssignableNode> assignableNodes, ClusterContext clusterContext,
      Set<String> busyInstances, OptimalAssignment optimalAssignment) {
    Map<AssignableNode, List<HardConstraint>> hardConstraintFailures = new ConcurrentHashMap<>();
    List<AssignableNode> candidateNodes = assignableNodes.parallelStream().filter(candidateNode -> {
      boolean isValid = true;
      // need to record all the failure reasons and it gives us the ability to debug/fix the runtime
      // cluster environment
      for (HardConstraint hardConstraint : _hardConstraints) {
        if (!hardConstraint.isAssignmentValid(candidateNode, replica, clusterContext)) {
          hardConstraintFailures.computeIfAbsent(candidateNode, node -> new ArrayList<>())
              .add(hardConstraint);
          isValid = false;
        }
      }
      return isValid;
    }).collect(Collectors.toList());

    if (candidateNodes.isEmpty()) {
      optimalAssignment.recordAssignmentFailure(replica,
          Maps.transformValues(hardConstraintFailures, this::convertFailureReasons));
      return Optional.empty();
    }

    return candidateNodes.parallelStream().map(node -> new HashMap.SimpleEntry<>(node,
        getAssignmentNormalizedScore(node, replica, clusterContext)))
        .max((nodeEntry1, nodeEntry2) -> {
          int scoreCompareResult = nodeEntry1.getValue().compareTo(nodeEntry2.getValue());
          if (scoreCompareResult == 0) {
            // If the evaluation scores of 2 nodes are the same, the algorithm assigns the replica
            // to the idle node first.
            String instanceName1 = nodeEntry1.getKey().getInstanceName();
            String instanceName2 = nodeEntry2.getKey().getInstanceName();
            int idleScore1 = busyInstances.contains(instanceName1) ? 0 : 1;
            int idleScore2 = busyInstances.contains(instanceName2) ? 0 : 1;
            return idleScore1 != idleScore2 ? (idleScore1 - idleScore2)
                : -instanceName1.compareTo(instanceName2);
          } else {
            return scoreCompareResult;
          }
        }).map(Map.Entry::getKey);
  }

  private double getAssignmentNormalizedScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    double sum = 0;
    for (Map.Entry<SoftConstraint, Float> softConstraintEntry : _softConstraints.entrySet()) {
      SoftConstraint softConstraint = softConstraintEntry.getKey();
      float weight = softConstraintEntry.getValue();
      if (weight != 0) {
        // Skip calculating zero weighted constraints.
        sum += weight * softConstraint.getAssignmentNormalizedScore(node, replica, clusterContext);
      }
    }
    return sum;
  }

  private List<String> convertFailureReasons(List<HardConstraint> hardConstraints) {
    return hardConstraints.stream().map(HardConstraint::getDescription)
        .collect(Collectors.toList());
  }

  /**
   * @param assignments A collection of resource replicas assignment.
   * @return A set of instance names that have at least one replica assigned in the input assignments.
   */
  private Set<String> getBusyInstances(Collection<ResourceAssignment> assignments) {
    return assignments.stream().flatMap(
        resourceAssignment -> resourceAssignment.getRecord().getMapFields().values().stream()
            .flatMap(instanceStateMap -> instanceStateMap.keySet().stream())
            .collect(Collectors.toSet()).stream()).collect(Collectors.toSet());
  }

  /**
   * Initialize the replica priority queue and the tracking map.
   */
  private void initializeReplicaPriorityQueue(List<AssignableNode> allNodes,
      List<AssignableReplica> allReplicas,
      PriorityQueue<Triple<AssignableReplica, AssignableNode, Float>> replicaPriorityQueue,
      Map<String, PriorityQueue<Pair<AssignableNode, Float>>> replicaImpactTrackingMap) {
    for (AssignableReplica replica : allReplicas) {
      Pair<AssignableNode, Float> criticalNodeImpact;
      if (replicaImpactTrackingMap.containsKey(replica.toString())) {
        // The impact has already been calculated for the other replica of the same partition with a
        // same state. Just return the calculated result.
        // WARN: the assumption here is that replicas of the same partition with same state are
        // always weighted the same. Otherwise, the tracking map will return inaccurate result.
        criticalNodeImpact = replicaImpactTrackingMap.get(replica.toString()).peek();
      } else {
        // Node priority queue is sorted so the smallest predicted used node is the first element.
        PriorityQueue<Pair<AssignableNode, Float>> nodePriorityQueue =
            new PriorityQueue<>(Comparator.comparing(Pair::getRight));
        // The impact of a replica is defined by the smallest "projected highest utilization" among
        // all the instances. This indicate the minimum possible impact after this replica is
        // assigned to the cluster.
        for (AssignableNode node : allNodes) {
          float utilization = node.getProjectedHighestUtilization(replica.getCapacity());
          nodePriorityQueue.add(new MutablePair<>(node, utilization));
        }
        replicaImpactTrackingMap.put(replica.toString(), nodePriorityQueue);
        criticalNodeImpact = nodePriorityQueue.peek();
      }
      replicaPriorityQueue.add(new MutableTriple<>(replica, criticalNodeImpact.getLeft(),
          criticalNodeImpact.getRight()));
    }
  }

  /**
   * Update the runtime replica impact tracking map. Also update the replica priority queue if
   * necessary.
   */
  private void UpdateReplicaPriorityQueue(
      PriorityQueue<Triple<AssignableReplica, AssignableNode, Float>> replicaPriorityQueue,
      Map<String, PriorityQueue<Pair<AssignableNode, Float>>> replicaImpactTrackingMap,
      AssignableNode updatedNode) {
    List<Triple<AssignableReplica, AssignableNode, Float>> adjustedReplicaQueueElements =
        new ArrayList<>();
    Iterator<Triple<AssignableReplica, AssignableNode, Float>> iter =
        replicaPriorityQueue.iterator();
    while (iter.hasNext()) {
      MutableTriple<AssignableReplica, AssignableNode, Float> curReplicaQueueElemTriple =
          (MutableTriple<AssignableReplica, AssignableNode, Float>) iter.next();
      if (!curReplicaQueueElemTriple.getMiddle().equals(updatedNode)) {
        // If the current critical impact node does not have new assignment, then it is still
        // valid since the new assignment only increase the utilization.
        continue;
      }
      // else, if the current critical impact node has a new assignment, then we need to update
      // the impact for this replica.
      AssignableReplica curReplica = curReplicaQueueElemTriple.getLeft();
      float newImpact = updatedNode.getProjectedHighestUtilization(curReplica.getCapacity());
      if (Float.compare(newImpact, curReplicaQueueElemTriple.getRight()) == 0) {
        // If the new assignment does not really increase the utilization, then there is no need
        // to update anything.
        continue;
      }
      // Otherwise, adjust the order of the current replica in the Queue
      // 1. Remove the top item from the queue.
      iter.remove();
      // 2. Update and find the new critical impact node.
      PriorityQueue<Pair<AssignableNode, Float>> nodeImpactRecords =
          replicaImpactTrackingMap.get(curReplica.toString());
      // Since we just need to find an item smaller than the newImpact and is still valid.
      // Otherwise, the newImpact corresponding node will be update and it can be the new critical
      // impact.
      // Note that we have delayed the update of tracking map elements. So some items might become
      // invalid since the real utilization has been already increased. It can be easily fixed by
      // recalculating the projected highest utilization.
      while (nodeImpactRecords.peek().getRight() <= newImpact) {
        Pair<AssignableNode, Float> topNodeImpact = nodeImpactRecords.peek();
        AssignableNode topNode = topNodeImpact.getLeft();
        float recalculatedImpact = topNode
            .getProjectedHighestUtilization(curReplica.getCapacity());
        if (recalculatedImpact != topNodeImpact.getRight()) {
          MutablePair adjustedNodeImpact = (MutablePair) nodeImpactRecords.poll();
          adjustedNodeImpact.setRight(recalculatedImpact);
          nodeImpactRecords.offer(adjustedNodeImpact);
        } else {
          // The current top record is still the critical impact. So no need to keep updating.
          // We will just use it to update the replica priority queue.
          break;
        }
      }
      // Update the replica impact triple with the newly calculated node impact information.
      Pair<AssignableNode, Float> topNodeImpact = nodeImpactRecords.peek();
      curReplicaQueueElemTriple.setMiddle(topNodeImpact.getLeft());
      curReplicaQueueElemTriple.setRight(topNodeImpact.getRight());
      adjustedReplicaQueueElements.add(curReplicaQueueElemTriple);
    }
    replicaPriorityQueue.addAll(adjustedReplicaQueueElements);
  }

  private class AssignmentReplicaComparator implements Comparator<Triple<AssignableReplica, AssignableNode, Float>> {
    private final Map<String, Integer> _replicaHashCodeMap;
    private final Map<String, ResourceAssignment> _bestPossibleAssignment;
    private final Map<String, ResourceAssignment> _baselineAssignment;

    AssignmentReplicaComparator(ClusterModel clusterModel, List<AssignableReplica> allReplicas) {
      _bestPossibleAssignment = clusterModel.getContext().getBestPossibleAssignment();
      _baselineAssignment = clusterModel.getContext().getBaselineAssignment();
      // Pre-process information of all the replicas to avoid repeated calculation for comparing.
      _replicaHashCodeMap = allReplicas.parallelStream().collect(Collectors
          .toMap(AssignableReplica::toString, replica -> Objects
                  .hash(replica.toString(), clusterModel.getAssignableNodes().keySet()),
              (hash1, hash2) -> hash2));
    }

    @Override
    public int compare(Triple<AssignableReplica, AssignableNode, Float> replica1Triple,
        Triple<AssignableReplica, AssignableNode, Float> replica2Triple) {
      AssignableReplica replica1 = replica1Triple.getLeft();
      AssignableReplica replica2 = replica2Triple.getLeft();

      String resourceName1 = replica1.getResourceName();
      String resourceName2 = replica2.getResourceName();

      // 1. Sort according if the assignment exists in the best possible and/or baseline assignment
      if (_bestPossibleAssignment.containsKey(resourceName1) != _bestPossibleAssignment
          .containsKey(resourceName2)) {
        // If the best possible assignment contains only one replica's assignment,
        // prioritize the replica.
        return _bestPossibleAssignment.containsKey(resourceName1) ? -1 : 1;
      }

      if (_baselineAssignment.containsKey(resourceName1) != _baselineAssignment
          .containsKey(resourceName2)) {
        // If the baseline assignment contains only one replica's assignment, prioritize the replica.
        return _baselineAssignment.containsKey(resourceName1) ? -1 : 1;
      }

      // 2. Sort according to the state priority. Or the greedy algorithm will unnecessarily shuffle
      // the states between replicas.
      int statePriority1 = replica1.getStatePriority();
      int statePriority2 = replica2.getStatePriority();
      if (statePriority1 != statePriority2) {
        // Note we shall prioritize the replica with a higher state priority,
        // the smaller priority number means higher priority.
        return statePriority1 - statePriority2;
      }

      // 3. Sort according to the replica impact based on the weight.
      // So the greedy algorithm will place the more impactful replicas first.
      int compareResult = Float.compare(replica2Triple.getRight(), replica1Triple.getRight());
      if (compareResult != 0) {
        return compareResult;
      }

      // 4. Sort according to the resource/partition name.
      // If none of the above conditions is making a difference, try to randomize the replicas
      // order.
      // Otherwise, the same replicas might always be moved in each rebalancing. This is because
      // their placement calculating will always happen at the critical moment while the cluster is
      // almost close to the expected utilization.
      //
      // Note that to ensure the algorithm is deterministic with the same inputs, do not use
      // Random functions here. Use hashcode based on the cluster topology information to get
      // a controlled randomized order is good enough.
      Integer replicaHash1 = _replicaHashCodeMap.get(replica1.toString());
      Integer replicaHash2 = _replicaHashCodeMap.get(replica2.toString());
      if (!replicaHash1.equals(replicaHash2)) {
        return replicaHash1.compareTo(replicaHash2);
      } else {
        // In case of hash collision, return order according to the name.
        return replica1.toString().compareTo(replica2.toString());
      }
    }
  }
}
