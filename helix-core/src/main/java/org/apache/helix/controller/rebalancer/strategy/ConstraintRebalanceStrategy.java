package org.apache.helix.controller.rebalancer.strategy;

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
import org.apache.helix.ZNRecord;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceHardConstraint;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceSoftConstraint;
import org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.rebalancer.constraint.PartitionWeightAwareEvennessConstraint;
import org.apache.helix.controller.rebalancer.strategy.crushMapping.CardDealingAdjustmentAlgorithm;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Constraints based rebalance strategy.
 * Assignment is calculated according to the specified constraints.
 */
public class ConstraintRebalanceStrategy extends AbstractEvenDistributionRebalanceStrategy {
  private static final Logger _logger = LoggerFactory.getLogger(ConstraintRebalanceStrategy.class);
  // For the instances that are restricted by soft constraints, the minimum weight for assigning partitions.
  private static final int MIN_INSTANCE_WEIGHT = 1;

  // CRUSH ensures deterministic and evenness
  private final RebalanceStrategy _baseStrategy = new CrushRebalanceStrategy();

  protected RebalanceStrategy getBaseRebalanceStrategy() {
    return _baseStrategy;
  }

  private final List<AbstractRebalanceHardConstraint> _hardConstraints = new ArrayList<>();
  private final List<AbstractRebalanceSoftConstraint> _softConstraints = new ArrayList<>();

  private List<String> _partitions;
  private int _maxPerNode;

  // resource replica state requirement
  private LinkedHashMap<String, Integer> _states;
  // extend state requirement to a ordered list
  private List<String> _orderedStateList;

  public ConstraintRebalanceStrategy(
      List<? extends AbstractRebalanceHardConstraint> hardConstraints,
      List<? extends AbstractRebalanceSoftConstraint> softConstraints) {
    if (hardConstraints != null) {
      _hardConstraints.addAll(hardConstraints);
    }
    if (softConstraints != null) {
      _softConstraints.addAll(softConstraints);
    }
    if (_hardConstraints.isEmpty() && _softConstraints.isEmpty()) {
      throw new HelixException(
          "Failed to construct ConstraintRebalanceStrategy since no constraint is provided.");
    }
  }

  /**
   * This strategy is currently for rebalance tool only.
   * For the constructor defined for AutoRebalancer, use a simplified default constraint to ensure balance.
   *
   * Note this strategy will flip-flop almost for sure if directly used in the existing rebalancer.
   * TODO Enable different constraints for automatic rebalance process in the controller later.
   */
  public ConstraintRebalanceStrategy() {
    _logger.debug("Init constraint rebalance strategy using the default even constraint.");
    PartitionWeightAwareEvennessConstraint defaultConstraint =
        new PartitionWeightAwareEvennessConstraint(new PartitionWeightProvider() {
          @Override
          public int getPartitionWeight(String resource, String partition) {
            return 1;
          }
        }, new CapacityProvider() {
          @Override
          public int getParticipantCapacity(String participant) {
            return MIN_INSTANCE_WEIGHT;
          }

          @Override
          public int getParticipantUsage(String participant) {
            return 0;
          }
        });
    _softConstraints.add(defaultConstraint);
  }

  protected CardDealingAdjustmentAlgorithm getCardDealingAlgorithm(Topology topology) {
    // For constraint based strategy, need more fine-grained assignment for each partition.
    // So evenness is more important.
    return new CardDealingAdjustmentAlgorithm(topology, _replica,
        CardDealingAdjustmentAlgorithm.Mode.EVENNESS);
  }

  @Override
  public void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode) {
    _resourceName = resourceName;
    _partitions = new ArrayList<>(partitions);
    _maxPerNode = maximumPerNode;
    _states = states;
    _orderedStateList = new ArrayList<>();
    for (String state : states.keySet()) {
      for (int i = 0; i < states.get(state); i++) {
        _orderedStateList.add(state);
      }
    }
  }

  /**
   * Generate assignment based on the constraints.
   *
   * @param allNodes         All instances
   * @param liveNodes        List of live instances
   * @param currentMapping   current replica mapping. Will directly use this mapping if it meets state model requirement
   * @param clusterData      cluster data
   * @return IdeaState node that contains both preference list and a proposed state mapping.
   * @throws HelixException
   */
  @Override
  public ZNRecord computePartitionAssignment(final List<String> allNodes,
      final List<String> liveNodes, final Map<String, Map<String, String>> currentMapping,
      ClusterDataCache clusterData) throws HelixException {
    // Since instance weight will be replaced by constraint evaluation, record it in advance to avoid
    // overwriting.
    Map<String, Integer> instanceWeightRecords = new HashMap<>();
    for (InstanceConfig instanceConfig : clusterData.getInstanceConfigMap().values()) {
      if (instanceConfig.getWeight() != InstanceConfig.WEIGHT_NOT_SET) {
        instanceWeightRecords.put(instanceConfig.getInstanceName(), instanceConfig.getWeight());
      }
    }

    List<String> candidates = new ArrayList<>(allNodes);
    // Only calculate for configured nodes.
    // Remove all non-configured nodes.
    candidates.retainAll(clusterData.getInstanceConfigMap().keySet());

    // For generating the IdealState ZNRecord
    Map<String, List<String>> preferenceList = new HashMap<>();
    Map<String, Map<String, String>> idealStateMap = new HashMap<>();

    for (String partition : _partitions) {
      if (currentMapping.containsKey(partition)) {
        Map<String, String> partitionMapping = currentMapping.get(partition);
        // check for the preferred assignment
        partitionMapping = validateStateMap(partitionMapping);
        if (partitionMapping != null) {
          _logger.debug(
              "The provided preferred partition assignment meets state model requirements. Skip rebalance.");
          preferenceList.put(partition, new ArrayList<>(partitionMapping.keySet()));
          idealStateMap.put(partition, partitionMapping);
          updateConstraints(partition, partitionMapping);
          continue;
        }
      } // else, recalculate the assignment
      List<String> assignment =
          computeSinglePartitionAssignment(partition, candidates, liveNodes, clusterData);

      // Shuffle the list.
      // Note that single partition assignment won't have enough inputs to ensure state evenness.
      // So need to shuffle again, here
      Collections.shuffle(assignment,
          new Random(String.format("%s.%s", _resourceName, partition).hashCode()));

      // Calculate for replica states
      Map<String, String> stateMap = new HashMap<>();
      for (int i = 0; i < assignment.size(); i++) {
        stateMap.put(assignment.get(i), _orderedStateList.get(i));
      }

      // Update idea states & preference list
      idealStateMap.put(partition, stateMap);
      preferenceList.put(partition, assignment);
      // Note, only update with the new pending assignment
      updateConstraints(partition, stateMap);
    }

    // recover the original weight
    for (String instanceName : instanceWeightRecords.keySet()) {
      clusterData.getInstanceConfigMap().get(instanceName)
          .setWeight(instanceWeightRecords.get(instanceName));
    }

    ZNRecord result = new ZNRecord(_resourceName);
    result.setListFields(preferenceList);
    result.setMapFields(idealStateMap);
    return result;
  }

  /**
   * @param actualMapping
   * @return a filtered state mapping that fit state model definition.
   *          Or null if the input mapping is conflict with state model.
   */
  private Map<String, String> validateStateMap(Map<String, String> actualMapping) {
    Map<String, String> filteredStateMapping = new HashMap<>();

    Map<String, Integer> tmpStates = new HashMap<>(_states);
    for (String partition : actualMapping.keySet()) {
      String state = actualMapping.get(partition);
      if (tmpStates.containsKey(state)) {
        int count = tmpStates.get(state);
        if (count > 0) {
          filteredStateMapping.put(partition, state);
          tmpStates.put(state, count - 1);
        }
      }
    }

    for (String state : tmpStates.keySet()) {
      if (tmpStates.get(state) > 0) {
        return null;
      }
    }
    return filteredStateMapping;
  }

  /**
   * Calculate for a fine-grained assignment for all replicas of a single partition.
   *
   * @param partitionName
   * @param allNodes
   * @param liveNodes
   * @param clusterData
   * @return
   */
  private List<String> computeSinglePartitionAssignment(String partitionName,
      final List<String> allNodes, final List<String> liveNodes, ClusterDataCache clusterData) {
    List<String> qualifiedNodes = new ArrayList<>(allNodes);

    // do hard constraints check and find all qualified instances
    for (AbstractRebalanceHardConstraint hardConstraint : _hardConstraints) {
      Map<String, String[]> proposedAssignment = Collections
          .singletonMap(partitionName, qualifiedNodes.toArray(new String[qualifiedNodes.size()]));
      boolean[] validateResults =
          hardConstraint.isValid(_resourceName, proposedAssignment).get(partitionName);
      for (int i = 0; i < validateResults.length; i++) {
        if (!validateResults[i]) {
          qualifiedNodes.remove(proposedAssignment.get(partitionName)[i]);
        }
      }
    }

    int[] instancePriority = new int[qualifiedNodes.size()];
    Map<String, String[]> proposedAssignment = Collections
        .singletonMap(partitionName, qualifiedNodes.toArray(new String[qualifiedNodes.size()]));
    for (AbstractRebalanceSoftConstraint softConstraint : _softConstraints) {
      if (softConstraint.getConstraintWeight() == 0) {
        continue;
      }

      int[] evaluateResults =
          softConstraint.evaluate(_resourceName, proposedAssignment).get(partitionName);
      for (int i = 0; i < evaluateResults.length; i++) {
        // accumulate all evaluate results
        instancePriority[i] += evaluateResults[i] * softConstraint.getConstraintWeight();
      }
    }

    // Since the evaluated result can be a negative number, get the min result as the baseline for normalizing all priorities to set weight.
    int baseline = Integer.MAX_VALUE;
    for (int priority : instancePriority) {
      if (baseline > priority) {
        baseline = priority;
      }
    }
    // Limit the weight to be at least MIN_INSTANCE_WEIGHT
    for (int i = 0; i < instancePriority.length; i++) {
      clusterData.getInstanceConfigMap().get(qualifiedNodes.get(i))
          .setWeight(instancePriority[i] - baseline + MIN_INSTANCE_WEIGHT);
    }

    // Trigger rebalance only for a single partition.
    // Note that if we do it for the whole resource,
    // the result won't be accurate since the pending assignment won't be updated to constraints.
    super.init(_resourceName, Collections.singletonList(partitionName), _states, _maxPerNode);
    ZNRecord partitionAssignment = super
        .computePartitionAssignment(qualifiedNodes, liveNodes, Collections.EMPTY_MAP, clusterData);

    return partitionAssignment.getListFields().get(partitionName);
  }

  private void updateConstraints(String partition, Map<String, String> pendingAssignment) {
    if (pendingAssignment.isEmpty()) {
      _logger.warn("No pending assignment needs to update. Skip constraint update.");
      return;
    }

    ResourcesStateMap tempStateMap = new ResourcesStateMap();
    tempStateMap.setState(_resourceName, new Partition(partition), pendingAssignment);
    _logger.debug("Update constraints with pending assignment: " + tempStateMap.toString());

    for (AbstractRebalanceHardConstraint hardConstraint : _hardConstraints) {
      hardConstraint.updateAssignment(tempStateMap);
    }
    for (AbstractRebalanceSoftConstraint softConstraint : _softConstraints) {
      softConstraint.updateAssignment(tempStateMap);
    }
  }
}
