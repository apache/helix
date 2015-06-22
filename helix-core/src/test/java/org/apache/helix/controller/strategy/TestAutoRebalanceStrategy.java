package org.apache.helix.controller.strategy;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.ReplicaPlacementScheme;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TestAutoRebalanceStrategy {
  private static Logger logger = Logger.getLogger(TestAutoRebalanceStrategy.class);

  /**
   * Sanity test for a basic Master-Slave model
   */
  @Test
  public void simpleMasterSlaveTest() {
    final int NUM_ITERATIONS = 10;
    final int NUM_PARTITIONS = 10;
    final int NUM_LIVE_NODES = 12;
    final int NUM_TOTAL_NODES = 20;
    final int MAX_PER_NODE = 5;

    final String[] STATE_NAMES = {
        "MASTER", "SLAVE"
    };
    final int[] STATE_COUNTS = {
        1, 2
    };

    runTest("BasicMasterSlave", NUM_ITERATIONS, NUM_PARTITIONS, NUM_LIVE_NODES, NUM_TOTAL_NODES,
        MAX_PER_NODE, STATE_NAMES, STATE_COUNTS);
  }

  /**
   * Run a test for an arbitrary state model.
   * @param name Name of the test state model
   * @param numIterations Number of rebalance tasks to run
   * @param numPartitions Number of partitions for the resource
   * @param numLiveNodes Number of live nodes in the cluster
   * @param numTotalNodes Number of nodes in the cluster, must be greater than or equal to
   *          numLiveNodes
   * @param maxPerNode Maximum number of replicas a node can serve
   * @param stateNames States ordered by preference
   * @param stateCounts Number of replicas that should be in each state
   */
  private void runTest(String name, int numIterations, int numPartitions, int numLiveNodes,
      int numTotalNodes, int maxPerNode, String[] stateNames, int[] stateCounts) {
    List<String> partitions = new ArrayList<String>();
    for (int i = 0; i < numPartitions; i++) {
      partitions.add("p_" + i);
    }

    List<String> liveNodes = new ArrayList<String>();
    List<String> allNodes = new ArrayList<String>();
    for (int i = 0; i < numTotalNodes; i++) {
      allNodes.add("n_" + i);
      if (i < numLiveNodes) {
        liveNodes.add("n_" + i);
      }
    }

    Map<String, Map<String, String>> currentMapping = new TreeMap<String, Map<String, String>>();

    LinkedHashMap<String, Integer> states = new LinkedHashMap<String, Integer>();
    for (int i = 0; i < Math.min(stateNames.length, stateCounts.length); i++) {
      states.put(stateNames[i], stateCounts[i]);
    }

    StateModelDefinition stateModelDef = getIncompleteStateModelDef(name, stateNames[0], states);

    new AutoRebalanceTester(partitions, states, liveNodes, currentMapping, allNodes, maxPerNode,
        stateModelDef, new AutoRebalanceStrategy.DefaultPlacementScheme())
        .runRepeatedly(numIterations);
  }

  /**
   * Get a StateModelDefinition without transitions. The auto rebalancer doesn't take transitions
   * into account when computing mappings, so this is acceptable.
   * @param modelName name to give the model
   * @param initialState initial state for all nodes
   * @param states ordered map of state to count
   * @return incomplete StateModelDefinition for rebalancing
   */
  private StateModelDefinition getIncompleteStateModelDef(String modelName, String initialState,
      LinkedHashMap<String, Integer> states) {
    StateModelDefinition.Builder builder =
        new StateModelDefinition.Builder(StateModelDefId.from(modelName));
    builder.initialState(State.from(initialState));
    int i = 0;
    for (String state : states.keySet()) {
      builder.addState(State.from(state), i);
      builder.upperBound(State.from(state), states.get(state));
      i++;
    }
    return builder.build();
  }

  class AutoRebalanceTester {
    private static final double P_KILL = 0.45;
    private static final double P_ADD = 0.1;
    private static final double P_RESURRECT = 0.45;
    private static final String RESOURCE_NAME = "resource";

    private List<String> _partitions;
    private LinkedHashMap<String, Integer> _states;
    private List<String> _liveNodes;
    private Set<String> _liveSet;
    private Set<String> _removedSet;
    private Set<String> _nonLiveSet;
    private Map<String, Map<String, String>> _currentMapping;
    private List<String> _allNodes;
    private int _maxPerNode;
    private StateModelDefinition _stateModelDef;
    private ReplicaPlacementScheme _placementScheme;
    private Random _random;

    public AutoRebalanceTester(List<String> partitions, LinkedHashMap<String, Integer> states,
        List<String> liveNodes, Map<String, Map<String, String>> currentMapping,
        List<String> allNodes, int maxPerNode, StateModelDefinition stateModelDef,
        ReplicaPlacementScheme placementScheme) {
      _partitions = partitions;
      _states = states;
      _liveNodes = liveNodes;
      _liveSet = new TreeSet<String>();
      for (String node : _liveNodes) {
        _liveSet.add(node);
      }
      _removedSet = new TreeSet<String>();
      _nonLiveSet = new TreeSet<String>();
      _currentMapping = currentMapping;
      _allNodes = allNodes;
      for (String node : allNodes) {
        if (!_liveSet.contains(node)) {
          _nonLiveSet.add(node);
        }
      }
      _maxPerNode = maxPerNode;
      _stateModelDef = stateModelDef;
      _placementScheme = placementScheme;
      _random = new Random();
    }

    /**
     * Repeatedly randomly select a task to run and report the result
     * @param numIterations
     *          Number of random tasks to run in sequence
     */
    public void runRepeatedly(int numIterations) {
      logger.info("~~~~ Initial State ~~~~~");
      ZNRecord initialResult =
          new AutoRebalanceStrategy(RESOURCE_NAME, _partitions, _states, _maxPerNode,
              _placementScheme).computePartitionAssignment(_liveNodes, _currentMapping, _allNodes);
      _currentMapping = getMapping(initialResult.getListFields());
      logger.info(_currentMapping);
      getRunResult(_currentMapping, initialResult.getListFields());
      for (int i = 0; i < numIterations; i++) {
        logger.info("~~~~ Iteration " + i + " ~~~~~");
        ZNRecord znRecord = runOnceRandomly();
        if (znRecord != null) {
          final Map<String, List<String>> listResult = znRecord.getListFields();
          final Map<String, Map<String, String>> mapResult = getMapping(listResult);
          logger.info(mapResult);
          logger.info(listResult);
          getRunResult(mapResult, listResult);
          _currentMapping = mapResult;
        }
      }
    }

    private Map<String, Map<String, String>> getMapping(final Map<String, List<String>> listResult) {
      final Map<String, Map<String, String>> mapResult = new HashMap<String, Map<String, String>>();
      for (String partition : _partitions) {
        Map<String, String> rawCurStateMap = _currentMapping.get(partition);
        ClusterConfig cluster =
            new ClusterConfig.Builder(ClusterId.from("cluster")).addStateModelDefinition(
                _stateModelDef).build();
        Set<ParticipantId> liveParticipantSet = Sets.newHashSet();
        for (String node : _liveNodes) {
          liveParticipantSet.add(ParticipantId.from(node));
        }
        List<ParticipantId> preferenceList =
            IdealState.preferenceListFromStringList(listResult.get(partition));
        Set<ParticipantId> disabledParticipantsForPartition = Collections.emptySet();
        Map<ParticipantId, State> currentStateMap =
            IdealState.participantStateMapFromStringMap(rawCurStateMap);
        Map<State, String> upperBounds =
            ConstraintBasedAssignment.stateConstraints(_stateModelDef,
                ResourceId.from(RESOURCE_NAME), cluster);
        Map<ParticipantId, State> assignment =
            ConstraintBasedAssignment.computeAutoBestStateForPartition(upperBounds,
                liveParticipantSet, _stateModelDef, preferenceList, currentStateMap,
                disabledParticipantsForPartition, true);
        mapResult.put(partition, IdealState.stringMapFromParticipantStateMap(assignment));
      }
      return mapResult;
    }

    /**
     * Output various statistics and correctness check results
     * @param mapFields
     *          The map-map assignment generated by the rebalancer
     * @param listFields
     *          The map-list assignment generated by the rebalancer
     */
    public void getRunResult(final Map<String, Map<String, String>> mapFields,
        final Map<String, List<String>> listFields) {
      logger.info("***** Statistics *****");
      dumpStatistics(mapFields);
      verifyCorrectness(mapFields, listFields);
    }

    /**
     * Output statistics about the assignment
     * @param mapFields
     *          The map-map assignment generated by the rebalancer
     */
    public void dumpStatistics(final Map<String, Map<String, String>> mapFields) {
      Map<String, Integer> partitionsPerNode = getPartitionBucketsForNode(mapFields);
      int nodeCount = _liveNodes.size();
      logger.info("Total number of nodes: " + nodeCount);
      logger.info("Nodes: " + _liveNodes);
      int sumPartitions = getSum(partitionsPerNode.values());
      logger.info("Total number of partitions: " + sumPartitions);
      double averagePartitions = getAverage(partitionsPerNode.values());
      logger.info("Average number of partitions per node: " + averagePartitions);
      double stdevPartitions = getStdev(partitionsPerNode.values(), averagePartitions);
      logger.info("Standard deviation of partitions: " + stdevPartitions);

      // Statistics about each state
      Map<String, Map<String, Integer>> statesPerNode = getStateBucketsForNode(mapFields);
      for (String state : _states.keySet()) {
        Map<String, Integer> nodeStateCounts = new TreeMap<String, Integer>();
        for (Entry<String, Map<String, Integer>> nodeStates : statesPerNode.entrySet()) {
          Map<String, Integer> stateCounts = nodeStates.getValue();
          if (stateCounts.containsKey(state)) {
            nodeStateCounts.put(nodeStates.getKey(), stateCounts.get(state));
          } else {
            nodeStateCounts.put(nodeStates.getKey(), 0);
          }
        }
        int sumStates = getSum(nodeStateCounts.values());
        logger.info("Total number of state " + state + ": " + sumStates);
        double averageStates = getAverage(nodeStateCounts.values());
        logger.info("Average number of state " + state + " per node: " + averageStates);
        double stdevStates = getStdev(nodeStateCounts.values(), averageStates);
        logger.info("Standard deviation of state " + state + " per node: " + stdevStates);
      }
    }

    /**
     * Run a set of correctness tests, reporting success or failure
     * @param mapFields
     *          The map-map assignment generated by the rebalancer
     * @param listFields
     *          The map-list assignment generated by the rebalancer
     */
    public void verifyCorrectness(final Map<String, Map<String, String>> mapFields,
        final Map<String, List<String>> listFields) {
      final Map<String, Integer> partitionsPerNode = getPartitionBucketsForNode(mapFields);
      boolean maxConstraintMet = maxNotExceeded(partitionsPerNode);
      assert maxConstraintMet : "Max per node constraint: FAIL";
      logger.info("Max per node constraint: PASS");

      boolean liveConstraintMet = onlyLiveAssigned(partitionsPerNode);
      assert liveConstraintMet : "Only live nodes have partitions constraint: FAIL";
      logger.info("Only live nodes have partitions constraint: PASS");

      boolean stateAssignmentPossible = correctStateAssignmentCount(mapFields);
      assert stateAssignmentPossible : "State replica constraint: FAIL";
      logger.info("State replica constraint: PASS");

      boolean nodesUniqueForPartitions = atMostOnePartitionReplicaPerNode(listFields);
      assert nodesUniqueForPartitions : "Node uniqueness per partition constraint: FAIL";
      logger.info("Node uniqueness per partition constraint: PASS");
    }

    private boolean maxNotExceeded(final Map<String, Integer> partitionsPerNode) {
      for (String node : partitionsPerNode.keySet()) {
        Integer value = partitionsPerNode.get(node);
        if (value > _maxPerNode) {
          logger.error("ERROR: Node " + node + " has " + value
              + " partitions despite a maximum of " + _maxPerNode);
          return false;
        }
      }
      return true;
    }

    private boolean onlyLiveAssigned(final Map<String, Integer> partitionsPerNode) {
      for (final Entry<String, Integer> nodeState : partitionsPerNode.entrySet()) {
        boolean isLive = _liveSet.contains(nodeState.getKey());
        boolean isEmpty = nodeState.getValue() == 0;
        if (!isLive && !isEmpty) {
          logger.error("ERROR: Node " + nodeState.getKey() + " is not live, but has "
              + nodeState.getValue() + " replicas!");
          return false;
        }
      }
      return true;
    }

    private boolean correctStateAssignmentCount(final Map<String, Map<String, String>> assignment) {
      for (final Entry<String, Map<String, String>> partitionEntry : assignment.entrySet()) {
        final Map<String, String> nodeMap = partitionEntry.getValue();
        final Map<String, Integer> stateCounts = new TreeMap<String, Integer>();
        for (String state : nodeMap.values()) {
          if (!stateCounts.containsKey(state)) {
            stateCounts.put(state, 1);
          } else {
            stateCounts.put(state, stateCounts.get(state) + 1);
          }
        }
        for (String state : stateCounts.keySet()) {
          if (state.equals(HelixDefinedState.DROPPED.toString())) {
            continue;
          }
          int count = stateCounts.get(state);
          int maximumCount = _states.get(state);
          if (count > maximumCount) {
            logger.error("ERROR: State " + state + " for partition " + partitionEntry.getKey()
                + " has " + count + " replicas when " + maximumCount + " is allowed!");
            return false;
          }
        }
      }
      return true;
    }

    private boolean atMostOnePartitionReplicaPerNode(final Map<String, List<String>> listFields) {
      for (final Entry<String, List<String>> partitionEntry : listFields.entrySet()) {
        Set<String> nodeSet = new HashSet<String>(partitionEntry.getValue());
        int numUniques = nodeSet.size();
        int total = partitionEntry.getValue().size();
        if (numUniques < total) {
          logger.error("ERROR: Partition " + partitionEntry.getKey() + " is assigned to " + total
              + " nodes, but only " + numUniques + " are unique!");
          return false;
        }
      }
      return true;
    }

    private double getAverage(final Collection<Integer> values) {
      double sum = 0.0;
      for (Integer value : values) {
        sum += value;
      }
      if (values.size() != 0) {
        return sum / values.size();
      } else {
        return -1.0;
      }
    }

    private int getSum(final Collection<Integer> values) {
      int sum = 0;
      for (Integer value : values) {
        sum += value;
      }
      return sum;
    }

    private double getStdev(final Collection<Integer> values, double mean) {
      double sum = 0.0;
      for (Integer value : values) {
        double deviation = mean - value;
        sum += Math.pow(deviation, 2.0);
      }
      if (values.size() != 0) {
        sum /= values.size();
        return Math.pow(sum, 0.5);
      } else {
        return -1.0;
      }
    }

    private Map<String, Integer> getPartitionBucketsForNode(
        final Map<String, Map<String, String>> assignment) {
      Map<String, Integer> partitionsPerNode = new TreeMap<String, Integer>();
      for (String node : _liveNodes) {
        partitionsPerNode.put(node, 0);
      }
      for (Entry<String, Map<String, String>> partitionEntry : assignment.entrySet()) {
        final Map<String, String> nodeMap = partitionEntry.getValue();
        for (String node : nodeMap.keySet()) {
          String state = nodeMap.get(node);
          if (state.equals(HelixDefinedState.DROPPED.toString())) {
            continue;
          }
          // add 1 for every occurrence of a node
          if (!partitionsPerNode.containsKey(node)) {
            partitionsPerNode.put(node, 1);
          } else {
            partitionsPerNode.put(node, partitionsPerNode.get(node) + 1);
          }
        }
      }
      return partitionsPerNode;
    }

    private Map<String, Map<String, Integer>> getStateBucketsForNode(
        final Map<String, Map<String, String>> assignment) {
      Map<String, Map<String, Integer>> result = new TreeMap<String, Map<String, Integer>>();
      for (String n : _liveNodes) {
        result.put(n, new TreeMap<String, Integer>());
      }
      for (Map<String, String> nodeStateMap : assignment.values()) {
        for (Entry<String, String> nodeState : nodeStateMap.entrySet()) {
          if (!result.containsKey(nodeState.getKey())) {
            result.put(nodeState.getKey(), new TreeMap<String, Integer>());
          }
          Map<String, Integer> stateMap = result.get(nodeState.getKey());
          if (!stateMap.containsKey(nodeState.getValue())) {
            stateMap.put(nodeState.getValue(), 1);
          } else {
            stateMap.put(nodeState.getValue(), stateMap.get(nodeState.getValue()) + 1);
          }
        }
      }
      return result;
    }

    /**
     * Randomly choose between killing, adding, or resurrecting a single node
     * @return (Partition -> (Node -> State)) ZNRecord
     */
    public ZNRecord runOnceRandomly() {
      double choose = _random.nextDouble();
      ZNRecord result = null;
      if (choose < P_KILL) {
        result = removeSingleNode(null);
      } else if (choose < P_KILL + P_ADD) {
        result = addSingleNode(null);
      } else if (choose < P_KILL + P_ADD + P_RESURRECT) {
        result = resurrectSingleNode(null);
      }
      return result;
    }

    /**
     * Run rebalancer trying to add a never-live node
     * @param node
     *          Optional String to add
     * @return ZNRecord result returned by the rebalancer
     */
    public ZNRecord addSingleNode(String node) {
      logger.info("=================== add node =================");
      if (_nonLiveSet.size() == 0) {
        logger.warn("Cannot add node because there are no nodes left to add.");
        return null;
      }

      // Get a random never-live node
      if (node == null || !_nonLiveSet.contains(node)) {
        node = getRandomSetElement(_nonLiveSet);
      }
      logger.info("Adding " + node);
      _liveNodes.add(node);
      _liveSet.add(node);
      _nonLiveSet.remove(node);

      return new AutoRebalanceStrategy(RESOURCE_NAME, _partitions, _states, _maxPerNode,
          _placementScheme).computePartitionAssignment(_liveNodes, _currentMapping, _allNodes);
    }

    /**
     * Run rebalancer trying to remove a live node
     * @param node
     *          Optional String to remove
     * @return ZNRecord result returned by the rebalancer
     */
    public ZNRecord removeSingleNode(String node) {
      logger.info("=================== remove node =================");
      if (_liveSet.size() == 0) {
        logger.warn("Cannot remove node because there are no nodes left to remove.");
        return null;
      }

      // Get a random never-live node
      if (node == null || !_liveSet.contains(node)) {
        node = getRandomSetElement(_liveSet);
      }
      logger.info("Removing " + node);
      _removedSet.add(node);
      _liveNodes.remove(node);
      _liveSet.remove(node);

      // the rebalancer expects that the current mapping doesn't contain deleted
      // nodes
      for (Map<String, String> nodeMap : _currentMapping.values()) {
        if (nodeMap.containsKey(node)) {
          nodeMap.remove(node);
        }
      }

      return new AutoRebalanceStrategy(RESOURCE_NAME, _partitions, _states, _maxPerNode,
          _placementScheme).computePartitionAssignment(_liveNodes, _currentMapping, _allNodes);
    }

    /**
     * Run rebalancer trying to add back a removed node
     * @param node
     *          Optional String to resurrect
     * @return ZNRecord result returned by the rebalancer
     */
    public ZNRecord resurrectSingleNode(String node) {
      logger.info("=================== resurrect node =================");
      if (_removedSet.size() == 0) {
        logger.warn("Cannot remove node because there are no nodes left to resurrect.");
        return null;
      }

      // Get a random never-live node
      if (node == null || !_removedSet.contains(node)) {
        node = getRandomSetElement(_removedSet);
      }
      logger.info("Resurrecting " + node);
      _removedSet.remove(node);
      _liveNodes.add(node);
      _liveSet.add(node);

      return new AutoRebalanceStrategy(RESOURCE_NAME, _partitions, _states, _maxPerNode,
          _placementScheme).computePartitionAssignment(_liveNodes, _currentMapping, _allNodes);
    }

    private <T> T getRandomSetElement(Set<T> source) {
      int element = _random.nextInt(source.size());
      int i = 0;
      for (T node : source) {
        if (i == element) {
          return node;
        }
        i++;
      }
      return null;
    }
  }

  /**
   * Tests the following scenario: nodes come up one by one, then one node is taken down. Preference
   * lists should prefer nodes in the current mapping at all times, but when all nodes are in the
   * current mapping, then it should distribute states as evenly as possible.
   */
  @Test
  public void testOrphansNotPreferred() {
    final ResourceId RESOURCE = ResourceId.from("resource");
    final PartitionId[] PARTITIONS =
        {
            PartitionId.from("resource_0"), PartitionId.from("resource_1"),
            PartitionId.from("resource_2")
        };
    final StateModelDefinition STATE_MODEL =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    final int REPLICA_COUNT = 2;
    final ParticipantId[] NODES = {
        ParticipantId.from("n0"), ParticipantId.from("n1"), ParticipantId.from("n2")
    };

    ReplicaPlacementScheme scheme = new AutoRebalanceStrategy.DefaultPlacementScheme();
    // initial state, one node, no mapping
    List<ParticipantId> allNodes = Lists.newArrayList(NODES[0]);
    List<ParticipantId> liveNodes = Lists.newArrayList(NODES[0]);
    Map<PartitionId, Map<ParticipantId, State>> currentMapping = Maps.newHashMap();
    for (PartitionId partition : PARTITIONS) {
      currentMapping.put(partition, new HashMap<ParticipantId, State>());
    }

    // make sure that when the first node joins, a single replica is assigned fairly
    List<PartitionId> partitions = ImmutableList.copyOf(PARTITIONS);
    Map<State, String> upperBounds = Maps.newHashMap();
    for (State state : STATE_MODEL.getTypedStatesPriorityList()) {
      upperBounds.put(state, STATE_MODEL.getNumParticipantsPerState(state));
    }
    LinkedHashMap<State, Integer> stateCount =
        ConstraintBasedAssignment.stateCount(upperBounds, STATE_MODEL, liveNodes.size(),
            REPLICA_COUNT);
    ZNRecord znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);
    Map<String, List<String>> preferenceLists = znRecord.getListFields();
    for (PartitionId partition : currentMapping.keySet()) {
      // make sure these are all MASTER
      List<String> preferenceList = preferenceLists.get(partition.toString());
      Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.size(), 1, "invalid preference list for " + partition);
    }

    // now assign a replica to the first node in the current mapping, and add a second node
    allNodes.add(NODES[1]);
    liveNodes.add(NODES[1]);
    stateCount =
        ConstraintBasedAssignment.stateCount(upperBounds, STATE_MODEL, liveNodes.size(),
            REPLICA_COUNT);
    for (PartitionId partition : PARTITIONS) {
      currentMapping.get(partition).put(NODES[0], State.from("MASTER"));
    }
    znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);
    preferenceLists = znRecord.getListFields();
    for (PartitionId partition : currentMapping.keySet()) {
      List<String> preferenceList = preferenceLists.get(partition.toString());
      Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.size(), 2, "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.get(0), NODES[0].toString(),
          "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.get(1), NODES[1].toString(),
          "invalid preference list for " + partition);
    }

    // now set the current mapping to reflect this update and make sure that it distributes masters
    for (PartitionId partition : PARTITIONS) {
      currentMapping.get(partition).put(NODES[1], State.from("SLAVE"));
    }
    znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);
    preferenceLists = znRecord.getListFields();
    Set<String> firstNodes = Sets.newHashSet();
    for (PartitionId partition : currentMapping.keySet()) {
      List<String> preferenceList = preferenceLists.get(partition.toString());
      Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.size(), 2, "invalid preference list for " + partition);
      firstNodes.add(preferenceList.get(0));
    }
    Assert.assertEquals(firstNodes.size(), 2, "masters not evenly distributed");

    // set a mapping corresponding to a valid mapping for 2 nodes, add a third node, check that the
    // new node is never the most preferred
    allNodes.add(NODES[2]);
    liveNodes.add(NODES[2]);
    stateCount =
        ConstraintBasedAssignment.stateCount(upperBounds, STATE_MODEL, liveNodes.size(),
            REPLICA_COUNT);

    // recall that the other two partitions are [MASTER, SLAVE], which is fine, just reorder one
    currentMapping.get(PARTITIONS[1]).put(NODES[0], State.from("SLAVE"));
    currentMapping.get(PARTITIONS[1]).put(NODES[1], State.from("MASTER"));
    znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);
    preferenceLists = znRecord.getListFields();
    boolean newNodeUsed = false;
    for (PartitionId partition : currentMapping.keySet()) {
      List<String> preferenceList = preferenceLists.get(partition.toString());
      Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.size(), 2, "invalid preference list for " + partition);
      if (preferenceList.contains(NODES[2].toString())) {
        newNodeUsed = true;
        Assert.assertEquals(preferenceList.get(1), NODES[2].toString(),
            "newly added node not at preference list tail for " + partition);
      }
    }
    Assert.assertTrue(newNodeUsed, "not using " + NODES[2]);

    // now remap this to take the new node into account, should go back to balancing masters, slaves
    // evenly across all nodes
    for (PartitionId partition : PARTITIONS) {
      currentMapping.get(partition).clear();
    }
    currentMapping.get(PARTITIONS[0]).put(NODES[0], State.from("MASTER"));
    currentMapping.get(PARTITIONS[0]).put(NODES[1], State.from("SLAVE"));
    currentMapping.get(PARTITIONS[1]).put(NODES[1], State.from("MASTER"));
    currentMapping.get(PARTITIONS[1]).put(NODES[2], State.from("SLAVE"));
    currentMapping.get(PARTITIONS[2]).put(NODES[0], State.from("MASTER"));
    currentMapping.get(PARTITIONS[2]).put(NODES[2], State.from("SLAVE"));
    znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);
    preferenceLists = znRecord.getListFields();
    firstNodes.clear();
    Set<String> secondNodes = Sets.newHashSet();
    for (PartitionId partition : currentMapping.keySet()) {
      List<String> preferenceList = preferenceLists.get(partition.toString());
      Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.size(), 2, "invalid preference list for " + partition);
      firstNodes.add(preferenceList.get(0));
      secondNodes.add(preferenceList.get(1));
    }
    Assert.assertEquals(firstNodes.size(), 3, "masters not distributed evenly");
    Assert.assertEquals(secondNodes.size(), 3, "slaves not distributed evenly");

    // remove a node now, but use the current mapping with everything balanced just prior
    liveNodes.remove(0);
    stateCount =
        ConstraintBasedAssignment.stateCount(upperBounds, STATE_MODEL, liveNodes.size(),
            REPLICA_COUNT);

    // remove all references of n0 from the mapping, keep everything else in a legal state
    for (PartitionId partition : PARTITIONS) {
      currentMapping.get(partition).clear();
    }
    currentMapping.get(PARTITIONS[0]).put(NODES[1], State.from("MASTER"));
    currentMapping.get(PARTITIONS[1]).put(NODES[1], State.from("MASTER"));
    currentMapping.get(PARTITIONS[1]).put(NODES[2], State.from("SLAVE"));
    currentMapping.get(PARTITIONS[2]).put(NODES[2], State.from("MASTER"));
    znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);
    preferenceLists = znRecord.getListFields();
    for (PartitionId partition : currentMapping.keySet()) {
      List<String> preferenceList = preferenceLists.get(partition.toString());
      Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.size(), 2, "invalid preference list for " + partition);
      Map<ParticipantId, State> stateMap = currentMapping.get(partition);
      for (ParticipantId participant : stateMap.keySet()) {
        Assert.assertTrue(preferenceList.contains(participant.toString()),
            "minimal movement violated for " + partition);
      }
      for (String participant : preferenceList) {
        if (!stateMap.containsKey(ParticipantId.from(participant))) {
          Assert.assertNotSame(preferenceList.get(0), participant,
              "newly moved replica should not be master for " + partition);
        }
      }
    }

    // finally, adjust the current mapping to reflect 2 nodes and make sure everything's even again
    for (PartitionId partition : PARTITIONS) {
      currentMapping.get(partition).clear();
    }
    currentMapping.get(PARTITIONS[0]).put(NODES[1], State.from("MASTER"));
    currentMapping.get(PARTITIONS[0]).put(NODES[2], State.from("SLAVE"));
    currentMapping.get(PARTITIONS[1]).put(NODES[1], State.from("SLAVE"));
    currentMapping.get(PARTITIONS[1]).put(NODES[2], State.from("MASTER"));
    currentMapping.get(PARTITIONS[2]).put(NODES[1], State.from("SLAVE"));
    currentMapping.get(PARTITIONS[2]).put(NODES[2], State.from("MASTER"));
    znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);
    preferenceLists = znRecord.getListFields();
    firstNodes.clear();
    for (PartitionId partition : currentMapping.keySet()) {
      List<String> preferenceList = preferenceLists.get(partition.toString());
      Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
      Assert.assertEquals(preferenceList.size(), 2, "invalid preference list for " + partition);
      firstNodes.add(preferenceList.get(0));
    }
    Assert.assertEquals(firstNodes.size(), 2, "masters not evenly distributed");
  }

  /**
   * Tests the following scenario: there is only a single partition for a resource. Two nodes up,
   * partition should
   * be assigned to one of them. Take down that node, partition should move. Bring back up that
   * node, partition should not move unnecessarily.
   */
  @Test
  public void testWontMoveSinglePartitionUnnecessarily() {
    final ResourceId RESOURCE = ResourceId.from("resource");
    final PartitionId partition = PartitionId.from("resource_0");
    final StateModelDefinition STATE_MODEL =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline());
    LinkedHashMap<State, Integer> stateCount = Maps.newLinkedHashMap();
    stateCount.put(State.from("ONLINE"), 1);
    final ParticipantId[] NODES = {
        ParticipantId.from("n0"), ParticipantId.from("n1")
    };

    ReplicaPlacementScheme scheme = new AutoRebalanceStrategy.DefaultPlacementScheme();
    // initial state, one node, no mapping
    List<ParticipantId> allNodes = Lists.newArrayList(NODES);
    List<ParticipantId> liveNodes = Lists.newArrayList(NODES);
    Map<PartitionId, Map<ParticipantId, State>> currentMapping = Maps.newHashMap();
    currentMapping.put(partition, new HashMap<ParticipantId, State>());

    // Both nodes there
    List<PartitionId> partitions = Lists.newArrayList(partition);
    Map<State, String> upperBounds = Maps.newHashMap();
    for (State state : STATE_MODEL.getTypedStatesPriorityList()) {
      upperBounds.put(state, STATE_MODEL.getNumParticipantsPerState(state));
    }

    ZNRecord znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);
    Map<String, List<String>> preferenceLists = znRecord.getListFields();
    List<String> preferenceList = preferenceLists.get(partition.toString());
    Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
    Assert.assertEquals(preferenceList.size(), 1, "invalid preference list for " + partition);
    String state = znRecord.getMapField(partition.toString()).get(preferenceList.get(0));
    Assert.assertEquals(state, "ONLINE", "Invalid state for " + partition);
    ParticipantId preferredNode = ParticipantId.from(preferenceList.get(0));
    ParticipantId otherNode = preferredNode.equals(NODES[0]) ? NODES[1] : NODES[0];
    // ok, see what happens if we've got the partition on the other node (e.g. due to the preferred
    // node being down).
    currentMapping.get(partition).put(otherNode, State.from(state));

    znRecord =
        new AutoRebalanceStrategy(RESOURCE, partitions, stateCount, Integer.MAX_VALUE, scheme)
            .typedComputePartitionAssignment(liveNodes, currentMapping, allNodes);

    preferenceLists = znRecord.getListFields();
    preferenceList = preferenceLists.get(partition.toString());
    Assert.assertNotNull(preferenceList, "invalid preference list for " + partition);
    Assert.assertEquals(preferenceList.size(), 1, "invalid preference list for " + partition);
    state = znRecord.getMapField(partition.toString()).get(preferenceList.get(0));
    Assert.assertEquals(state, "ONLINE", "Invalid state for " + partition);
    ParticipantId finalPreferredNode = ParticipantId.from(preferenceList.get(0));
    // finally, make sure we haven't moved it.
    Assert.assertEquals(finalPreferredNode, otherNode);
  }
}
