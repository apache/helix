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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState.IdealStateProperty;

/**
 * DefaultIdealStateCalculator tries to optimally allocate two state partitions among
 * storage nodes.
 * Given a batch of storage nodes, the partition and replication factor, the algorithm first given a
 * initial state
 * When new batches of storage nodes are added, the algorithm will calculate the new ideal state
 * such that the total
 * partition movements are minimized.
 */
public class DefaultTwoStateStrategy {
  static final String _PrimaryAssignmentMap = "PrimaryAssignmentMap";
  static final String _SecondaryAssignmentMap = "SecondaryAssignmentMap";
  static final String _partitions = "partitions";
  static final String _replicas = "replicas";

  /**
   * Calculate the initial ideal state given a batch of storage instances, the replication factor
   * and
   * number of partitions
   * 1. Calculate the primary state assignment by random shuffling
   * 2. for each storage instance, calculate the 1st secondary state assignment map, by another
   * random shuffling
   * 3. for each storage instance, calculate the i-th secondary state assignment map
   * 4. Combine the i-th secondary state assignment maps together
   * @param instanceNames
   *          list of storage node instances
   * @param partitions
   *          number of partitions
   * @param replicas
   *          The number of replicas (secondary partitions) per primary partition
   * @param primaryStateValue
   *          primary state value: e.g. "MASTER" or "LEADER"
   * @param secondaryStateValue
   *          secondary state value: e.g. "SLAVE" or "STANDBY"
   * @param resourceName
   * @return a ZNRecord that contain the idealstate info
   */
  public static ZNRecord calculateIdealState(List<String> instanceNames, int partitions,
      int replicas, String resourceName, String primaryStateValue, String secondaryStateValue) {
    Collections.sort(instanceNames);
    if (instanceNames.size() < replicas + 1) {
      throw new HelixException("Number of instances must not be less than replicas + 1. "
          + "instanceNr:" + instanceNames.size() + ", replicas:" + replicas);
    } else if (partitions < instanceNames.size()) {
      ZNRecord idealState =
          ShufflingTwoStateStrategy.calculateIdealState(instanceNames, partitions, replicas,
              resourceName, 12345, primaryStateValue, secondaryStateValue);
      int i = 0;
      for (String partitionId : idealState.getMapFields().keySet()) {
        Map<String, String> partitionAssignmentMap = idealState.getMapField(partitionId);
        List<String> partitionAssignmentPriorityList = new ArrayList<String>();
        String primaryInstance = "";
        for (String instanceName : partitionAssignmentMap.keySet()) {
          if (partitionAssignmentMap.get(instanceName).equalsIgnoreCase(primaryStateValue)
              && primaryInstance.equals("")) {
            primaryInstance = instanceName;
          } else {
            partitionAssignmentPriorityList.add(instanceName);
          }
        }
        Collections.shuffle(partitionAssignmentPriorityList, new Random(i++));
        partitionAssignmentPriorityList.add(0, primaryInstance);
        idealState.setListField(partitionId, partitionAssignmentPriorityList);
      }
      return idealState;
    }

    Map<String, Object> result = calculateInitialIdealState(instanceNames, partitions, replicas);

    return convertToZNRecord(result, resourceName, primaryStateValue, secondaryStateValue);
  }

  public static ZNRecord calculateIdealStateBatch(List<List<String>> instanceBatches,
      int partitions, int replicas, String resourceName, String primaryStateValue,
      String secondaryStateValue) {
    Map<String, Object> result =
        calculateInitialIdealState(instanceBatches.get(0), partitions, replicas);

    for (int i = 1; i < instanceBatches.size(); i++) {
      result = calculateNextIdealState(instanceBatches.get(i), result);
    }

    return convertToZNRecord(result, resourceName, primaryStateValue, secondaryStateValue);
  }

  /**
   * Convert the internal result (stored as a Map<String, Object>) into ZNRecord.
   */
  public static ZNRecord convertToZNRecord(Map<String, Object> result, String resourceName,
      String primaryStateValue, String secondaryStateValue) {
    Map<String, List<Integer>> nodePrimaryAssignmentMap =
        (Map<String, List<Integer>>) (result.get(_PrimaryAssignmentMap));
    Map<String, Map<String, List<Integer>>> nodeSecondaryAssignmentMap =
        (Map<String, Map<String, List<Integer>>>) (result.get(_SecondaryAssignmentMap));

    int partitions = (Integer) (result.get("partitions"));

    ZNRecord idealState = new ZNRecord(resourceName);
    idealState.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(),
        String.valueOf(partitions));

    for (String instanceName : nodePrimaryAssignmentMap.keySet()) {
      for (Integer partitionId : nodePrimaryAssignmentMap.get(instanceName)) {
        String partitionName = resourceName + "_" + partitionId;
        if (!idealState.getMapFields().containsKey(partitionName)) {
          idealState.setMapField(partitionName, new TreeMap<String, String>());
        }
        idealState.getMapField(partitionName).put(instanceName, primaryStateValue);
      }
    }

    for (String instanceName : nodeSecondaryAssignmentMap.keySet()) {
      Map<String, List<Integer>> secondaryAssignmentMap =
          nodeSecondaryAssignmentMap.get(instanceName);

      for (String secondaryNode : secondaryAssignmentMap.keySet()) {
        List<Integer> secondaryAssignment = secondaryAssignmentMap.get(secondaryNode);
        for (Integer partitionId : secondaryAssignment) {
          String partitionName = resourceName + "_" + partitionId;
          idealState.getMapField(partitionName).put(secondaryNode, secondaryStateValue);
        }
      }
    }
    // generate the priority list of instances per partition. the primary should be at front
    // and the secondaries follow.

    for (String partitionId : idealState.getMapFields().keySet()) {
      Map<String, String> partitionAssignmentMap = idealState.getMapField(partitionId);
      List<String> partitionAssignmentPriorityList = new ArrayList<String>();
      String primaryInstance = "";
      for (String instanceName : partitionAssignmentMap.keySet()) {
        if (partitionAssignmentMap.get(instanceName).equalsIgnoreCase(primaryStateValue)
            && primaryInstance.equals("")) {
          primaryInstance = instanceName;
        } else {
          partitionAssignmentPriorityList.add(instanceName);
        }
      }
      Collections.shuffle(partitionAssignmentPriorityList);
      partitionAssignmentPriorityList.add(0, primaryInstance);
      idealState.setListField(partitionId, partitionAssignmentPriorityList);
    }
    assert (result.containsKey("replicas"));
    idealState.setSimpleField(IdealStateProperty.REPLICAS.toString(), result.get("replicas")
        .toString());
    return idealState;
  }

  /**
   * Calculate the initial ideal state given a batch of storage instances, the replication factor
   * and
   * number of partitions
   * 1. Calculate the primary assignment by random shuffling
   * 2. for each storage instance, calculate the 1st secondary state assignment map, by another
   * random shuffling
   * 3. for each storage instance, calculate the i-th secondary state assignment map
   * 4. Combine the i-th secondary state assignment maps together
   * @param instanceNames
   *          list of storage node instances
   * @param weight
   *          weight for the initial storage node (each node has the same weight)
   * @param partitions
   *          number of partitions
   * @param replicas
   *          The number of replicas (secondary partitions) per primary partition
   * @return a map that contain the idealstate info
   */
  public static Map<String, Object> calculateInitialIdealState(List<String> instanceNames,
      int partitions, int replicas) {
    Random r = new Random(54321);
    assert (replicas <= instanceNames.size() - 1);
    ArrayList<Integer> primaryPartitionAssignment = new ArrayList<Integer>();
    for (int i = 0; i < partitions; i++) {
      primaryPartitionAssignment.add(i);
    }
    // shuffle the partition id array
    Collections.shuffle(primaryPartitionAssignment, new Random(r.nextInt()));

    // 1. Generate the random primary partition assignment
    // instanceName -> List of primary partitions on that instance
    Map<String, List<Integer>> nodePrimaryAssignmentMap = new TreeMap<String, List<Integer>>();
    for (int i = 0; i < primaryPartitionAssignment.size(); i++) {
      String instanceName = instanceNames.get(i % instanceNames.size());
      if (!nodePrimaryAssignmentMap.containsKey(instanceName)) {
        nodePrimaryAssignmentMap.put(instanceName, new ArrayList<Integer>());
      }
      nodePrimaryAssignmentMap.get(instanceName).add(primaryPartitionAssignment.get(i));
    }

    // instanceName -> secondary assignment for its primary partitions
    // secondary assignment: instanceName -> list of secondary partitions on it
    List<Map<String, Map<String, List<Integer>>>> nodeSecondaryAssignmentMapsList =
        new ArrayList<Map<String, Map<String, List<Integer>>>>(replicas);

    Map<String, Map<String, List<Integer>>> firstNodeSecondaryAssignmentMap =
        new TreeMap<String, Map<String, List<Integer>>>();
    Map<String, Map<String, List<Integer>>> combinedNodeSecondaryAssignmentMap =
        new TreeMap<String, Map<String, List<Integer>>>();

    if (replicas > 0) {
      // 2. For each node, calculate the evenly distributed secondary state as the first secondary
      // state assignment
      // We will figure out the 2nd ...replicas-th secondary state assignment based on the first
      // level secondary state assignment
      for (int i = 0; i < instanceNames.size(); i++) {
        List<String> secondaryInstances = new ArrayList<String>();
        ArrayList<Integer> secondaryAssignment = new ArrayList<Integer>();
        TreeMap<String, List<Integer>> secondaryAssignmentMap =
            new TreeMap<String, List<Integer>>();

        for (int j = 0; j < instanceNames.size(); j++) {
          if (j != i) {
            secondaryInstances.add(instanceNames.get(j));
            secondaryAssignmentMap.put(instanceNames.get(j), new ArrayList<Integer>());
          }
        }
        // Get the number of primary partitions on instanceName
        List<Integer> primaryAssignment = nodePrimaryAssignmentMap.get(instanceNames.get(i));
        // do a random shuffling as in step 1, so that the first-level secondary states are
        // distributed among rest instances

        for (int j = 0; j < primaryAssignment.size(); j++) {
          secondaryAssignment.add(j);
        }
        Collections.shuffle(secondaryAssignment, new Random(r.nextInt()));

        Collections.shuffle(secondaryInstances, new Random(instanceNames.get(i).hashCode()));
        // Get the secondary assignment map of node instanceName
        for (int j = 0; j < primaryAssignment.size(); j++) {
          String secondaryInstanceName =
              secondaryInstances.get(secondaryAssignment.get(j) % secondaryInstances.size());
          if (!secondaryAssignmentMap.containsKey(secondaryInstanceName)) {
            secondaryAssignmentMap.put(secondaryInstanceName, new ArrayList<Integer>());
          }
          secondaryAssignmentMap.get(secondaryInstanceName).add(primaryAssignment.get(j));
        }
        firstNodeSecondaryAssignmentMap.put(instanceNames.get(i), secondaryAssignmentMap);
      }
      nodeSecondaryAssignmentMapsList.add(firstNodeSecondaryAssignmentMap);
      // From the first secondary assignment map, calculate the rest secondary assignment maps
      for (int replicaOrder = 1; replicaOrder < replicas; replicaOrder++) {
        // calculate the next secondary partition assignment map
        Map<String, Map<String, List<Integer>>> nextNodeSecondaryAssignmentMap =
            calculateNextSecondaryAssignemntMap(firstNodeSecondaryAssignmentMap, replicaOrder);
        nodeSecondaryAssignmentMapsList.add(nextNodeSecondaryAssignmentMap);
      }

      // Combine the calculated 1...replicas-th secondary assignment map together
      for (String instanceName : nodePrimaryAssignmentMap.keySet()) {
        Map<String, List<Integer>> combinedSecondaryAssignmentMap =
            new TreeMap<String, List<Integer>>();

        for (Map<String, Map<String, List<Integer>>> secondaryNodeAssignmentMap : nodeSecondaryAssignmentMapsList) {
          Map<String, List<Integer>> secondaryAssignmentMap =
              secondaryNodeAssignmentMap.get(instanceName);

          for (String secondaryInstance : secondaryAssignmentMap.keySet()) {
            if (!combinedSecondaryAssignmentMap.containsKey(secondaryInstance)) {
              combinedSecondaryAssignmentMap.put(secondaryInstance, new ArrayList<Integer>());
            }
            combinedSecondaryAssignmentMap.get(secondaryInstance).addAll(
                secondaryAssignmentMap.get(secondaryInstance));
          }
        }
        migrateSecondaryAssignMapToNewInstances(combinedSecondaryAssignmentMap,
            new ArrayList<String>());
        combinedNodeSecondaryAssignmentMap.put(instanceName, combinedSecondaryAssignmentMap);
      }
    }
    /*
     * // Print the result master and slave assignment maps
     * System.out.println("Master assignment:");
     * for(String instanceName : nodeMasterAssignmentMap.keySet())
     * {
     * System.out.println(instanceName+":");
     * for(Integer x : nodeMasterAssignmentMap.get(instanceName))
     * {
     * System.out.print(x+" ");
     * }
     * System.out.println();
     * System.out.println("Slave assignment:");
     * int slaveOrder = 1;
     * for(Map<String, Map<String, List<Integer>>> slaveNodeAssignmentMap :
     * nodeSlaveAssignmentMapsList)
     * {
     * System.out.println("Slave assignment order :" + (slaveOrder++));
     * Map<String, List<Integer>> slaveAssignmentMap = slaveNodeAssignmentMap.get(instanceName);
     * for(String slaveName : slaveAssignmentMap.keySet())
     * {
     * System.out.print("\t" + slaveName +":\n\t" );
     * for(Integer x : slaveAssignmentMap.get(slaveName))
     * {
     * System.out.print(x + " ");
     * }
     * System.out.println("\n");
     * }
     * }
     * System.out.println("\nCombined slave assignment map");
     * Map<String, List<Integer>> slaveAssignmentMap =
     * combinedNodeSlaveAssignmentMap.get(instanceName);
     * for(String slaveName : slaveAssignmentMap.keySet())
     * {
     * System.out.print("\t" + slaveName +":\n\t" );
     * for(Integer x : slaveAssignmentMap.get(slaveName))
     * {
     * System.out.print(x + " ");
     * }
     * System.out.println("\n");
     * }
     * }
     */
    Map<String, Object> result = new TreeMap<String, Object>();
    result.put("PrimaryAssignmentMap", nodePrimaryAssignmentMap);
    result.put("SecondaryAssignmentMap", combinedNodeSecondaryAssignmentMap);
    result.put("replicas", new Integer(replicas));
    result.put("partitions", new Integer(partitions));
    return result;
  }

  /**
   * In the case there are more than 1 secondary, we use the following algorithm to calculate the
   * n-th secondary
   * assignment map based on the first level secondary assignment map.
   * @param firstInstanceSecondaryAssignmentMap the first secondary assignment map for all instances
   * @param order of the secondary state
   * @return the n-th secondary assignment map for all the instances
   */
  static Map<String, Map<String, List<Integer>>> calculateNextSecondaryAssignemntMap(
      Map<String, Map<String, List<Integer>>> firstInstanceSecondaryAssignmentMap, int replicaOrder) {
    Map<String, Map<String, List<Integer>>> result =
        new TreeMap<String, Map<String, List<Integer>>>();

    for (String currentInstance : firstInstanceSecondaryAssignmentMap.keySet()) {
      Map<String, List<Integer>> resultAssignmentMap = new TreeMap<String, List<Integer>>();
      result.put(currentInstance, resultAssignmentMap);
    }

    for (String currentInstance : firstInstanceSecondaryAssignmentMap.keySet()) {
      Map<String, List<Integer>> previousSecondaryAssignmentMap =
          firstInstanceSecondaryAssignmentMap.get(currentInstance);
      Map<String, List<Integer>> resultAssignmentMap = result.get(currentInstance);
      int offset = replicaOrder - 1;
      for (String instance : previousSecondaryAssignmentMap.keySet()) {
        List<String> otherInstances =
            new ArrayList<String>(previousSecondaryAssignmentMap.size() - 1);
        // Obtain an array of other instances
        for (String otherInstance : previousSecondaryAssignmentMap.keySet()) {
          otherInstances.add(otherInstance);
        }
        Collections.sort(otherInstances);
        int instanceIndex = -1;
        for (int index = 0; index < otherInstances.size(); index++) {
          if (otherInstances.get(index).equalsIgnoreCase(instance)) {
            instanceIndex = index;
          }
        }
        assert (instanceIndex >= 0);
        if (instanceIndex == otherInstances.size() - 1) {
          instanceIndex--;
        }
        // Since we need to evenly distribute the secondaries on "instance" to other partitions, we
        // need to remove "instance" from the array.
        otherInstances.remove(instance);

        // distribute previous secondary assignment to other instances.
        List<Integer> previousAssignmentList = previousSecondaryAssignmentMap.get(instance);
        for (int i = 0; i < previousAssignmentList.size(); i++) {

          // Evenly distribute the previousAssignmentList to the remaining other instances
          int newInstanceIndex = (i + offset + instanceIndex) % otherInstances.size();
          String newInstance = otherInstances.get(newInstanceIndex);
          if (!resultAssignmentMap.containsKey(newInstance)) {
            resultAssignmentMap.put(newInstance, new ArrayList<Integer>());
          }
          resultAssignmentMap.get(newInstance).add(previousAssignmentList.get(i));
        }
      }
    }
    return result;
  }

  /**
   * Given the current idealState, and the list of new Instances needed to be added, calculate the
   * new Ideal state.
   * 1. Calculate how many primary partitions should be moved to the new cluster of instances
   * 2. assign the number of primary partitions px to be moved to each previous node
   * 3. for each previous node,
   * 3.1 randomly choose px nodes, move them to temp list
   * 3.2 for each px nodes, remove them from the secondary assignment map; record the map position
   * of
   * the partition;
   * 3.3 calculate # of new nodes that should be put in the secondary assignment map
   * 3.4 even-fy the secondary assignment map;
   * 3.5 randomly place # of new nodes that should be placed in
   * 4. from all the temp primary node list get from 3.1,
   * 4.1 randomly assign them to nodes in the new cluster
   * 5. for each node in the new cluster,
   * 5.1 assemble the secondary assignment map
   * 5.2 even-fy the secondary assignment map
   * @param newInstances
   *          list of new added storage node instances
   * @param weight
   *          weight for the new storage nodes (each node has the same weight)
   * @param previousIdealState
   *          The previous ideal state
   * @return a map that contain the updated idealstate info
   */
  public static Map<String, Object> calculateNextIdealState(List<String> newInstances,
      Map<String, Object> previousIdealState) {
    // Obtain the primary / secondary assignment info maps
    Collections.sort(newInstances);
    Map<String, List<Integer>> previousPrimaryAssignmentMap =
        (Map<String, List<Integer>>) (previousIdealState.get("PrimaryAssignmentMap"));
    Map<String, Map<String, List<Integer>>> nodeSecondaryAssignmentMap =
        (Map<String, Map<String, List<Integer>>>) (previousIdealState.get("SecondaryAssignmentMap"));

    List<String> oldInstances = new ArrayList<String>();
    for (String oldInstance : previousPrimaryAssignmentMap.keySet()) {
      oldInstances.add(oldInstance);
    }

    int previousInstanceNum = previousPrimaryAssignmentMap.size();
    int partitions = (Integer) (previousIdealState.get("partitions"));

    // TODO: take weight into account when calculate this

    int totalPrimaryParitionsToMove =
        partitions * (newInstances.size()) / (previousInstanceNum + newInstances.size());
    int numPrimariesFromEachNode = totalPrimaryParitionsToMove / previousInstanceNum;
    int remain = totalPrimaryParitionsToMove % previousInstanceNum;

    // Note that when remain > 0, we should make [remain] moves with (numPrimariesFromEachNode + 1)
    // partitions.
    // And we should first choose those (numPrimariesFromEachNode + 1) moves from the instances that
    // has more
    // primary partitions
    List<Integer> primaryPartitionListToMove = new ArrayList<Integer>();

    // For corresponding moved secondary partitions, keep track of their original location; the new
    // node does not
    // need to migrate all of them.
    Map<String, List<Integer>> secondaryPartitionsToMoveMap = new TreeMap<String, List<Integer>>();

    // Make sure that the instances that holds more primary partitions are put in front
    List<String> bigList = new ArrayList<String>(), smallList = new ArrayList<String>();
    for (String oldInstance : previousPrimaryAssignmentMap.keySet()) {
      List<Integer> primaryAssignmentList = previousPrimaryAssignmentMap.get(oldInstance);
      if (primaryAssignmentList.size() > numPrimariesFromEachNode) {
        bigList.add(oldInstance);
      } else {
        smallList.add(oldInstance);
      }
    }
    // "sort" the list, such that the nodes that has more primary partitions moves more partitions
    // to the
    // new added batch of instances.
    bigList.addAll(smallList);
    for (String oldInstance : bigList) {
      List<Integer> primaryAssignmentList = previousPrimaryAssignmentMap.get(oldInstance);
      int numToChoose = numPrimariesFromEachNode;
      if (remain > 0) {
        numToChoose = numPrimariesFromEachNode + 1;
        remain--;
      }
      // randomly remove numToChoose of primary partitions to the new added nodes
      ArrayList<Integer> primaryPartionsMoved = new ArrayList<Integer>();
      randomSelect(primaryAssignmentList, primaryPartionsMoved, numToChoose);

      primaryPartitionListToMove.addAll(primaryPartionsMoved);
      Map<String, List<Integer>> secondaryAssignmentMap =
          nodeSecondaryAssignmentMap.get(oldInstance);
      removeFromSecondaryAssignmentMap(secondaryAssignmentMap, primaryPartionsMoved,
          secondaryPartitionsToMoveMap);

      // Make sure that for old instances, the secondary placement map is evenly distributed
      // Trace the "local secondary moves", which should together contribute to most of the
      // secondary migrations
      migrateSecondaryAssignMapToNewInstances(secondaryAssignmentMap, newInstances);
      // System.out.println("local moves: "+ movesWithinInstance);
    }
    // System.out.println("local slave moves total: "+ totalSlaveMoves);
    // calculate the primary /secondary assignment for the new added nodes

    // We already have the list of primary partitions that will migrate to new batch of instances,
    // shuffle the partitions and assign them to new instances
    Collections.shuffle(primaryPartitionListToMove, new Random(12345));
    for (int i = 0; i < newInstances.size(); i++) {
      String newInstance = newInstances.get(i);
      List<Integer> primaryPartitionList = new ArrayList<Integer>();
      for (int j = 0; j < primaryPartitionListToMove.size(); j++) {
        if (j % newInstances.size() == i) {
          primaryPartitionList.add(primaryPartitionListToMove.get(j));
        }
      }

      Map<String, List<Integer>> secondaryPartitionMap = new TreeMap<String, List<Integer>>();
      for (String oldInstance : oldInstances) {
        secondaryPartitionMap.put(oldInstance, new ArrayList<Integer>());
      }
      // Build the secondary assignment map for the new instance, based on the saved information
      // about those secondary partition locations in secondaryPartitionsToMoveMap
      for (Integer x : primaryPartitionList) {
        for (String oldInstance : secondaryPartitionsToMoveMap.keySet()) {
          List<Integer> secondaries = secondaryPartitionsToMoveMap.get(oldInstance);
          if (secondaries.contains(x)) {
            secondaryPartitionMap.get(oldInstance).add(x);
          }
        }
      }
      // add entry for other new instances into the secondaryPartitionMap
      List<String> otherNewInstances = new ArrayList<String>();
      for (String instance : newInstances) {
        if (!instance.equalsIgnoreCase(newInstance)) {
          otherNewInstances.add(instance);
        }
      }
      // Make sure that secondary partitions are evenly distributed
      migrateSecondaryAssignMapToNewInstances(secondaryPartitionMap, otherNewInstances);

      // Update the result in the result map. We can reuse the input previousIdealState map as
      // the result.
      previousPrimaryAssignmentMap.put(newInstance, primaryPartitionList);
      nodeSecondaryAssignmentMap.put(newInstance, secondaryPartitionMap);

    }
    /*
     * // Print content of the master/ slave assignment maps
     * for(String instanceName : previousMasterAssignmentMap.keySet())
     * {
     * System.out.println(instanceName+":");
     * for(Integer x : previousMasterAssignmentMap.get(instanceName))
     * {
     * System.out.print(x+" ");
     * }
     * System.out.println("\nmaster partition moved:");
     * System.out.println();
     * System.out.println("Slave assignment:");
     * Map<String, List<Integer>> slaveAssignmentMap = nodeSlaveAssignmentMap.get(instanceName);
     * for(String slaveName : slaveAssignmentMap.keySet())
     * {
     * System.out.print("\t" + slaveName +":\n\t" );
     * for(Integer x : slaveAssignmentMap.get(slaveName))
     * {
     * System.out.print(x + " ");
     * }
     * System.out.println("\n");
     * }
     * }
     * System.out.println("Master partitions migrated to new instances");
     * for(Integer x : masterPartitionListToMove)
     * {
     * System.out.print(x+" ");
     * }
     * System.out.println();
     * System.out.println("Slave partitions migrated to new instances");
     * for(String oldInstance : slavePartitionsToMoveMap.keySet())
     * {
     * System.out.print(oldInstance + ": ");
     * for(Integer x : slavePartitionsToMoveMap.get(oldInstance))
     * {
     * System.out.print(x+" ");
     * }
     * System.out.println();
     * }
     */
    return previousIdealState;
  }

  public ZNRecord calculateNextIdealState(List<String> newInstances,
      Map<String, Object> previousIdealState, String resourceName, String primaryStateValue,
      String secondaryStateValue) {
    return convertToZNRecord(calculateNextIdealState(newInstances, previousIdealState),
        resourceName, primaryStateValue, secondaryStateValue);
  }

  /**
   * Given the list of primary partitions that will be migrated away from the storage instance,
   * Remove their entries from the local instance secondary assignment map.
   * @param secondaryAssignmentMap the local instance secondary assignment map
   * @param primaryPartionsMoved the list of primary partition ids that will be migrated away
   * @param removedAssignmentMap keep track of the removed secondary assignment info. The info can
   *          be
   *          used by new added storage nodes.
   */
  static void removeFromSecondaryAssignmentMap(Map<String, List<Integer>> secondaryAssignmentMap,
      List<Integer> primaryPartionsMoved, Map<String, List<Integer>> removedAssignmentMap) {
    for (String instanceName : secondaryAssignmentMap.keySet()) {
      List<Integer> secondaryAssignment = secondaryAssignmentMap.get(instanceName);
      for (Integer partitionId : primaryPartionsMoved) {
        if (secondaryAssignment.contains(partitionId)) {
          secondaryAssignment.remove(partitionId);
          if (!removedAssignmentMap.containsKey(instanceName)) {
            removedAssignmentMap.put(instanceName, new ArrayList<Integer>());
          }
          removedAssignmentMap.get(instanceName).add(partitionId);
        }
      }
    }
  }

  /**
   * Since some new storage instances are added, each existing storage instance should migrate some
   * secondary partitions to the new added instances.
   * The algorithm keeps moving one partition to from the instance that hosts most secondary
   * partitions
   * to the instance that hosts least number of partitions, until max-min <= 1.
   * In this way we can guarantee that all instances hosts almost same number of secondary
   * partitions, also
   * secondary partitions are evenly distributed.
   * @param secondaryAssignmentMap the local instance secondary assignment map
   * @param primaryPartionsMoved the list of primary partition ids that will be migrated away
   * @param removedAssignmentMap keep track of the removed secondary assignment info. The info can
   *          be
   *          used by new added storage nodes.
   */
  static int migrateSecondaryAssignMapToNewInstances(
      Map<String, List<Integer>> secondaryAssignmentMap, List<String> newInstances) {
    int moves = 0;
    boolean done = false;
    for (String newInstance : newInstances) {
      secondaryAssignmentMap.put(newInstance, new ArrayList<Integer>());
    }
    while (!done) {
      List<Integer> maxAssignment = null, minAssignment = null;
      int minCount = Integer.MAX_VALUE, maxCount = Integer.MIN_VALUE;
      String minInstance = "";
      for (String instanceName : secondaryAssignmentMap.keySet()) {
        List<Integer> secondaryAssignment = secondaryAssignmentMap.get(instanceName);
        if (minCount > secondaryAssignment.size()) {
          minCount = secondaryAssignment.size();
          minAssignment = secondaryAssignment;
          minInstance = instanceName;
        }
        if (maxCount < secondaryAssignment.size()) {
          maxCount = secondaryAssignment.size();
          maxAssignment = secondaryAssignment;
        }
      }
      if (maxCount - minCount <= 1) {
        done = true;
      } else {
        int indexToMove = -1;
        // find a partition that is not contained in the minAssignment list
        for (int i = 0; i < maxAssignment.size(); i++) {
          if (!minAssignment.contains(maxAssignment.get(i))) {
            indexToMove = i;
            break;
          }
        }

        minAssignment.add(maxAssignment.get(indexToMove));
        maxAssignment.remove(indexToMove);

        if (newInstances.contains(minInstance)) {
          moves++;
        }
      }
    }
    return moves;
  }

  /**
   * Randomly select a number of elements from original list and put them in the selectedList
   * The algorithm is used to select primary partitions to be migrated when new instances are added.
   * @param originalList the original list
   * @param selectedList the list that contain selected elements
   * @param num number of elements to be selected
   */
  static void randomSelect(List<Integer> originalList, List<Integer> selectedList, int num) {
    assert (originalList.size() >= num);
    int[] indexArray = new int[originalList.size()];
    for (int i = 0; i < indexArray.length; i++) {
      indexArray[i] = i;
    }
    int numRemains = originalList.size();
    Random r = new Random(numRemains);
    for (int j = 0; j < num; j++) {
      int randIndex = r.nextInt(numRemains);
      selectedList.add(originalList.get(randIndex));
      originalList.remove(randIndex);
      numRemains--;
    }
  }

  public static void main(String args[]) {
    List<String> instanceNames = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      instanceNames.add("localhost:123" + i);
    }
    int partitions = 48 * 3, replicas = 3;
    Map<String, Object> resultOriginal =
        DefaultTwoStateStrategy.calculateInitialIdealState(instanceNames, partitions, replicas);

  }
}
