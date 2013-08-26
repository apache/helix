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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.util.RebalanceUtil;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestEspressoStorageClusterIdealState {
  @Test
  public void testEspressoStorageClusterIdealState() throws Exception {
    List<String> instanceNames = new ArrayList<String>();
    for (int i = 0; i < 5; i++) {
      instanceNames.add("localhost:123" + i);
    }
    int partitions = 8, replicas = 0;
    Map<String, Object> result0 =
        DefaultTwoStateStrategy.calculateInitialIdealState(instanceNames, partitions, replicas);
    Verify(result0, partitions, replicas);

    partitions = 8192;
    replicas = 3;

    instanceNames.clear();
    for (int i = 0; i < 20; i++) {
      instanceNames.add("localhost:123" + i);
    }
    Map<String, Object> resultOriginal =
        DefaultTwoStateStrategy.calculateInitialIdealState(instanceNames, partitions, replicas);

    Verify(resultOriginal, partitions, replicas);
    printStat(resultOriginal);

    Map<String, Object> result1 =
        DefaultTwoStateStrategy.calculateInitialIdealState(instanceNames, partitions, replicas);

    List<String> instanceNames2 = new ArrayList<String>();
    for (int i = 30; i < 35; i++) {
      instanceNames2.add("localhost:123" + i);
    }

    DefaultTwoStateStrategy.calculateNextIdealState(instanceNames2, result1);

    List<String> instanceNames3 = new ArrayList<String>();
    for (int i = 35; i < 40; i++) {
      instanceNames3.add("localhost:123" + i);
    }

    DefaultTwoStateStrategy.calculateNextIdealState(instanceNames3, result1);
    Double masterKeepRatio = 0.0, slaveKeepRatio = 0.0;
    Verify(result1, partitions, replicas);
    double[] result = compareResult(resultOriginal, result1);
    masterKeepRatio = result[0];
    slaveKeepRatio = result[1];
    Assert.assertTrue(0.66 < masterKeepRatio && 0.67 > masterKeepRatio);
    Assert.assertTrue(0.66 < slaveKeepRatio && 0.67 > slaveKeepRatio);

  }

  @Test
  public void testRebalance2() {
    int partitions = 1256, replicas = 3;
    List<String> instanceNames = new ArrayList<String>();

    for (int i = 0; i < 10; i++) {
      instanceNames.add("localhost:123" + i);
    }

    Map<String, Object> resultOriginal =
        DefaultTwoStateStrategy.calculateInitialIdealState(instanceNames, partitions, replicas);

    ZNRecord idealState1 =
        DefaultTwoStateStrategy.convertToZNRecord(resultOriginal, "TestDB", "MASTER", "SLAVE");

    Map<String, Object> result1 =
        RebalanceUtil.buildInternalIdealState(new IdealState(idealState1));
    List<String> instanceNames2 = new ArrayList<String>();
    for (int i = 30; i < 35; i++) {
      instanceNames2.add("localhost:123" + i);
    }

    Map<String, Object> result2 =
        DefaultTwoStateStrategy.calculateNextIdealState(instanceNames2, result1);

    Verify(resultOriginal, partitions, replicas);
    Verify(result2, partitions, replicas);
    Double masterKeepRatio = 0.0, slaveKeepRatio = 0.0;
    double[] result = compareResult(resultOriginal, result2);
    masterKeepRatio = result[0];
    slaveKeepRatio = result[1];
    Assert.assertTrue(0.66 < masterKeepRatio && 0.67 > masterKeepRatio);
    Assert.assertTrue(0.66 < slaveKeepRatio && 0.67 > slaveKeepRatio);
  }

  public static void Verify(Map<String, Object> result, int partitions, int replicas) {
    Map<String, List<Integer>> masterAssignmentMap =
        (Map<String, List<Integer>>) (result.get("PrimaryAssignmentMap"));
    Map<String, Map<String, List<Integer>>> nodeSlaveAssignmentMap =
        (Map<String, Map<String, List<Integer>>>) (result.get("SecondaryAssignmentMap"));

    AssertJUnit.assertTrue(partitions == (Integer) (result.get("partitions")));

    // Verify master partitions covers all master partitions on each node
    Map<Integer, Integer> masterCounterMap = new TreeMap<Integer, Integer>();
    for (int i = 0; i < partitions; i++) {
      masterCounterMap.put(i, 0);
    }

    int minMasters = Integer.MAX_VALUE, maxMasters = Integer.MIN_VALUE;
    for (String instanceName : masterAssignmentMap.keySet()) {
      List<Integer> masterList = masterAssignmentMap.get(instanceName);
      // the assert needs to be changed when weighting is introduced
      // AssertJUnit.assertTrue(masterList.size() == partitions /masterAssignmentMap.size() |
      // masterList.size() == (partitions /masterAssignmentMap.size()+1) );

      for (Integer x : masterList) {
        AssertJUnit.assertTrue(masterCounterMap.get(x) == 0);
        masterCounterMap.put(x, 1);
      }
      if (minMasters > masterList.size()) {
        minMasters = masterList.size();
      }
      if (maxMasters < masterList.size()) {
        maxMasters = masterList.size();
      }
    }
    // Master partition should be evenly distributed most of the time
    System.out.println("Masters: max: " + maxMasters + " Min:" + minMasters);
    // Each master partition should occur only once
    for (int i = 0; i < partitions; i++) {
      AssertJUnit.assertTrue(masterCounterMap.get(i) == 1);
    }
    AssertJUnit.assertTrue(masterCounterMap.size() == partitions);

    // for each node, verify the master partitions and the slave partition assignment map
    if (replicas == 0) {
      AssertJUnit.assertTrue(nodeSlaveAssignmentMap.size() == 0);
      return;
    }

    AssertJUnit.assertTrue(masterAssignmentMap.size() == nodeSlaveAssignmentMap.size());
    for (String instanceName : masterAssignmentMap.keySet()) {
      AssertJUnit.assertTrue(nodeSlaveAssignmentMap.containsKey(instanceName));

      Map<String, List<Integer>> slaveAssignmentMap = nodeSlaveAssignmentMap.get(instanceName);
      Map<Integer, Integer> slaveCountMap = new TreeMap<Integer, Integer>();
      List<Integer> masterList = masterAssignmentMap.get(instanceName);

      for (Integer masterPartitionId : masterList) {
        slaveCountMap.put(masterPartitionId, 0);
      }
      // Make sure that masterList are covered replica times by the slave assignment.
      int minSlaves = Integer.MAX_VALUE, maxSlaves = Integer.MIN_VALUE;
      for (String hostInstance : slaveAssignmentMap.keySet()) {
        List<Integer> slaveAssignment = slaveAssignmentMap.get(hostInstance);
        Set<Integer> occurenceSet = new HashSet<Integer>();

        // Each slave should occur only once in the list, since the list is per-node slaves
        for (Integer slavePartition : slaveAssignment) {
          AssertJUnit.assertTrue(!occurenceSet.contains(slavePartition));
          occurenceSet.add(slavePartition);

          slaveCountMap.put(slavePartition, slaveCountMap.get(slavePartition) + 1);
        }
        if (minSlaves > slaveAssignment.size()) {
          minSlaves = slaveAssignment.size();
        }
        if (maxSlaves < slaveAssignment.size()) {
          maxSlaves = slaveAssignment.size();
        }
      }
      // check if slave distribution is even
      AssertJUnit.assertTrue(maxSlaves - minSlaves <= 1);
      // System.out.println("Slaves: max: "+maxSlaves+" Min:"+ minSlaves);

      // for each node, the slave assignment map should cover the masters for exactly replica
      // times
      AssertJUnit.assertTrue(slaveCountMap.size() == masterList.size());
      for (Integer masterPartitionId : masterList) {
        AssertJUnit.assertTrue(slaveCountMap.get(masterPartitionId) == replicas);
      }
    }

  }

  public void printStat(Map<String, Object> result) {
    // print out master distribution

    // print out slave distribution

  }

  public static double[] compareResult(Map<String, Object> result1, Map<String, Object> result2) {
    double[] result = new double[2];
    Map<String, List<Integer>> masterAssignmentMap1 =
        (Map<String, List<Integer>>) (result1.get("PrimaryAssignmentMap"));
    Map<String, Map<String, List<Integer>>> nodeSlaveAssignmentMap1 =
        (Map<String, Map<String, List<Integer>>>) (result1.get("SecondaryAssignmentMap"));

    Map<String, List<Integer>> masterAssignmentMap2 =
        (Map<String, List<Integer>>) (result2.get("PrimaryAssignmentMap"));
    Map<String, Map<String, List<Integer>>> nodeSlaveAssignmentMap2 =
        (Map<String, Map<String, List<Integer>>>) (result2.get("SecondaryAssignmentMap"));

    int commonMasters = 0;
    int commonSlaves = 0;
    int partitions = (Integer) (result1.get("partitions"));
    int replicas = (Integer) (result1.get("replicas"));

    AssertJUnit.assertTrue((Integer) (result2.get("partitions")) == partitions);
    AssertJUnit.assertTrue((Integer) (result2.get("replicas")) == replicas);

    // masterMap1 maps from partition id to the holder instance name
    Map<Integer, String> masterMap1 = new TreeMap<Integer, String>();
    for (String instanceName : masterAssignmentMap1.keySet()) {
      List<Integer> masterList1 = masterAssignmentMap1.get(instanceName);
      for (Integer partition : masterList1) {
        AssertJUnit.assertTrue(!masterMap1.containsKey(partition));
        masterMap1.put(partition, instanceName);
      }
    }
    // go through masterAssignmentMap2 and find out the common number
    for (String instanceName : masterAssignmentMap2.keySet()) {
      List<Integer> masterList2 = masterAssignmentMap2.get(instanceName);
      for (Integer partition : masterList2) {
        if (masterMap1.get(partition).equalsIgnoreCase(instanceName)) {
          commonMasters++;
        }
      }
    }

    result[0] = 1.0 * commonMasters / partitions;
    System.out.println(commonMasters + " master partitions are kept, "
        + (partitions - commonMasters) + " moved, keep ratio:" + 1.0 * commonMasters / partitions);

    // maps from the partition id to the instance names that holds its slave partition
    Map<Integer, Set<String>> slaveMap1 = new TreeMap<Integer, Set<String>>();
    for (String instanceName : nodeSlaveAssignmentMap1.keySet()) {
      Map<String, List<Integer>> slaveAssignment1 = nodeSlaveAssignmentMap1.get(instanceName);
      for (String slaveHostName : slaveAssignment1.keySet()) {
        List<Integer> slaveList = slaveAssignment1.get(slaveHostName);
        for (Integer partition : slaveList) {
          if (!slaveMap1.containsKey(partition)) {
            slaveMap1.put(partition, new TreeSet<String>());
          }
          AssertJUnit.assertTrue(!slaveMap1.get(partition).contains(slaveHostName));
          slaveMap1.get(partition).add(slaveHostName);
        }
      }
    }

    for (String instanceName : nodeSlaveAssignmentMap2.keySet()) {
      Map<String, List<Integer>> slaveAssignment2 = nodeSlaveAssignmentMap2.get(instanceName);
      for (String slaveHostName : slaveAssignment2.keySet()) {
        List<Integer> slaveList = slaveAssignment2.get(slaveHostName);
        for (Integer partition : slaveList) {
          if (slaveMap1.get(partition).contains(slaveHostName)) {
            commonSlaves++;
          }
        }
      }
    }
    result[1] = 1.0 * commonSlaves / partitions / replicas;
    System.out.println(commonSlaves + " slave partitions are kept, "
        + (partitions * replicas - commonSlaves) + " moved. keep ratio:" + 1.0 * commonSlaves
        / partitions / replicas);
    return result;
  }

}
