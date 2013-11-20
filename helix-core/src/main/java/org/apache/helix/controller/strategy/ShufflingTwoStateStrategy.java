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

import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState.IdealStateProperty;

/*
 * Ideal state calculator for the cluster manager V1. The ideal state is
 * calculated by randomly assign primary partitions to storage nodes. This is intended for a
 * two-state scheme where one is primary and the other is secondary.
 *
 * Note that the following code is a native strategy and is for cluster manager V1 only. We will
 * use the other algorithm to calculate the ideal state in future milestones.
 *
 *
 * */

public class ShufflingTwoStateStrategy {
  /*
   * Given the number of nodes, partitions and replica number, calculate the
   * ideal state in the following manner: For the primary partition assignment,
   * 1. construct Arraylist partitionList, with partitionList[i] = i; 2. Shuffle
   * the partitions array 3. Scan the shuffled array, then assign
   * partitionList[i] to node (i % nodes)
   * for the slave partitions, simply put them in the node after the node that
   * contains the master partition.
   * The result of the method is a ZNRecord, which contains a list of maps; each
   * map is from the name of nodes to either state name ("MASTER" or "SLAVE" for
   * MasterSlave).
   */

  /**
   * Calculate an ideal state for a MasterSlave configuration
   */
  public static ZNRecord calculateIdealState(List<String> instanceNames, int partitions,
      int replicas, String resourceName, long randomSeed) {
    return calculateIdealState(instanceNames, partitions, replicas, resourceName, randomSeed,
        "MASTER", "SLAVE");
  }

  public static ZNRecord calculateIdealState(List<String> instanceNames, int partitions,
      int replicas, String resourceName, long randomSeed, String primaryValue, String secondaryValue) {
    if (instanceNames.size() <= replicas) {
      throw new IllegalArgumentException("Replicas must be less than number of nodes");
    }

    Collections.sort(instanceNames);

    ZNRecord result = new ZNRecord(resourceName);

    List<Integer> partitionList = new ArrayList<Integer>(partitions);
    for (int i = 0; i < partitions; i++) {
      partitionList.add(new Integer(i));
    }
    Random rand = new Random(randomSeed);
    // Shuffle the partition list
    Collections.shuffle(partitionList, rand);

    for (int i = 0; i < partitionList.size(); i++) {
      int partitionId = partitionList.get(i);
      Map<String, String> partitionAssignment = new TreeMap<String, String>();
      int primaryNode = i % instanceNames.size();
      // the first in the list is the node that contains the primary
      partitionAssignment.put(instanceNames.get(primaryNode), primaryValue);

      // for the jth replica, we put it on (primaryNode + j) % nodes-th
      // node
      for (int j = 1; j <= replicas; j++) {
        int index = (primaryNode + j * partitionList.size()) % instanceNames.size();
        while (partitionAssignment.keySet().contains(instanceNames.get(index))) {
          index = (index + 1) % instanceNames.size();
        }
        partitionAssignment.put(instanceNames.get(index), secondaryValue);
      }
      String partitionName = resourceName + "_" + partitionId;
      result.setMapField(partitionName, partitionAssignment);
    }
    result.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(), String.valueOf(partitions));
    return result;
  }

  public static ZNRecord calculateIdealState(List<String> instanceNames, int partitions,
      int replicas, String resourceName) {
    long randomSeed = 888997632;
    // seed is a constant, so that the shuffle always give same result
    return calculateIdealState(instanceNames, partitions, replicas, resourceName, randomSeed);
  }
}
