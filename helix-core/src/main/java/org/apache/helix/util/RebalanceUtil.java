package org.apache.helix.util;

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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixException;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;

public class RebalanceUtil {
  public static Map<String, Object> buildInternalIdealState(IdealState state) {
    // Try parse the partition number from name DB_n. If not, sort the partitions and
    // assign id
    Map<String, Integer> partitionIndex = new HashMap<String, Integer>();
    Map<String, String> reversePartitionIndex = new HashMap<String, String>();
    boolean indexInPartitionName = true;
    for (PartitionId partitionId : state.getPartitionIdSet()) {
      String partitionName = partitionId.stringify();
      int lastPos = partitionName.lastIndexOf("_");
      if (lastPos < 0) {
        indexInPartitionName = false;
        break;
      }
      try {
        String idStr = partitionName.substring(lastPos + 1);
        int partition = Integer.parseInt(idStr);
        partitionIndex.put(partitionName, partition);
        reversePartitionIndex.put(state.getResourceId().stringify() + "_" + partition,
            partitionName);
      } catch (Exception e) {
        indexInPartitionName = false;
        partitionIndex.clear();
        reversePartitionIndex.clear();
        break;
      }
    }

    if (indexInPartitionName == false) {
      List<String> partitions = new ArrayList<String>();
      partitions.addAll(Lists.transform(Lists.newArrayList(state.getPartitionIdSet()),
          Functions.toStringFunction()));
      Collections.sort(partitions);
      for (int i = 0; i < partitions.size(); i++) {
        partitionIndex.put(partitions.get(i), i);
        reversePartitionIndex.put(state.getResourceId().stringify() + "_" + i, partitions.get(i));
      }
    }

    Map<String, List<Integer>> nodeMasterAssignmentMap = new TreeMap<String, List<Integer>>();
    Map<String, Map<String, List<Integer>>> combinedNodeSlaveAssignmentMap =
        new TreeMap<String, Map<String, List<Integer>>>();
    for (PartitionId partition : state.getPartitionIdSet()) {
      List<String> instances = state.getRecord().getListField(partition.stringify());
      String master = instances.get(0);
      if (!nodeMasterAssignmentMap.containsKey(master)) {
        nodeMasterAssignmentMap.put(master, new ArrayList<Integer>());
      }
      if (!combinedNodeSlaveAssignmentMap.containsKey(master)) {
        combinedNodeSlaveAssignmentMap.put(master, new TreeMap<String, List<Integer>>());
      }
      nodeMasterAssignmentMap.get(master).add(partitionIndex.get(partition));
      for (int i = 1; i < instances.size(); i++) {
        String instance = instances.get(i);
        Map<String, List<Integer>> slaveMap = combinedNodeSlaveAssignmentMap.get(master);
        if (!slaveMap.containsKey(instance)) {
          slaveMap.put(instance, new ArrayList<Integer>());
        }
        slaveMap.get(instance).add(partitionIndex.get(partition));
      }
    }

    Map<String, Object> result = new TreeMap<String, Object>();
    result.put("PrimaryAssignmentMap", nodeMasterAssignmentMap);
    result.put("SecondaryAssignmentMap", combinedNodeSlaveAssignmentMap);
    result.put("replicas", Integer.parseInt(state.getReplicas()));
    result.put("partitions", new Integer(state.getRecord().getListFields().size()));
    result.put("reversePartitionIndex", reversePartitionIndex);
    return result;
  }

  public static String[] parseStates(String clusterName, StateModelDefinition stateModDef) {
    String[] result = new String[2];
    String masterStateValue = null, slaveStateValue = null;

    // StateModelDefinition def = new StateModelDefinition(stateModDef);

    List<String> statePriorityList = stateModDef.getStatesPriorityList();

    for (String state : statePriorityList) {
      String count = stateModDef.getNumInstancesPerState(state);
      if (count.equals("1")) {
        if (masterStateValue != null) {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        masterStateValue = state;
      } else if (count.equalsIgnoreCase("R")) {
        if (slaveStateValue != null) {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        slaveStateValue = state;
      } else if (count.equalsIgnoreCase("N")) {
        if (!(masterStateValue == null && slaveStateValue == null)) {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        masterStateValue = slaveStateValue = state;
      }
    }
    if (masterStateValue == null && slaveStateValue == null) {
      throw new HelixException("Invalid or unsupported state model definition");
    }

    if (masterStateValue == null) {
      masterStateValue = slaveStateValue;
    }
    result[0] = masterStateValue;
    result[1] = slaveStateValue;
    return result;
  }
}
