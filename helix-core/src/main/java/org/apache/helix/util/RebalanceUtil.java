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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixException;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(RebalanceUtil.class.getName());

  public static Map<String, Object> buildInternalIdealState(IdealState state) {
    // Try parse the partition number from name DB_n. If not, sort the partitions and
    // assign id
    Map<String, Integer> partitionIndex = new HashMap<String, Integer>();
    Map<String, String> reversePartitionIndex = new HashMap<String, String>();
    boolean indexInPartitionName = true;
    for (String partitionId : state.getPartitionSet()) {
      int lastPos = partitionId.lastIndexOf("_");
      if (lastPos < 0) {
        indexInPartitionName = false;
        break;
      }
      try {
        String idStr = partitionId.substring(lastPos + 1);
        int partition = Integer.parseInt(idStr);
        partitionIndex.put(partitionId, partition);
        reversePartitionIndex.put(state.getResourceName() + "_" + partition, partitionId);
      } catch (Exception e) {
        indexInPartitionName = false;
        partitionIndex.clear();
        reversePartitionIndex.clear();
        break;
      }
    }

    if (indexInPartitionName == false) {
      List<String> partitions = new ArrayList<String>();
      partitions.addAll(state.getPartitionSet());
      Collections.sort(partitions);
      for (int i = 0; i < partitions.size(); i++) {
        partitionIndex.put(partitions.get(i), i);
        reversePartitionIndex.put(state.getResourceName() + "_" + i, partitions.get(i));
      }
    }

    Map<String, List<Integer>> nodeMasterAssignmentMap = new TreeMap<String, List<Integer>>();
    Map<String, Map<String, List<Integer>>> combinedNodeSlaveAssignmentMap =
        new TreeMap<String, Map<String, List<Integer>>>();
    for (String partition : state.getPartitionSet()) {
      List<String> instances = state.getRecord().getListField(partition);
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
    result.put("MasterAssignmentMap", nodeMasterAssignmentMap);
    result.put("SlaveAssignmentMap", combinedNodeSlaveAssignmentMap);
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

  /**
   * Enables/disables controller to run management mode pipeline.
   *
   * @param clusterName target cluster name
   * @param enabled enable/disable controller to management mode pipeline
   */
  public static void enableManagementMode(String clusterName, boolean enabled) {
    GenericHelixController leaderController =
        GenericHelixController.getLeaderController(clusterName);
    if (leaderController != null) {
      LOG.info("Switching management mode pipeline for cluster={}, enabled={}", clusterName,
          enabled);
      leaderController.setInManagementMode(enabled);
    } else {
      throw new HelixException(String.format("Failed to switch management mode pipeline, "
          + "enabled=%s. Controller for cluster %s does not exist", enabled, clusterName));
    }

    // Triggers an event to immediately run the pipeline
    scheduleOnDemandPipeline(clusterName, 0L);
  }

  public static void scheduleOnDemandPipeline(String clusterName, long delay) {
    scheduleOnDemandPipeline(clusterName, delay, true);
  }

  public static void scheduleOnDemandPipeline(String clusterName, long delay,
      boolean shouldRefreshCache) {
    if (clusterName == null) {
      LOG.error("Failed to issue a pipeline run. ClusterName is null.");
      return;
    }
    if (delay < 0L) {
      LOG.error("Failed to issue a pipeline run. Delay is invalid.");
      return;
    }
    GenericHelixController leaderController =
        GenericHelixController.getLeaderController(clusterName);
    if (leaderController != null) {
      leaderController.scheduleOnDemandRebalance(delay, shouldRefreshCache);
    } else {
      LOG.error("Failed to issue a pipeline. Controller for cluster {} does not exist.",
          clusterName);
    }
  }

  /**
   * Build best possible state out by copying the state map from current state output.
   * It'll be used for generating pending ST cancellation messages.
   *
   * @param resourceNames collection of resource names
   * @param currentStateOutput Current state output {@link CurrentStateOutput}
   * @return {@link BestPossibleStateOutput}
   */
  public static BestPossibleStateOutput buildBestPossibleState(Collection<String> resourceNames,
      CurrentStateOutput currentStateOutput) {
    BestPossibleStateOutput output = new BestPossibleStateOutput();

    for (String resource : resourceNames) {
      Map<Partition, Map<String, String>> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource);
      if (currentStateMap != null) {
        output.setState(resource, currentStateMap);
      }
    }

    return output;
  }

  /**
   * runStage allows the run of individual stages. It can be used to mock a part of the Controller
   * pipeline run.
   *
   * An example usage is as follows:
   *       runStage(event, new ResourceComputationStage());
   *       runStage(event, new CurrentStateComputationStage());
   *       runStage(event, new BestPossibleStateCalcStage());
   * By running these stages, we are able to obtain BestPossibleStateOutput in the event object.
   * @param event
   * @param stage
   * @throws Exception
   */
  public static void runStage(ClusterEvent event, Stage stage) throws Exception {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    stage.process(event);
    stage.postProcess();
  }
}
