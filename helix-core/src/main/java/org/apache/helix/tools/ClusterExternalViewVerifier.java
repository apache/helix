package org.apache.helix.tools;

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

import org.apache.helix.api.Cluster;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.ResourceAssignment;
import org.apache.log4j.Logger;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * given zk, cluster, and a list of expected live-instances
 * check whether cluster's external-view reaches best-possible states
 */
public class ClusterExternalViewVerifier extends ClusterVerifier {
  private static Logger LOG = Logger.getLogger(ClusterExternalViewVerifier.class);

  final List<String> _expectSortedLiveNodes; // always sorted

  public ClusterExternalViewVerifier(ZkClient zkclient, String clusterName,
      List<String> expectLiveNodes) {
    super(zkclient, clusterName);
    _expectSortedLiveNodes = expectLiveNodes;
    Collections.sort(_expectSortedLiveNodes);
  }

  boolean verifyLiveNodes(List<ParticipantId> actualLiveNodes) {
    Collections.sort(actualLiveNodes);
    List<String> rawActualLiveNodes =
        Lists.transform(actualLiveNodes, Functions.toStringFunction());
    return _expectSortedLiveNodes.equals(rawActualLiveNodes);
  }

  /**
   * @param externalView
   * @param bestPossibleState map of partition to map of instance to state
   * @return
   */
  boolean verifyExternalView(ExternalView externalView,
      Map<PartitionId, Map<String, String>> bestPossibleState) {
    Map<String, Map<String, String>> bestPossibleStateMap =
        convertBestPossibleState(bestPossibleState);
    // trimBestPossibleState(bestPossibleStateMap);

    Map<String, Map<String, String>> externalViewMap = externalView.getRecord().getMapFields();
    return externalViewMap.equals(bestPossibleStateMap);
  }

  static void runStage(ClusterEvent event, Stage stage) throws Exception {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    stage.process(event);
    stage.postProcess();
  }

  BestPossibleStateOutput calculateBestPossibleState(Cluster cluster) throws Exception {
    ClusterEvent event = new ClusterEvent("event");
    event.addAttribute("Cluster", cluster);

    List<Stage> stages = new ArrayList<Stage>();
    stages.add(new ResourceComputationStage());
    stages.add(new CurrentStateComputationStage());
    stages.add(new BestPossibleStateCalcStage());

    for (Stage stage : stages) {
      runStage(event, stage);
    }

    return event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
  }

  /**
   * remove empty map and DROPPED state from best possible state
   * @param bestPossibleState
   */
  // static void trimBestPossibleState(Map<String, Map<String, String>> bestPossibleState) {
  // Iterator<Entry<String, Map<String, String>>> iter = bestPossibleState.entrySet().iterator();
  // while (iter.hasNext()) {
  // Map.Entry<String, Map<String, String>> entry = iter.next();
  // Map<String, String> instanceStateMap = entry.getValue();
  // if (instanceStateMap.isEmpty()) {
  // iter.remove();
  // } else {
  // // remove instances with DROPPED state
  // Iterator<Map.Entry<String, String>> insIter = instanceStateMap.entrySet().iterator();
  // while (insIter.hasNext()) {
  // Map.Entry<String, String> insEntry = insIter.next();
  // String state = insEntry.getValue();
  // if (state.equalsIgnoreCase(HelixDefinedState.DROPPED.toString())) {
  // insIter.remove();
  // }
  // }
  // }
  // }
  // }

  static Map<String, Map<String, String>> convertBestPossibleState(
      Map<PartitionId, Map<String, String>> bestPossibleState) {
    Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
    for (PartitionId partition : bestPossibleState.keySet()) {
      result.put(partition.stringify(), bestPossibleState.get(partition));
    }
    return result;
  }

  @Override
  public boolean verify() throws Exception {
    ClusterAccessor clusterAccessor = new ClusterAccessor(ClusterId.from(_clusterName), _accessor);
    Cluster cluster = clusterAccessor.readCluster();

    List<ParticipantId> liveInstances = new ArrayList<ParticipantId>();
    liveInstances.addAll(cluster.getLiveParticipantMap().keySet());
    boolean success = verifyLiveNodes(liveInstances);
    if (!success) {
      LOG.info("liveNodes not match, expect: " + _expectSortedLiveNodes + ", actual: "
          + liveInstances);
      return false;
    }

    BestPossibleStateOutput bestPossbileStates = calculateBestPossibleState(cluster);
    Map<String, ExternalView> externalViews =
        _accessor.getChildValuesMap(_keyBuilder.externalViews());

    // TODO all ideal-states should be included in external-views

    for (String resourceName : externalViews.keySet()) {
      ExternalView externalView = externalViews.get(resourceName);
      ResourceAssignment assignment =
          bestPossbileStates.getResourceAssignment(ResourceId.from(resourceName));
      final Map<PartitionId, Map<String, String>> bestPossibleState = Maps.newHashMap();
      for (PartitionId partitionId : assignment.getMappedPartitionIds()) {
        Map<String, String> rawStateMap =
            ResourceAssignment.stringMapFromReplicaMap(assignment.getReplicaMap(partitionId));
        bestPossibleState.put(partitionId, rawStateMap);
      }
      success = verifyExternalView(externalView, bestPossibleState);
      if (!success) {
        LOG.info("external-view for resource: " + resourceName + " not match");
        return false;
      }
    }

    return true;
  }

}
