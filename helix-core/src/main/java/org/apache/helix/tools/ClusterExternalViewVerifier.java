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

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * given zk, cluster, and a list of expected live-instances
 * check whether cluster's external-view reaches best-possible states
 */
/**
 * This class is deprecated, please use BestPossibleExternalViewVerifier in tools.ClusterVerifiers instead.
 */
@Deprecated
public class ClusterExternalViewVerifier extends ClusterVerifier {
  private static Logger LOG = LoggerFactory.getLogger(ClusterExternalViewVerifier.class);

  final List<String> _expectSortedLiveNodes; // always sorted

  public ClusterExternalViewVerifier(HelixZkClient zkclient, String clusterName,
      List<String> expectLiveNodes) {
    super(zkclient, clusterName);
    _expectSortedLiveNodes = expectLiveNodes;
    Collections.sort(_expectSortedLiveNodes);
  }

  boolean verifyLiveNodes(List<String> actualLiveNodes) {
    Collections.sort(actualLiveNodes);
    return _expectSortedLiveNodes.equals(actualLiveNodes);
  }

  /**
   * @param externalView
   * @param bestPossibleState map of partition to map of instance to state
   * @return
   */
  boolean verifyExternalView(ExternalView externalView,
      Map<Partition, Map<String, String>> bestPossibleState) {
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

  BestPossibleStateOutput calculateBestPossibleState(ResourceControllerDataProvider cache)
      throws Exception {
    ClusterEvent event = new ClusterEvent(ClusterEventType.StateVerifier);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    List<Stage> stages = new ArrayList<Stage>();
    stages.add(new ResourceComputationStage());
    stages.add(new CurrentStateComputationStage());
    stages.add(new BestPossibleStateCalcStage());

    for (Stage stage : stages) {
      runStage(event, stage);
    }

    return event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
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
      Map<Partition, Map<String, String>> bestPossibleState) {
    Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
    for (Partition partition : bestPossibleState.keySet()) {
      result.put(partition.getPartitionName(), bestPossibleState.get(partition));
    }
    return result;
  }

  @Override
  public boolean verify() throws Exception {
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    cache.refresh(_accessor);

    List<String> liveInstances = new ArrayList<String>();
    liveInstances.addAll(cache.getLiveInstances().keySet());
    boolean success = verifyLiveNodes(liveInstances);
    if (!success) {
      LOG.info("liveNodes not match, expect: " + _expectSortedLiveNodes + ", actual: "
          + liveInstances);
      return false;
    }

    BestPossibleStateOutput bestPossbileStates = calculateBestPossibleState(cache);
    Map<String, ExternalView> externalViews =
        _accessor.getChildValuesMap(_keyBuilder.externalViews());

    // TODO all ideal-states should be included in external-views

    for (String resourceName : externalViews.keySet()) {
      ExternalView externalView = externalViews.get(resourceName);
      Map<Partition, Map<String, String>> bestPossbileState =
          bestPossbileStates.getResourceMap(resourceName);
      success = verifyExternalView(externalView, bestPossbileState);
      if (!success) {
        LOG.info("external-view for resource: " + resourceName + " not match");
        return false;
      }
    }

    return true;
  }

}
