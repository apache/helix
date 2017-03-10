package org.apache.helix.controller.rebalancer;

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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * This is a Rebalancer specific to full automatic mode. It is tasked with computing the ideal
 * state of a resource, fully adapting to the addition or removal of instances. This includes
 * computation of a new preference list and a partition to instance and state mapping based on the
 * computed instance preferences.
 * The input is the current assignment of partitions to instances, as well as existing instance
 * preferences, if any.
 * The output is a preference list and a mapping based on that preference list, i.e. partition p
 * has a replica on node k with state s.
 */
public class AutoRebalancer extends AbstractRebalancer {
  private static final Logger LOG = Logger.getLogger(AutoRebalancer.class);

  @Override public IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState, CurrentStateOutput currentStateOutput,
      ClusterDataCache clusterData) {
    List<String> partitions = new ArrayList<String>(currentIdealState.getPartitionSet());
    String stateModelName = currentIdealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = clusterData.getStateModelDef(stateModelName);
    Map<String, LiveInstance> liveInstance = clusterData.getLiveInstances();
    String replicas = currentIdealState.getReplicas();

    LinkedHashMap<String, Integer> stateCountMap = stateModelDef
        .getStateCountMap(liveInstance.size(), Integer.parseInt(replicas));
    List<String> liveNodes = new ArrayList<String>(liveInstance.keySet());
    List<String> allNodes = new ArrayList<String>(clusterData.getInstanceConfigMap().keySet());
    allNodes.removeAll(clusterData.getDisabledInstances());
    liveNodes.retainAll(allNodes);

    Map<String, Map<String, String>> currentMapping =
        currentMapping(currentStateOutput, resourceName, partitions, stateCountMap);

    // If there are nodes tagged with resource name, use only those nodes
    Set<String> taggedNodes = new HashSet<String>();
    Set<String> taggedLiveNodes = new HashSet<String>();
    if (currentIdealState.getInstanceGroupTag() != null) {
      for (String instanceName : allNodes) {
        if (clusterData.getInstanceConfigMap().get(instanceName)
            .containsTag(currentIdealState.getInstanceGroupTag())) {
          taggedNodes.add(instanceName);
          if (liveInstance.containsKey(instanceName)) {
            taggedLiveNodes.add(instanceName);
          }
        }
      }
      if (!taggedLiveNodes.isEmpty()) {
        // live nodes exist that have this tag
        if (LOG.isInfoEnabled()) {
          LOG.info("found the following participants with tag "
              + currentIdealState.getInstanceGroupTag() + " for " + resourceName + ": "
              + taggedLiveNodes);
        }
      } else if (taggedNodes.isEmpty()) {
        // no live nodes and no configured nodes have this tag
        LOG.warn("Resource " + resourceName + " has tag " + currentIdealState.getInstanceGroupTag()
            + " but no configured participants have this tag");
      } else {
        // configured nodes have this tag, but no live nodes have this tag
        LOG.warn("Resource " + resourceName + " has tag " + currentIdealState.getInstanceGroupTag()
            + " but no live participants have this tag");
      }
      allNodes = new ArrayList<String>(taggedNodes);
      liveNodes = new ArrayList<String>(taggedLiveNodes);
    }

    // sort node lists to ensure consistent preferred assignments
    Collections.sort(allNodes);
    Collections.sort(liveNodes);

    int maxPartition = currentIdealState.getMaxPartitionsPerInstance();
    _rebalanceStrategy =
        getRebalanceStrategy(currentIdealState.getRebalanceStrategy(), partitions, resourceName,
            stateCountMap, maxPartition);
    ZNRecord newMapping = _rebalanceStrategy
        .computePartitionAssignment(allNodes, liveNodes, currentMapping, clusterData);

    if (LOG.isDebugEnabled()) {
      LOG.debug("currentMapping: " + currentMapping);
      LOG.debug("stateCountMap: " + stateCountMap);
      LOG.debug("liveNodes: " + liveNodes);
      LOG.debug("allNodes: " + allNodes);
      LOG.debug("maxPartition: " + maxPartition);
      LOG.debug("newMapping: " + newMapping);
    }

    IdealState newIdealState = new IdealState(resourceName);
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    newIdealState.setRebalanceMode(RebalanceMode.FULL_AUTO);
    newIdealState.getRecord().setListFields(newMapping.getListFields());

    return newIdealState;
  }
}
