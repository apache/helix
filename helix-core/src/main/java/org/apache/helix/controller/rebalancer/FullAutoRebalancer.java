package org.apache.helix.controller.rebalancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.controller.context.ControllerContextProvider;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.DefaultPlacementScheme;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.ReplicaPlacementScheme;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

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

public class FullAutoRebalancer implements HelixRebalancer {
  // These should be final, but are initialized in init rather than a constructor
  private AutoRebalanceStrategy _algorithm;

  private static Logger LOG = Logger.getLogger(FullAutoRebalancer.class);

  @Override
  public void init(HelixManager helixManager, ControllerContextProvider contextProvider) {
    // do nothing
  }

  @Override
  public ResourceAssignment computeResourceMapping(IdealState idealState,
      RebalancerConfig rebalancerConfig, ResourceAssignment prevAssignment, Cluster cluster,
      ResourceCurrentState currentState) {
    boolean isEnabled = (idealState != null) ? idealState.isEnabled() : true;
    StateModelDefinition stateModelDef =
        cluster.getStateModelMap().get(idealState.getStateModelDefId());
    // Compute a preference list based on the current ideal state
    List<PartitionId> partitions = new ArrayList<PartitionId>(idealState.getPartitionIdSet());
    Map<ParticipantId, Participant> liveParticipants = cluster.getLiveParticipantMap();
    Map<ParticipantId, Participant> allParticipants = cluster.getParticipantMap();
    int replicas = -1;
    String replicaStr = idealState.getReplicas();
    if (replicaStr.equals(StateModelToken.ANY_LIVEINSTANCE.toString())) {
      replicas = liveParticipants.size();
    } else {
      replicas = Integer.valueOf(replicaStr);
    }

    // count how many replicas should be in each state
    Map<State, String> upperBounds =
        ConstraintBasedAssignment.stateConstraints(stateModelDef, idealState.getResourceId(),
            cluster.getConfig());
    LinkedHashMap<State, Integer> stateCountMap =
        ConstraintBasedAssignment.stateCount(upperBounds, stateModelDef, liveParticipants.size(),
            replicas);

    // get the participant lists
    List<ParticipantId> liveParticipantList =
        new ArrayList<ParticipantId>(liveParticipants.keySet());
    List<ParticipantId> allParticipantList =
        new ArrayList<ParticipantId>(cluster.getParticipantMap().keySet());

    // compute the current mapping from the current state
    Map<PartitionId, Map<ParticipantId, State>> currentMapping =
        currentMapping(idealState, currentState, stateCountMap);

    // If there are nodes tagged with resource, use only those nodes
    // If there are nodes tagged with resource name, use only those nodes
    Set<ParticipantId> taggedNodes = new HashSet<ParticipantId>();
    Set<ParticipantId> taggedLiveNodes = new HashSet<ParticipantId>();
    if (idealState.getInstanceGroupTag() != null) {
      for (ParticipantId participantId : allParticipantList) {
        if (cluster.getParticipantMap().get(participantId).hasTag(idealState.getInstanceGroupTag())) {
          taggedNodes.add(participantId);
          if (liveParticipants.containsKey(participantId)) {
            taggedLiveNodes.add(participantId);
          }
        }
      }
      if (!taggedLiveNodes.isEmpty()) {
        // live nodes exist that have this tag
        if (LOG.isDebugEnabled()) {
          LOG.debug("found the following participants with tag " + idealState.getInstanceGroupTag()
              + " for " + idealState.getResourceId() + ": " + taggedLiveNodes);
        }
      } else if (taggedNodes.isEmpty()) {
        // no live nodes and no configured nodes have this tag
        LOG.warn("Resource " + idealState.getResourceId() + " has tag "
            + idealState.getInstanceGroupTag() + " but no configured participants have this tag");
      } else {
        // configured nodes have this tag, but no live nodes have this tag
        LOG.warn("Resource " + idealState.getResourceId() + " has tag "
            + idealState.getInstanceGroupTag() + " but no live participants have this tag");
      }
      allParticipantList = new ArrayList<ParticipantId>(taggedNodes);
      liveParticipantList = new ArrayList<ParticipantId>(taggedLiveNodes);
    }

    // determine which nodes the replicas should live on
    int maxPartition = idealState.getMaxPartitionsPerInstance();
    if (LOG.isDebugEnabled()) {
      LOG.debug("currentMapping: " + currentMapping);
      LOG.debug("stateCountMap: " + stateCountMap);
      LOG.debug("liveNodes: " + liveParticipantList);
      LOG.debug("allNodes: " + allParticipantList);
      LOG.debug("maxPartition: " + maxPartition);
    }
    ReplicaPlacementScheme placementScheme = new DefaultPlacementScheme();
    _algorithm =
        new AutoRebalanceStrategy(idealState.getResourceId(), partitions, stateCountMap,
            maxPartition, placementScheme);
    ZNRecord newMapping =
        _algorithm.typedComputePartitionAssignment(liveParticipantList, currentMapping,
            allParticipantList);

    if (LOG.isInfoEnabled()) {
      LOG.info("newMapping: " + newMapping);
    }

    // compute a full partition mapping for the resource
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + idealState.getResourceId());
    }
    ResourceAssignment partitionMapping = new ResourceAssignment(idealState.getResourceId());
    for (PartitionId partition : partitions) {
      Set<ParticipantId> disabledParticipantsForPartition =
          ConstraintBasedAssignment.getDisabledParticipants(allParticipants, partition);
      List<String> rawPreferenceList = newMapping.getListField(partition.stringify());
      if (rawPreferenceList == null) {
        rawPreferenceList = Collections.emptyList();
      }
      List<ParticipantId> preferenceList =
          Lists.transform(rawPreferenceList, new Function<String, ParticipantId>() {
            @Override
            public ParticipantId apply(String participantName) {
              return ParticipantId.from(participantName);
            }
          });
      preferenceList =
          ConstraintBasedAssignment.getPreferenceList(cluster, partition, preferenceList);
      Map<ParticipantId, State> bestStateForPartition =
          ConstraintBasedAssignment.computeAutoBestStateForPartition(upperBounds,
              liveParticipants.keySet(), stateModelDef, preferenceList,
              currentState.getCurrentStateMap(idealState.getResourceId(), partition),
              disabledParticipantsForPartition, isEnabled);
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  private Map<PartitionId, Map<ParticipantId, State>> currentMapping(IdealState idealState,
      ResourceCurrentState currentStateOutput, Map<State, Integer> stateCountMap) {
    Map<PartitionId, Map<ParticipantId, State>> map =
        new HashMap<PartitionId, Map<ParticipantId, State>>();

    for (PartitionId partition : idealState.getPartitionIdSet()) {
      Map<ParticipantId, State> curStateMap =
          currentStateOutput.getCurrentStateMap(idealState.getResourceId(), partition);
      map.put(partition, new HashMap<ParticipantId, State>());
      for (ParticipantId node : curStateMap.keySet()) {
        State state = curStateMap.get(node);
        if (stateCountMap.containsKey(state)) {
          map.get(partition).put(node, state);
        }
      }

      Map<ParticipantId, State> pendingStateMap =
          currentStateOutput.getPendingStateMap(idealState.getResourceId(), partition);
      for (ParticipantId node : pendingStateMap.keySet()) {
        State state = pendingStateMap.get(node);
        if (stateCountMap.containsKey(state)) {
          map.get(partition).put(node, state);
        }
      }
    }
    return map;
  }
}
