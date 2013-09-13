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

import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.FullAutoRebalancerConfig;
import org.apache.helix.api.Id;
import org.apache.helix.api.Participant;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.State;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.rebalancer.util.NewConstraintBasedAssignment;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.DefaultPlacementScheme;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.ReplicaPlacementScheme;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;

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
public class NewAutoRebalancer implements NewRebalancer<FullAutoRebalancerConfig> {
  // These should be final, but are initialized in init rather than a constructor
  private AutoRebalanceStrategy _algorithm;

  private static final Logger LOG = Logger.getLogger(NewAutoRebalancer.class);

  @Override
  public ResourceAssignment computeResourceMapping(FullAutoRebalancerConfig config,
      Cluster cluster, ResourceCurrentState currentState) {
    StateModelDefinition stateModelDef =
        cluster.getStateModelMap().get(config.getStateModelDefId());
    // Compute a preference list based on the current ideal state
    List<PartitionId> partitions = new ArrayList<PartitionId>(config.getPartitionSet());
    List<String> partitionNames = Lists.transform(partitions, Functions.toStringFunction());
    Map<ParticipantId, Participant> liveParticipants = cluster.getLiveParticipantMap();
    Map<ParticipantId, Participant> allParticipants = cluster.getParticipantMap();
    int replicas = -1;
    if (config.canAssignAnyLiveParticipant()) {
      replicas = liveParticipants.size();
    } else {
      replicas = config.getReplicaCount();
    }

    LinkedHashMap<String, Integer> stateCountMap =
        ConstraintBasedAssignment.stateCount(stateModelDef, liveParticipants.size(), replicas);
    List<ParticipantId> liveParticipantList =
        new ArrayList<ParticipantId>(liveParticipants.keySet());
    List<ParticipantId> allParticipantList =
        new ArrayList<ParticipantId>(cluster.getParticipantMap().keySet());
    List<String> liveNodes = Lists.transform(liveParticipantList, Functions.toStringFunction());
    Map<PartitionId, Map<ParticipantId, State>> currentMapping =
        currentMapping(config, currentState, stateCountMap);

    // If there are nodes tagged with resource, use only those nodes
    Set<String> taggedNodes = new HashSet<String>();
    if (config.getParticipantGroupTag() != null) {
      for (ParticipantId participantId : liveParticipantList) {
        if (liveParticipants.get(participantId).hasTag(config.getParticipantGroupTag())) {
          taggedNodes.add(participantId.stringify());
        }
      }
    }
    if (taggedNodes.size() > 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info("found the following instances with tag " + config.getResourceId() + " "
            + taggedNodes);
      }
      liveNodes = new ArrayList<String>(taggedNodes);
    }

    List<String> allNodes = Lists.transform(allParticipantList, Functions.toStringFunction());
    int maxPartition = config.getMaxPartitionsPerParticipant();

    if (LOG.isInfoEnabled()) {
      LOG.info("currentMapping: " + currentMapping);
      LOG.info("stateCountMap: " + stateCountMap);
      LOG.info("liveNodes: " + liveNodes);
      LOG.info("allNodes: " + allNodes);
      LOG.info("maxPartition: " + maxPartition);
    }
    ReplicaPlacementScheme placementScheme = new DefaultPlacementScheme();
    _algorithm =
        new AutoRebalanceStrategy(config.getResourceId().stringify(), partitionNames,
            stateCountMap, maxPartition, placementScheme);
    ZNRecord newMapping =
        _algorithm.computePartitionAssignment(liveNodes,
            ResourceAssignment.stringMapsFromReplicaMaps(currentMapping), allNodes);

    if (LOG.isInfoEnabled()) {
      LOG.info("newMapping: " + newMapping);
    }

    // compute a full partition mapping for the resource
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + config.getResourceId());
    }
    ResourceAssignment partitionMapping = new ResourceAssignment(config.getResourceId());
    for (PartitionId partition : partitions) {
      Set<ParticipantId> disabledParticipantsForPartition =
          NewConstraintBasedAssignment.getDisabledParticipants(allParticipants, partition);
      List<String> rawPreferenceList = newMapping.getListField(partition.stringify());
      if (rawPreferenceList == null) {
        rawPreferenceList = Collections.emptyList();
      }
      List<ParticipantId> preferenceList =
          Lists.transform(rawPreferenceList, new Function<String, ParticipantId>() {
            @Override
            public ParticipantId apply(String participantName) {
              return Id.participant(participantName);
            }
          });
      preferenceList =
          NewConstraintBasedAssignment.getPreferenceList(cluster, partition, preferenceList);
      Map<ParticipantId, State> bestStateForPartition =
          NewConstraintBasedAssignment.computeAutoBestStateForPartition(liveParticipants,
              stateModelDef, preferenceList,
              currentState.getCurrentStateMap(config.getResourceId(), partition),
              disabledParticipantsForPartition);
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  private Map<PartitionId, Map<ParticipantId, State>> currentMapping(
      FullAutoRebalancerConfig config, ResourceCurrentState currentStateOutput,
      Map<String, Integer> stateCountMap) {
    Map<PartitionId, Map<ParticipantId, State>> map =
        new HashMap<PartitionId, Map<ParticipantId, State>>();

    for (PartitionId partition : config.getPartitionSet()) {
      Map<ParticipantId, State> curStateMap =
          currentStateOutput.getCurrentStateMap(config.getResourceId(), partition);
      map.put(partition, new HashMap<ParticipantId, State>());
      for (ParticipantId node : curStateMap.keySet()) {
        State state = curStateMap.get(node);
        if (stateCountMap.containsKey(state)) {
          map.get(partition).put(node, state);
        }
      }

      Map<ParticipantId, State> pendingStateMap =
          currentStateOutput.getPendingStateMap(config.getResourceId(), partition);
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
