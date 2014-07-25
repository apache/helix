package org.apache.helix.controller.rebalancer.util;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Collection of functions that will compute the best possible state based on the participants and
 * the rebalancer configuration of a resource.
 */
public class ConstraintBasedAssignment {
  private static Logger logger = Logger.getLogger(ConstraintBasedAssignment.class);

  /**
   * Get a set of disabled participants for a partition
   * @param participantMap map of all participants
   * @param partitionId the partition to check
   * @return a set of all participants that are disabled for the partition
   */
  public static Set<ParticipantId> getDisabledParticipants(
      final Map<ParticipantId, Participant> participantMap, final PartitionId partitionId) {
    Set<ParticipantId> participantSet = new HashSet<ParticipantId>(participantMap.keySet());
    Set<ParticipantId> disabledParticipantsForPartition =
        Sets.filter(participantSet, new Predicate<ParticipantId>() {
          @Override
          public boolean apply(ParticipantId participantId) {
            Participant participant = participantMap.get(participantId);
            return !participant.isEnabled()
                || participant.getDisabledPartitionIds().contains(partitionId);
          }
        });
    return disabledParticipantsForPartition;
  }

  /**
   * Get an ordered list of participants that can serve a partition
   * @param cluster cluster snapshot
   * @param partitionId the partition to look up
   * @param config rebalancing constraints
   * @return list with most preferred participants first
   */
  public static List<ParticipantId> getPreferenceList(Cluster cluster, PartitionId partitionId,
      List<ParticipantId> prefList) {
    if (prefList != null && prefList.size() == 1
        && StateModelToken.ANY_LIVEINSTANCE.toString().equals(prefList.get(0).stringify())) {
      prefList = new ArrayList<ParticipantId>(cluster.getLiveParticipantMap().keySet());
      Collections.sort(prefList);
    }
    return prefList;
  }

  /**
   * Get a map of state to upper bound constraint given a cluster
   * @param stateModelDef the state model definition to check
   * @param resourceId the resource that is constraint
   * @param cluster the cluster the resource belongs to
   * @return map of state to upper bound
   */
  public static Map<State, String> stateConstraints(StateModelDefinition stateModelDef,
      ResourceId resourceId, ClusterConfig cluster) {
    Map<State, String> stateMap = Maps.newHashMap();
    for (State state : stateModelDef.getTypedStatesPriorityList()) {
      String num = stateModelDef.getNumParticipantsPerState(state);
      stateMap.put(state, num);
    }
    return stateMap;
  }

  /**
   * Get a mapping for a partition for the current state participants who have been dropped or
   * disabled for a given partition.
   * @param currentStateMap current map of participant id to state for a partition
   * @param participants participants selected to serve the partition
   * @param disabledParticipants participants that have been disabled for this partition
   * @param initialState the initial state of the resource state model
   * @param isEnabled true if resource is enabled, false otherwise
   * @return map of participant id to state of dropped and disabled partitions
   */
  public static Map<ParticipantId, State> dropAndDisablePartitions(
      Map<ParticipantId, State> currentStateMap, Collection<ParticipantId> participants,
      Set<ParticipantId> disabledParticipants, boolean isEnabled, State initialState) {
    Map<ParticipantId, State> participantStateMap = new HashMap<ParticipantId, State>();
    // if the resource is deleted, instancePreferenceList will be empty and
    // we should drop all resources.
    if (currentStateMap != null) {
      for (ParticipantId participantId : currentStateMap.keySet()) {
        if ((participants == null || !participants.contains(participantId))
            && !disabledParticipants.contains(participantId) && isEnabled) {
          // if dropped and not disabled, transit to DROPPED
          participantStateMap.put(participantId, State.from(HelixDefinedState.DROPPED));
        } else if ((currentStateMap.get(participantId) == null || !currentStateMap.get(
            participantId).equals(State.from(HelixDefinedState.ERROR)))
            && (disabledParticipants.contains(participantId) || !isEnabled)) {
          // if disabled and not in ERROR state, transit to initial-state (e.g. OFFLINE)
          participantStateMap.put(participantId, initialState);
        }
      }
    }
    return participantStateMap;
  }

  /**
   * compute best state for resource in SEMI_AUTO and FULL_AUTO modes
   * @param upperBounds map of state to upper bound
   * @param liveParticipantSet set of live participant ids
   * @param stateModelDef
   * @param participantPreferenceList
   * @param currentStateMap
   *          : participant->state for each partition
   * @param disabledParticipantsForPartition
   * @param isEnabled true if resource is enabled, false if disabled
   * @return
   */
  public static Map<ParticipantId, State> computeAutoBestStateForPartition(
      Map<State, String> upperBounds, Set<ParticipantId> liveParticipantSet,
      StateModelDefinition stateModelDef, List<ParticipantId> participantPreferenceList,
      Map<ParticipantId, State> currentStateMap,
      Set<ParticipantId> disabledParticipantsForPartition, boolean isEnabled) {
    // drop and disable participants if necessary
    Map<ParticipantId, State> participantStateMap =
        dropAndDisablePartitions(currentStateMap, participantPreferenceList,
            disabledParticipantsForPartition, isEnabled, stateModelDef.getTypedInitialState());

    // resource is deleted
    if (participantPreferenceList == null) {
      return participantStateMap;
    }

    List<State> statesPriorityList = stateModelDef.getTypedStatesPriorityList();
    boolean assigned[] = new boolean[participantPreferenceList.size()];

    for (State state : statesPriorityList) {
      String num = upperBounds.get(state);
      int stateCount = -1;
      if ("N".equals(num)) {
        Set<ParticipantId> liveAndEnabled = new HashSet<ParticipantId>(liveParticipantSet);
        liveAndEnabled.removeAll(disabledParticipantsForPartition);
        stateCount = isEnabled ? liveAndEnabled.size() : 0;
      } else if ("R".equals(num)) {
        stateCount = participantPreferenceList.size();
      } else {
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
          logger.error("Invalid count for state:" + state + " ,count=" + num);
        }
      }
      if (stateCount > -1) {
        int count = 0;
        for (int i = 0; i < participantPreferenceList.size(); i++) {
          ParticipantId participantId = participantPreferenceList.get(i);

          boolean notInErrorState =
              currentStateMap == null
                  || currentStateMap.get(participantId) == null
                  || !currentStateMap.get(participantId)
                      .equals(State.from(HelixDefinedState.ERROR));

          if (liveParticipantSet.contains(participantId) && !assigned[i] && notInErrorState
              && !disabledParticipantsForPartition.contains(participantId) && isEnabled) {
            participantStateMap.put(participantId, state);
            count = count + 1;
            assigned[i] = true;
            if (count == stateCount) {
              break;
            }
          }
        }
      }
    }
    return participantStateMap;
  }

  /**
   * Get the number of replicas that should be in each state for a partition
   * @param upperBounds map of state to upper bound
   * @param stateModelDef StateModelDefinition object
   * @param liveNodesNb number of live nodes
   * @param total number of replicas
   * @return state count map: state->count
   */
  public static LinkedHashMap<State, Integer> stateCount(Map<State, String> upperBounds,
      StateModelDefinition stateModelDef, int liveNodesNb, int totalReplicas) {
    LinkedHashMap<State, Integer> stateCountMap = new LinkedHashMap<State, Integer>();
    List<State> statesPriorityList = stateModelDef.getTypedStatesPriorityList();

    int replicas = totalReplicas;
    for (State state : statesPriorityList) {
      String num = upperBounds.get(state);
      if ("N".equals(num)) {
        stateCountMap.put(state, liveNodesNb);
      } else if ("R".equals(num)) {
        // wait until we get the counts for all other states
        continue;
      } else {
        int stateCount = -1;
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
          // LOG.error("Invalid count for state: " + state + ", count: " + num +
          // ", use -1 instead");
        }

        if (stateCount > 0) {
          stateCountMap.put(state, stateCount);
          replicas -= stateCount;
        }
      }
    }

    // get state count for R
    for (State state : statesPriorityList) {
      String num = upperBounds.get(state);
      if ("R".equals(num)) {
        stateCountMap.put(state, replicas);
        // should have at most one state using R
        break;
      }
    }
    return stateCountMap;
  }

  /**
   * compute best state for resource in CUSTOMIZED rebalancer mode
   * @param liveParticipantMap
   * @param stateModelDef
   * @param preferenceMap
   * @param currentStateMap
   * @param disabledParticipantsForPartition
   * @param isEnabled
   * @return
   */
  public static Map<ParticipantId, State> computeCustomizedBestStateForPartition(
      Set<ParticipantId> liveParticipantSet, StateModelDefinition stateModelDef,
      Map<ParticipantId, State> preferenceMap, Map<ParticipantId, State> currentStateMap,
      Set<ParticipantId> disabledParticipantsForPartition, boolean isEnabled) {
    Map<ParticipantId, State> participantStateMap = new HashMap<ParticipantId, State>();

    // if the resource is deleted, idealStateMap will be null/empty and
    // we should drop all resources.
    if (currentStateMap != null) {
      for (ParticipantId participantId : currentStateMap.keySet()) {
        if ((preferenceMap == null || !preferenceMap.containsKey(participantId))
            && !disabledParticipantsForPartition.contains(participantId) && isEnabled) {
          // if dropped and not disabled, transit to DROPPED
          participantStateMap.put(participantId, State.from(HelixDefinedState.DROPPED));
        } else if ((currentStateMap.get(participantId) == null || !currentStateMap.get(
            participantId).equals(State.from(HelixDefinedState.ERROR)))
            && (disabledParticipantsForPartition.contains(participantId) || !isEnabled)) {
          // if disabled and not in ERROR state, transit to initial-state (e.g. OFFLINE)
          participantStateMap.put(participantId, stateModelDef.getTypedInitialState());
        }
      }
    }

    // ideal state is deleted
    if (preferenceMap == null) {
      return participantStateMap;
    }

    for (ParticipantId participantId : preferenceMap.keySet()) {
      boolean notInErrorState =
          currentStateMap == null || currentStateMap.get(participantId) == null
              || !currentStateMap.get(participantId).equals(State.from(HelixDefinedState.ERROR));

      if (liveParticipantSet.contains(participantId) && notInErrorState
          && !disabledParticipantsForPartition.contains(participantId) && isEnabled) {
        participantStateMap.put(participantId, preferenceMap.get(participantId));
      }
    }
    return participantStateMap;
  }
}
