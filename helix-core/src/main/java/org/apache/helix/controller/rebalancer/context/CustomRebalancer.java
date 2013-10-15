package org.apache.helix.controller.rebalancer.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.controller.rebalancer.util.NewConstraintBasedAssignment;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

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

public class CustomRebalancer implements Rebalancer {

  private static final Logger LOG = Logger.getLogger(CustomRebalancer.class);

  @Override
  public void init(HelixManager helixManager) {
    // do nothing
  }

  @Override
  public ResourceAssignment computeResourceMapping(RebalancerConfig rebalancerConfig,
      Cluster cluster, ResourceCurrentState currentState) {
    CustomRebalancerContext config =
        rebalancerConfig.getRebalancerContext(CustomRebalancerContext.class);
    StateModelDefinition stateModelDef =
        cluster.getStateModelMap().get(config.getStateModelDefId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + config.getResourceId());
    }
    ResourceAssignment partitionMapping = new ResourceAssignment(config.getResourceId());
    for (PartitionId partition : config.getPartitionSet()) {
      Map<ParticipantId, State> currentStateMap =
          currentState.getCurrentStateMap(config.getResourceId(), partition);
      Set<ParticipantId> disabledInstancesForPartition =
          NewConstraintBasedAssignment.getDisabledParticipants(cluster.getParticipantMap(),
              partition);
      Map<ParticipantId, State> bestStateForPartition =
          computeCustomizedBestStateForPartition(cluster.getLiveParticipantMap().keySet(),
              stateModelDef, config.getPreferenceMap(partition), currentStateMap,
              disabledInstancesForPartition);
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  /**
   * compute best state for resource in CUSTOMIZED rebalancer mode
   * @param liveParticipantMap
   * @param stateModelDef
   * @param preferenceMap
   * @param currentStateMap
   * @param disabledParticipantsForPartition
   * @return
   */
  private Map<ParticipantId, State> computeCustomizedBestStateForPartition(
      Set<ParticipantId> liveParticipantSet, StateModelDefinition stateModelDef,
      Map<ParticipantId, State> preferenceMap, Map<ParticipantId, State> currentStateMap,
      Set<ParticipantId> disabledParticipantsForPartition) {
    Map<ParticipantId, State> participantStateMap = new HashMap<ParticipantId, State>();

    // if the resource is deleted, idealStateMap will be null/empty and
    // we should drop all resources.
    if (currentStateMap != null) {
      for (ParticipantId participantId : currentStateMap.keySet()) {
        if ((preferenceMap == null || !preferenceMap.containsKey(participantId))
            && !disabledParticipantsForPartition.contains(participantId)) {
          // if dropped and not disabled, transit to DROPPED
          participantStateMap.put(participantId, State.from(HelixDefinedState.DROPPED));
        } else if ((currentStateMap.get(participantId) == null || !currentStateMap.get(
            participantId).equals(State.from(HelixDefinedState.ERROR)))
            && disabledParticipantsForPartition.contains(participantId)) {
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
          && !disabledParticipantsForPartition.contains(participantId)) {
        participantStateMap.put(participantId, preferenceMap.get(participantId));
      }
    }

    return participantStateMap;
  }
}
