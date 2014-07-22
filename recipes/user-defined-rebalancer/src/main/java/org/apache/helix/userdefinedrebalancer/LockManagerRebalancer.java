package org.apache.helix.userdefinedrebalancer;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.context.ControllerContextProvider;
import org.apache.helix.controller.rebalancer.HelixRebalancer;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

public class LockManagerRebalancer implements HelixRebalancer {
  private static final Logger LOG = Logger.getLogger(LockManagerRebalancer.class);

  @Override
  public void init(HelixManager manager, ControllerContextProvider contextProvider) {
    // do nothing; this rebalancer is independent of the manager
  }

  /**
   * This rebalancer is invoked whenever there is a change in the cluster, including when new
   * participants join or leave, or the configuration of any participant changes. It is written
   * specifically to handle assignment of locks to nodes under the very simple lock-unlock state
   * model.
   */
  @Override
  public ResourceAssignment computeResourceMapping(IdealState idealState,
      RebalancerConfig rebalancerConfig, ResourceAssignment prevAssignment, Cluster cluster,
      ResourceCurrentState currentState) {
    // Initialize an empty mapping of locks to participants
    ResourceAssignment assignment = new ResourceAssignment(idealState.getResourceId());

    // Get the list of live participants in the cluster
    List<ParticipantId> liveParticipants =
        new ArrayList<ParticipantId>(cluster.getLiveParticipantMap().keySet());

    // Get the state model (should be a simple lock/unlock model) and the highest-priority state
    StateModelDefId stateModelDefId = idealState.getStateModelDefId();
    StateModelDefinition stateModelDef = cluster.getStateModelMap().get(stateModelDefId);
    if (stateModelDef.getStatesPriorityList().size() < 1) {
      LOG.error("Invalid state model definition. There should be at least one state.");
      return assignment;
    }
    State lockState = stateModelDef.getTypedStatesPriorityList().get(0);

    // Count the number of participants allowed to lock each lock
    String stateCount = stateModelDef.getNumParticipantsPerState(lockState);
    int lockHolders = 0;
    try {
      // a numeric value is a custom-specified number of participants allowed to lock the lock
      lockHolders = Integer.parseInt(stateCount);
    } catch (NumberFormatException e) {
      LOG.error("Invalid state model definition. The lock state does not have a valid count");
      return assignment;
    }

    // Fairly assign the lock state to the participants using a simple mod-based sequential
    // assignment. For instance, if each lock can be held by 3 participants, lock 0 would be held
    // by participants (0, 1, 2), lock 1 would be held by (1, 2, 3), and so on, wrapping around the
    // number of participants as necessary.
    // This assumes a simple lock-unlock model where the only state of interest is which nodes have
    // acquired each lock.
    int i = 0;
    for (PartitionId partition : idealState.getPartitionIdSet()) {
      Map<ParticipantId, State> replicaMap = new HashMap<ParticipantId, State>();
      for (int j = i; j < i + lockHolders; j++) {
        int participantIndex = j % liveParticipants.size();
        ParticipantId participant = liveParticipants.get(participantIndex);
        // enforce that a participant can only have one instance of a given lock
        if (!replicaMap.containsKey(participant)) {
          replicaMap.put(participant, lockState);
        }
      }
      assignment.addReplicaMap(partition, replicaMap);
      i++;
    }
    return assignment;
  }
}
