package org.apache.helix.controller.rebalancer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.rebalancer.context.SemiAutoRebalancerContext;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
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

/**
 * Rebalancer for the SEMI_AUTO mode. It expects a RebalancerConfig that understands the preferred
 * locations of each partition replica
 */
public class SemiAutoRebalancer implements HelixRebalancer {
  private static final Logger LOG = Logger.getLogger(SemiAutoRebalancer.class);

  @Override
  public void init(HelixManager helixManager) {
    // do nothing
  }

  @Override
  public ResourceAssignment computeResourceMapping(RebalancerConfig rebalancerConfig,
      Cluster cluster, ResourceCurrentState currentState) {
    SemiAutoRebalancerContext config =
        rebalancerConfig.getRebalancerContext(SemiAutoRebalancerContext.class);
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
          ConstraintBasedAssignment.getDisabledParticipants(cluster.getParticipantMap(),
              partition);
      List<ParticipantId> preferenceList =
          ConstraintBasedAssignment.getPreferenceList(cluster, partition,
              config.getPreferenceList(partition));
      Map<State, String> upperBounds =
          ConstraintBasedAssignment.stateConstraints(stateModelDef, config.getResourceId(),
              cluster.getConfig());
      Map<ParticipantId, State> bestStateForPartition =
          ConstraintBasedAssignment.computeAutoBestStateForPartition(upperBounds, cluster
              .getLiveParticipantMap().keySet(), stateModelDef, preferenceList, currentStateMap,
              disabledInstancesForPartition);
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

}
