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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.Cluster;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.RebalancerConfig;
import org.apache.helix.api.Resource;
import org.apache.helix.api.State;
import org.apache.helix.controller.rebalancer.util.NewConstraintBasedAssignment;
import org.apache.helix.controller.stages.NewCurrentStateOutput;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * This is a Rebalancer specific to semi-automatic mode. It is tasked with computing the ideal
 * state of a resource based on a predefined preference list of instances willing to accept
 * replicas.
 * The input is the optional current assignment of partitions to instances, as well as the required
 * existing instance preferences.
 * The output is a mapping based on that preference list, i.e. partition p has a replica on node k
 * with state s.
 */
public class NewSemiAutoRebalancer implements NewRebalancer {

  private static final Logger LOG = Logger.getLogger(NewSemiAutoRebalancer.class);

  @Override
  public ResourceAssignment computeResourceMapping(Resource resource, Cluster cluster,
      StateModelDefinition stateModelDef, NewCurrentStateOutput currentStateOutput) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + resource.getId());
    }
    ResourceAssignment partitionMapping = new ResourceAssignment(resource.getId());
    RebalancerConfig config = resource.getRebalancerConfig();
    for (PartitionId partition : resource.getPartitionSet()) {
      Map<ParticipantId, State> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource.getId(), partition);
      Set<ParticipantId> disabledInstancesForPartition =
          NewConstraintBasedAssignment.getDisabledParticipants(cluster.getParticipantMap(),
              partition);
      List<ParticipantId> preferenceList =
          NewConstraintBasedAssignment.getPreferenceList(cluster, partition,
              config.getPreferenceList(partition));
      Map<ParticipantId, State> bestStateForPartition =
          NewConstraintBasedAssignment.computeAutoBestStateForPartition(
              cluster.getLiveParticipantMap(), stateModelDef, preferenceList, currentStateMap,
              disabledInstancesForPartition);
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }
}
