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

import org.apache.helix.HelixManager;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.ResourceId;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
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
public class SemiAutoRebalancer implements Rebalancer {

  private static final Logger LOG = Logger.getLogger(SemiAutoRebalancer.class);

  @Override
  public void init(HelixManager manager) {
  }

  @Override
  public ResourceAssignment computeResourceMapping(Resource resource, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    String stateModelDefName = currentIdealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = clusterData.getStateModelDef(stateModelDefName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + resource.getResourceName());
    }
    ResourceAssignment partitionMapping =
        new ResourceAssignment(ResourceId.from(resource.getResourceName()));
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
      Set<String> disabledInstancesForPartition =
          clusterData.getDisabledInstancesForPartition(partition.toString());
      List<String> preferenceList =
          ConstraintBasedAssignment.getPreferenceList(clusterData, partition, currentIdealState,
              stateModelDef);
      Map<String, String> bestStateForPartition =
          ConstraintBasedAssignment.computeAutoBestStateForPartition(clusterData, stateModelDef,
              preferenceList, currentStateMap, disabledInstancesForPartition);
      partitionMapping.addReplicaMap(PartitionId.from(partition.getPartitionName()),
          ResourceAssignment.replicaMapFromStringMap(bestStateForPartition));
    }
    return partitionMapping;
  }
}
