package org.apache.helix.controller.stages;

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

import java.util.Map;
import java.util.Set;

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Id;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.RebalancerConfig;
import org.apache.helix.api.Resource;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.NewAutoRebalancer;
import org.apache.helix.controller.rebalancer.NewCustomRebalancer;
import org.apache.helix.controller.rebalancer.NewRebalancer;
import org.apache.helix.controller.rebalancer.NewSemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.util.NewConstraintBasedAssignment;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * For partition compute best possible (instance,state) pair based on
 * IdealState,StateModel,LiveInstance
 */
public class NewBestPossibleStateCalcStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(NewBestPossibleStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("START BestPossibleStateCalcStage.process()");
    }

    NewCurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    Map<ResourceId, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());
    Cluster cluster = event.getAttribute("ClusterDataCache");

    if (currentStateOutput == null || resourceMap == null || cluster == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }

    NewBestPossibleStateOutput bestPossibleStateOutput =
        compute(cluster, event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossibleStateOutput);

    long endTime = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("END BestPossibleStateCalcStage.process(). took: " + (endTime - startTime) + " ms");
    }
  }

  /**
   * Fallback for cases when the resource has been dropped, but current state exists
   * @param cluster cluster snapshot
   * @param resourceId the resource for which to generate an assignment
   * @param currentStateOutput full snapshot of the current state
   * @param stateModelDef state model the resource follows
   * @return assignment for the dropped resource
   */
  private ResourceAssignment mapDroppedResource(Cluster cluster, ResourceId resourceId,
      NewCurrentStateOutput currentStateOutput, StateModelDefinition stateModelDef) {
    ResourceAssignment partitionMapping = new ResourceAssignment(resourceId);
    Set<PartitionId> mappedPartitions =
        currentStateOutput.getCurrentStateMappedPartitions(resourceId);
    if (mappedPartitions == null) {
      return partitionMapping;
    }
    for (PartitionId partitionId : mappedPartitions) {
      Set<ParticipantId> disabledParticipantsForPartition =
          NewConstraintBasedAssignment.getDisabledParticipants(cluster.getParticipantMap(),
              partitionId);
      partitionMapping.addReplicaMap(partitionId, NewConstraintBasedAssignment
          .computeAutoBestStateForPartition(cluster.getLiveParticipantMap(), stateModelDef, null,
              currentStateOutput.getCurrentStateMap(resourceId, partitionId),
              disabledParticipantsForPartition));
    }
    return partitionMapping;
  }

  // TODO check this
  private NewBestPossibleStateOutput compute(Cluster cluster, ClusterEvent event,
      Map<ResourceId, Resource> resourceMap, NewCurrentStateOutput currentStateOutput) {
    NewBestPossibleStateOutput output = new NewBestPossibleStateOutput();
    Map<StateModelDefId, StateModelDefinition> stateModelDefs =
        event.getAttribute(AttributeName.STATE_MODEL_DEFINITIONS.toString());

    for (ResourceId resourceId : resourceMap.keySet()) {
      LOG.debug("Processing resource:" + resourceId);
      // Resource may be gone. In that case we need to get the state model name
      // from the current state
      Resource existResource = cluster.getResource(resourceId);
      if (existResource == null) {
        // if resource is deleted, then we do not know which rebalancer to use
        // instead, just mark all partitions of the resource as dropped
        if (LOG.isInfoEnabled()) {
          LOG.info("resource:" + resourceId + " does not exist anymore");
        }
        StateModelDefinition stateModelDef =
            stateModelDefs.get(currentStateOutput.getResourceStateModelDef(resourceId));
        ResourceAssignment droppedAssignment =
            mapDroppedResource(cluster, resourceId, currentStateOutput, stateModelDef);
        output.setResourceAssignment(resourceId, droppedAssignment);
        continue;
      }

      RebalancerConfig rebalancerConfig = existResource.getRebalancerConfig();
      NewRebalancer rebalancer = null;
      if (rebalancerConfig.getRebalancerMode() == RebalanceMode.USER_DEFINED
          && rebalancerConfig.getRebalancerRef() != null) {
        rebalancer = rebalancerConfig.getRebalancerRef().getRebalancer();
      }
      if (rebalancer == null) {
        if (rebalancerConfig.getRebalancerMode() == RebalanceMode.FULL_AUTO) {
          rebalancer = new NewAutoRebalancer();
        } else if (rebalancerConfig.getRebalancerMode() == RebalanceMode.SEMI_AUTO) {
          rebalancer = new NewSemiAutoRebalancer();
        } else {
          rebalancer = new NewCustomRebalancer();
        }
      }

      StateModelDefinition stateModelDef =
          stateModelDefs.get(rebalancerConfig.getStateModelDefId());
      ResourceAssignment resourceAssignment =
          rebalancer.computeResourceMapping(existResource, cluster, stateModelDef,
              currentStateOutput);
      if (resourceAssignment == null) {
        resourceAssignment =
            mapDroppedResource(cluster, resourceId, currentStateOutput, stateModelDef);
      }

      output.setResourceAssignment(resourceId, resourceAssignment);
    }

    return output;
  }
}
