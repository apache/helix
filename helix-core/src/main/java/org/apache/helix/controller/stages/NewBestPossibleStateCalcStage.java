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

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.context.Rebalancer;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;
import org.apache.helix.controller.rebalancer.util.NewConstraintBasedAssignment;
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

    ResourceCurrentState currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
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
      ResourceCurrentState currentStateOutput, StateModelDefinition stateModelDef) {
    ResourceAssignment partitionMapping = new ResourceAssignment(resourceId);
    Set<? extends PartitionId> mappedPartitions =
        currentStateOutput.getCurrentStateMappedPartitions(resourceId);
    if (mappedPartitions == null) {
      return partitionMapping;
    }
    for (PartitionId partitionId : mappedPartitions) {
      Set<ParticipantId> disabledParticipantsForPartition =
          NewConstraintBasedAssignment.getDisabledParticipants(cluster.getParticipantMap(),
              partitionId);
      Map<State, String> upperBounds =
          NewConstraintBasedAssignment.stateConstraints(stateModelDef, resourceId,
              cluster.getConfig());
      partitionMapping.addReplicaMap(partitionId, NewConstraintBasedAssignment
          .computeAutoBestStateForPartition(upperBounds, cluster.getLiveParticipantMap().keySet(),
              stateModelDef, null, currentStateOutput.getCurrentStateMap(resourceId, partitionId),
              disabledParticipantsForPartition));
    }
    return partitionMapping;
  }

  private NewBestPossibleStateOutput compute(Cluster cluster, ClusterEvent event,
      Map<ResourceId, ResourceConfig> resourceMap, ResourceCurrentState currentStateOutput) {
    NewBestPossibleStateOutput output = new NewBestPossibleStateOutput();
    Map<StateModelDefId, StateModelDefinition> stateModelDefs = cluster.getStateModelMap();

    for (ResourceId resourceId : resourceMap.keySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing resource:" + resourceId);
      }
      ResourceConfig resourceConfig = resourceMap.get(resourceId);
      RebalancerConfig rebalancerConfig = resourceConfig.getRebalancerConfig();
      ResourceAssignment resourceAssignment = null;
      if (rebalancerConfig != null) {
        Rebalancer rebalancer = rebalancerConfig.getRebalancer();
        if (rebalancer != null) {
          HelixManager manager = event.getAttribute("helixmanager");
          rebalancer.init(manager);
          resourceAssignment =
              rebalancer.computeResourceMapping(rebalancerConfig, cluster, currentStateOutput);
        }
      }
      if (resourceAssignment == null) {
        RebalancerContext context = rebalancerConfig.getRebalancerContext(RebalancerContext.class);
        StateModelDefinition stateModelDef = stateModelDefs.get(context.getStateModelDefId());
        resourceAssignment =
            mapDroppedResource(cluster, resourceId, currentStateOutput, stateModelDef);
      }

      output.setResourceAssignment(resourceId, resourceAssignment);
    }

    return output;
  }
}
