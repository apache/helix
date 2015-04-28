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

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.context.ControllerContextProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.FallbackRebalancer;
import org.apache.helix.controller.rebalancer.HelixRebalancer;
import org.apache.helix.controller.rebalancer.RebalancerRef;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * For partition compute best possible (instance,state) pair based on
 * IdealState,StateModel,LiveInstance
 */
public class BestPossibleStateCalcStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(BestPossibleStateCalcStage.class.getName());

  // cache for rebalancer instances
  private Map<ResourceId, HelixRebalancer> _rebalancerMap = Maps.newHashMap();

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
    Cluster cluster = event.getAttribute("Cluster");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (currentStateOutput == null || resourceMap == null || cluster == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|RESOURCES|Cluster");
    }

    BestPossibleStateOutput bestPossibleStateOutput =
        compute(cluster, event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossibleStateOutput);

    try {
      ClusterStatusMonitor clusterStatusMonitor =
          (ClusterStatusMonitor) event.getAttribute("clusterStatusMonitor");
      if (clusterStatusMonitor != null) {
        clusterStatusMonitor.setPerInstanceResourceStatus(bestPossibleStateOutput,
            cache.getInstanceConfigMap(), resourceMap, cache.getStateModelDefMap());
      }
    } catch (Exception e) {
      LOG.error("Could not update cluster status metrics!", e);
    }

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
    Set<PartitionId> mappedPartitions =
        currentStateOutput.getCurrentStateMappedPartitions(resourceId);
    if (mappedPartitions == null) {
      return partitionMapping;
    }
    for (PartitionId partitionId : mappedPartitions) {
      Set<ParticipantId> disabledParticipantsForPartition =
          ConstraintBasedAssignment.getDisabledParticipants(cluster.getParticipantMap(),
              partitionId);
      Map<State, String> upperBounds =
          ConstraintBasedAssignment
              .stateConstraints(stateModelDef, resourceId, cluster.getConfig());
      partitionMapping.addReplicaMap(partitionId, ConstraintBasedAssignment
          .computeAutoBestStateForPartition(upperBounds, cluster.getLiveParticipantMap().keySet(),
              stateModelDef, null, currentStateOutput.getCurrentStateMap(resourceId, partitionId),
              disabledParticipantsForPartition, true));
    }
    return partitionMapping;
  }

  /**
   * Update a ResourceAssignment with dropped and disabled participants for partitions
   * @param cluster cluster snapshot
   * @param resourceAssignment current resource assignment
   * @param currentStateOutput aggregated current state
   * @param stateModelDef state model definition for the resource
   */
  private void mapDroppedAndDisabledPartitions(Cluster cluster,
      ResourceAssignment resourceAssignment, ResourceCurrentState currentStateOutput,
      StateModelDefinition stateModelDef) {
    // get the total partition set: mapped and current state
    ResourceId resourceId = resourceAssignment.getResourceId();
    Set<PartitionId> mappedPartitions = Sets.newHashSet();
    mappedPartitions.addAll(currentStateOutput.getCurrentStateMappedPartitions(resourceId));
    mappedPartitions.addAll(resourceAssignment.getMappedPartitionIds());
    for (PartitionId partitionId : mappedPartitions) {
      // for each partition, get the dropped and disabled mappings
      Set<ParticipantId> disabledParticipants =
          ConstraintBasedAssignment.getDisabledParticipants(cluster.getParticipantMap(),
              partitionId);

      // get the error participants
      Map<ParticipantId, State> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceId, partitionId);
      Set<ParticipantId> errorParticipants = Sets.newHashSet();
      for (ParticipantId participantId : currentStateMap.keySet()) {
        State state = currentStateMap.get(participantId);
        if (State.from(HelixDefinedState.ERROR).equals(state)) {
          errorParticipants.add(participantId);
        }
      }

      // get the dropped and disabled map
      State initialState = stateModelDef.getTypedInitialState();
      Map<ParticipantId, State> participantStateMap = resourceAssignment.getReplicaMap(partitionId);
      Set<ParticipantId> participants = participantStateMap.keySet();
      Map<ParticipantId, State> droppedAndDisabledMap =
          ConstraintBasedAssignment.dropAndDisablePartitions(currentStateMap, participants,
              disabledParticipants, true, initialState);

      // don't map error participants
      for (ParticipantId participantId : errorParticipants) {
        droppedAndDisabledMap.remove(participantId);
      }
      // save the mappings, overwriting as necessary
      participantStateMap.putAll(droppedAndDisabledMap);

      // include this add step in case the resource assignment did not already map this partition
      resourceAssignment.addReplicaMap(partitionId, participantStateMap);
    }
  }

  private BestPossibleStateOutput compute(Cluster cluster, ClusterEvent event,
      Map<ResourceId, ResourceConfig> resourceMap, ResourceCurrentState currentStateOutput) {
    BestPossibleStateOutput output = new BestPossibleStateOutput();
    Map<StateModelDefId, StateModelDefinition> stateModelDefs = cluster.getStateModelMap();

    for (ResourceId resourceId : resourceMap.keySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing resource:" + resourceId);
      }
      ResourceConfig resourceConfig = resourceMap.get(resourceId);
      RebalancerConfig rebalancerConfig = resourceConfig.getRebalancerConfig();
      IdealState idealState = resourceConfig.getIdealState();
      StateModelDefinition stateModelDef = stateModelDefs.get(idealState.getStateModelDefId());
      ResourceAssignment resourceAssignment = null;
      // use a cached rebalancer if possible
      RebalancerRef ref = idealState.getRebalancerRef();
      HelixRebalancer rebalancer = null;
      if (_rebalancerMap.containsKey(resourceId)) {
        HelixRebalancer candidateRebalancer = _rebalancerMap.get(resourceId);
        if (ref != null && candidateRebalancer.getClass().equals(ref.toString())) {
          rebalancer = candidateRebalancer;
        }
      }

      // otherwise instantiate a new one
      if (rebalancer == null) {
        if (ref != null) {
          rebalancer = ref.getRebalancer();
        }
        HelixManager manager = event.getAttribute("helixmanager");
        ControllerContextProvider provider =
            event.getAttribute(AttributeName.CONTEXT_PROVIDER.toString());
        if (rebalancer == null) {
          rebalancer = new FallbackRebalancer();
        }
        rebalancer.init(manager, provider);
        _rebalancerMap.put(resourceId, rebalancer);
      }
      ResourceAssignment currentAssignment = null;
      try {
        resourceAssignment =
            rebalancer.computeResourceMapping(idealState, rebalancerConfig, currentAssignment,
                cluster, currentStateOutput);
      } catch (Exception e) {
        LOG.error("Rebalancer for resource " + resourceId + " failed.", e);
      }
      if (resourceAssignment == null) {
        resourceAssignment =
            mapDroppedResource(cluster, resourceId, currentStateOutput, stateModelDef);
      } else {
        mapDroppedAndDisabledPartitions(cluster, resourceAssignment, currentStateOutput,
            stateModelDef);
      }
      output.setResourceAssignment(resourceId, resourceAssignment);
    }

    return output;
  }
}
