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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Resource;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.log4j.Logger;

/**
 * This stage computes all the resources in a cluster. The resources are
 * computed from IdealStates -> this gives all the resources currently active
 * CurrentState for liveInstance-> Helps in finding resources that are inactive
 * and needs to be dropped
 */
public class ResourceComputationStage extends AbstractBaseStage {
  private static Logger LOG = Logger.getLogger(ResourceComputationStage.class);

  @Override
  public void process(ClusterEvent event) throws StageException {
    Cluster cluster = event.getAttribute("Cluster");
    if (cluster == null) {
      throw new StageException("Missing attributes in event: " + event + ". Requires Cluster");
    }

    Map<ResourceId, ResourceConfig> resCfgMap = new HashMap<ResourceId, ResourceConfig>();
    Map<ResourceId, ResourceConfig> csResCfgMap = getCurStateResourceCfgMap(cluster);

    // ideal-state may be removed, add all resource config in current-state but not in ideal-state
    for (ResourceId resourceId : csResCfgMap.keySet()) {
      if (!cluster.getResourceMap().keySet().contains(resourceId)) {
        resCfgMap.put(resourceId, csResCfgMap.get(resourceId));
      }
    }

    for (ResourceId resourceId : cluster.getResourceMap().keySet()) {
      Resource resource = cluster.getResource(resourceId);
      RebalancerConfig rebalancerCfg = resource.getRebalancerConfig();

      ResourceConfig.Builder resCfgBuilder = new ResourceConfig.Builder(resourceId);
      resCfgBuilder.schedulerTaskConfig(resource.getSchedulerTaskConfig());
      resCfgBuilder.rebalancerConfig(rebalancerCfg);
      resCfgBuilder.provisionerConfig(resource.getProvisionerConfig());
      resCfgBuilder.idealState(resource.getIdealState());
      resCfgMap.put(resourceId, resCfgBuilder.build());
    }

    event.addAttribute(AttributeName.RESOURCES.toString(), resCfgMap);
  }

  /**
   * Get resource config's from current-state
   * @param cluster
   * @return resource config map or empty map if not available
   * @throws StageException
   */
  Map<ResourceId, ResourceConfig> getCurStateResourceCfgMap(Cluster cluster) throws StageException {
    Map<ResourceId, IdealState> idealStateMap = new HashMap<ResourceId, IdealState>();

    for (Participant liveParticipant : cluster.getLiveParticipantMap().values()) {
      for (ResourceId resourceId : liveParticipant.getCurrentStateMap().keySet()) {
        CurrentState currentState = liveParticipant.getCurrentStateMap().get(resourceId);
        Map<String, String> resourceStateMap = currentState.getPartitionStateMap();
        if (resourceStateMap.isEmpty()) {
          // skip empty current state for dropped resource
          continue;
        }

        if (currentState.getStateModelDefRef() == null) {
          LOG.error("state model def is null." + "resource:" + currentState.getResourceId()
              + ", partitions: " + currentState.getPartitionStateMap().keySet() + ", states: "
              + currentState.getPartitionStateMap().values());
          throw new StageException("State model def is null for resource:"
              + currentState.getResourceId());
        }

        if (!idealStateMap.containsKey(resourceId)) {
          IdealState idealState = new IdealState(resourceId);
          idealState.setStateModelDefId(currentState.getStateModelDefId());
          idealState.setStateModelFactoryName(currentState.getStateModelFactoryName());
          idealState.setBucketSize(currentState.getBucketSize());
          idealState.setBatchMessageMode(currentState.getBatchMessageMode());
          idealStateMap.put(resourceId, idealState);
        }

        IdealState idealState = idealStateMap.get(resourceId);
        for (PartitionId partitionId : currentState.getTypedPartitionStateMap().keySet()) {
          idealState.setParticipantStateMap(partitionId, new HashMap<ParticipantId, State>());
          idealState.setPreferenceList(partitionId, new ArrayList<ParticipantId>());
        }
      }
    }

    Map<ResourceId, ResourceConfig> resCfgMap = new HashMap<ResourceId, ResourceConfig>();
    for (ResourceId resourceId : idealStateMap.keySet()) {
      ResourceConfig.Builder resCfgBuilder = new ResourceConfig.Builder(resourceId);
      IdealState idealState = idealStateMap.get(resourceId);
      resCfgBuilder.idealState(idealState);
      resCfgMap.put(resourceId, resCfgBuilder.build());
    }

    return resCfgMap;
  }
}
