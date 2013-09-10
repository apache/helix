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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Partition;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.RebalancerConfig;
import org.apache.helix.api.Resource;
import org.apache.helix.api.ResourceConfig;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.StateModelFactoryId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.CurrentState;
import org.apache.log4j.Logger;

/**
 * This stage computes all the resources in a cluster. The resources are
 * computed from IdealStates -> this gives all the resources currently active
 * CurrentState for liveInstance-> Helps in finding resources that are inactive
 * and needs to be dropped
 */
public class NewResourceComputationStage extends AbstractBaseStage {
  private static Logger LOG = Logger.getLogger(NewResourceComputationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    Cluster cluster = event.getAttribute("ClusterDataCache");
    if (cluster == null) {
      throw new StageException("Missing attributes in event:" + event + ". Requires Cluster");
    }

    Map<ResourceId, ResourceConfig.Builder> resourceBuilderMap =
        new LinkedHashMap<ResourceId, ResourceConfig.Builder>();
    // include all resources in ideal-state
    for (ResourceId resourceId : cluster.getResourceMap().keySet()) {
      Resource resource = cluster.getResource(resourceId);
      RebalancerConfig rebalancerConfig = resource.getRebalancerConfig();

      ResourceConfig.Builder resourceBuilder = new ResourceConfig.Builder(resourceId);
      resourceBuilder.rebalancerConfig(rebalancerConfig);
      Set<Partition> partitionSet = new HashSet<Partition>(resource.getPartitionMap().values());
      resourceBuilder.addPartitions(partitionSet);
      resourceBuilder.bucketSize(resource.getBucketSize());
      resourceBuilder.batchMessageMode(resource.getBatchMessageMode());
      resourceBuilder.schedulerTaskConfig(resource.getSchedulerTaskConfig());
      resourceBuilderMap.put(resourceId, resourceBuilder);
    }

    // include all partitions from CurrentState as well since idealState might be removed
    for (Participant liveParticipant : cluster.getLiveParticipantMap().values()) {
      for (ResourceId resourceId : liveParticipant.getCurrentStateMap().keySet()) {
        CurrentState currentState = liveParticipant.getCurrentStateMap().get(resourceId);

        if (currentState.getStateModelDefRef() == null) {
          LOG.error("state model def is null." + "resource:" + currentState.getResourceId()
              + ", partitions: " + currentState.getPartitionStateStringMap().keySet()
              + ", states: " + currentState.getPartitionStateStringMap().values());
          throw new StageException("State model def is null for resource:"
              + currentState.getResourceId());
        }

        // don't overwrite ideal state configs
        if (!resourceBuilderMap.containsKey(resourceId)) {
          RebalancerConfig.Builder rebalancerConfigBuilder =
              new RebalancerConfig.Builder(resourceId);
          rebalancerConfigBuilder.stateModelDef(currentState.getStateModelDefId());
          rebalancerConfigBuilder.stateModelFactoryId(new StateModelFactoryId(currentState
              .getStateModelFactoryName()));
          rebalancerConfigBuilder.bucketSize(currentState.getBucketSize());
          rebalancerConfigBuilder.batchMessageMode(currentState.getBatchMessageMode());

          ResourceConfig.Builder resourceBuilder = new ResourceConfig.Builder(resourceId);
          resourceBuilder.rebalancerConfig(rebalancerConfigBuilder.build());
          resourceBuilderMap.put(resourceId, resourceBuilder);
        }

        for (PartitionId partitionId : currentState.getPartitionStateMap().keySet()) {
          resourceBuilderMap.get(resourceId).addPartition(new Partition(partitionId));
        }
      }
    }

    // convert builder-map to resource-map
    Map<ResourceId, ResourceConfig> resourceMap = new LinkedHashMap<ResourceId, ResourceConfig>();
    for (ResourceId resourceId : resourceBuilderMap.keySet()) {
      resourceMap.put(resourceId, resourceBuilderMap.get(resourceId).build());
    }

    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
  }
}
