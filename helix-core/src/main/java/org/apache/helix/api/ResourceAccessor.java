package org.apache.helix.api;

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

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.context.CustomRebalancerContext;
import org.apache.helix.controller.rebalancer.context.PartitionedRebalancerContext;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;
import org.apache.helix.controller.rebalancer.context.SemiAutoRebalancerContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfiguration;

public class ResourceAccessor {
  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;

  public ResourceAccessor(HelixDataAccessor accessor) {
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
  }

  /**
   * Read a single snapshot of a resource
   * @param resourceId the resource id to read
   * @return Resource
   */
  public Resource readResource(ResourceId resourceId) {
    ResourceConfiguration config =
        _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealState(resourceId.stringify()));
    ExternalView externalView =
        _accessor.getProperty(_keyBuilder.externalView(resourceId.stringify()));
    ResourceAssignment resourceAssignment =
        _accessor.getProperty(_keyBuilder.resourceAssignment(resourceId.stringify()));
    return createResource(resourceId, config, idealState, externalView, resourceAssignment);
  }

  /**
   * save resource assignment
   * @param resourceId
   * @param resourceAssignment
   */
  public void setResourceAssignment(ResourceId resourceId, ResourceAssignment resourceAssignment) {
    _accessor.setProperty(_keyBuilder.resourceAssignment(resourceId.stringify()),
        resourceAssignment);
  }

  /**
   * save resource assignment
   * @param resourceId
   * @return resource assignment or null
   */
  public void getResourceAssignment(ResourceId resourceId) {
    _accessor.getProperty(_keyBuilder.resourceAssignment(resourceId.stringify()));
  }

  /**
   * Set a resource configuration, which may include user-defined configuration, as well as
   * rebalancer configuration
   * @param resourceId
   * @param configuration
   */
  public void setConfiguration(ResourceId resourceId, ResourceConfiguration configuration) {
    _accessor.setProperty(_keyBuilder.resourceConfig(resourceId.stringify()), configuration);
    // also set an ideal state if the resource supports it
    RebalancerConfig rebalancerConfig = new RebalancerConfig(configuration);
    IdealState idealState =
        rebalancerConfigToIdealState(rebalancerConfig, configuration.getBucketSize(),
            configuration.getBatchMessageMode());
    if (idealState != null) {
      _accessor.setProperty(_keyBuilder.idealState(resourceId.stringify()), idealState);
    }
  }

  /**
   * Get a resource configuration, which may include user-defined configuration, as well as
   * rebalancer configuration
   * @param resourceId
   * @return configuration
   */
  public void getConfiguration(ResourceId resourceId) {
    _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
  }

  /**
   * set external view of a resource
   * @param resourceId
   * @param extView
   */
  public void setExternalView(ResourceId resourceId, ExternalView extView) {
    _accessor.setProperty(_keyBuilder.externalView(resourceId.stringify()), extView);
  }

  /**
   * drop external view of a resource
   * @param resourceId
   */
  public void dropExternalView(ResourceId resourceId) {
    _accessor.removeProperty(_keyBuilder.externalView(resourceId.stringify()));
  }

  /**
   * Get an ideal state from a rebalancer config if the resource is partitioned
   * @param config RebalancerConfig instance
   * @param bucketSize bucket size to use
   * @param batchMessageMode true if batch messaging allowed, false otherwise
   * @return IdealState, or null
   */
  static IdealState rebalancerConfigToIdealState(RebalancerConfig config, int bucketSize,
      boolean batchMessageMode) {
    PartitionedRebalancerContext partitionedContext =
        config.getRebalancerContext(PartitionedRebalancerContext.class);
    if (partitionedContext != null) {
      IdealState idealState = new IdealState(partitionedContext.getResourceId());
      idealState.setRebalanceMode(partitionedContext.getRebalanceMode());
      idealState.setRebalancerRef(partitionedContext.getRebalancerRef());
      String replicas = null;
      if (partitionedContext.anyLiveParticipant()) {
        replicas = StateModelToken.ANY_LIVEINSTANCE.toString();
      } else {
        replicas = Integer.toString(partitionedContext.getReplicaCount());
      }
      idealState.setReplicas(replicas);
      idealState.setNumPartitions(partitionedContext.getPartitionSet().size());
      idealState.setInstanceGroupTag(partitionedContext.getParticipantGroupTag());
      idealState.setMaxPartitionsPerInstance(partitionedContext.getMaxPartitionsPerParticipant());
      idealState.setStateModelDefId(partitionedContext.getStateModelDefId());
      idealState.setStateModelFactoryId(partitionedContext.getStateModelFactoryId());
      idealState.setBucketSize(bucketSize);
      idealState.setBatchMessageMode(batchMessageMode);
      if (partitionedContext.getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
        SemiAutoRebalancerContext semiAutoContext =
            config.getRebalancerContext(SemiAutoRebalancerContext.class);
        for (PartitionId partitionId : semiAutoContext.getPartitionSet()) {
          idealState.setPreferenceList(partitionId, semiAutoContext.getPreferenceList(partitionId));
        }
      } else if (partitionedContext.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
        CustomRebalancerContext customContext =
            config.getRebalancerContext(CustomRebalancerContext.class);
        for (PartitionId partitionId : customContext.getPartitionSet()) {
          idealState.setParticipantStateMap(partitionId,
              customContext.getPreferenceMap(partitionId));
        }
      }
      return idealState;
    }
    return null;
  }

  /**
   * Create a resource snapshot instance from the physical model
   * @param resourceId the resource id
   * @param resourceConfiguration physical resource configuration
   * @param idealState ideal state of the resource
   * @param externalView external view of the resource
   * @param resourceAssignment current resource assignment
   * @return Resource
   */
  static Resource createResource(ResourceId resourceId,
      ResourceConfiguration resourceConfiguration, IdealState idealState,
      ExternalView externalView, ResourceAssignment resourceAssignment) {
    // TODO pass resource assignment
    UserConfig userConfig;
    if (resourceConfiguration != null) {
      userConfig = UserConfig.from(resourceConfiguration);
    } else {
      userConfig = new UserConfig(Scope.resource(resourceId));
    }
    int bucketSize = 0;
    boolean batchMessageMode = false;
    RebalancerContext rebalancerContext;
    if (idealState != null) {
      rebalancerContext = PartitionedRebalancerContext.from(idealState);
      bucketSize = idealState.getBucketSize();
      batchMessageMode = idealState.getBatchMessageMode();
    } else {
      if (resourceConfiguration != null) {
        bucketSize = resourceConfiguration.getBucketSize();
        batchMessageMode = resourceConfiguration.getBatchMessageMode();
        RebalancerConfig rebalancerConfig = new RebalancerConfig(resourceConfiguration);
        rebalancerContext = rebalancerConfig.getRebalancerContext(RebalancerContext.class);
      } else {
        rebalancerContext = new PartitionedRebalancerContext(RebalanceMode.NONE);
      }
    }
    return new Resource(resourceId, idealState, resourceAssignment, externalView,
        rebalancerContext, userConfig, bucketSize, batchMessageMode);
  }
}
