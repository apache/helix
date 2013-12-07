package org.apache.helix.api.accessor;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.Resource;
import org.apache.helix.api.Scope;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.ResourceConfig.ResourceType;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.config.BasicRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.CustomRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.PartitionedRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfigHolder;
import org.apache.helix.controller.rebalancer.config.SemiAutoRebalancerConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ResourceAccessor {
  private static final Logger LOG = Logger.getLogger(ResourceAccessor.class);
  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;

  public ResourceAccessor(HelixDataAccessor accessor) {
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
  }

  /**
   * Read a single snapshot of a resource
   * @param resourceId the resource id to read
   * @return Resource or null if not present
   */
  public Resource readResource(ResourceId resourceId) {
    ResourceConfiguration config =
        _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealStates(resourceId.stringify()));

    if (config == null && idealState == null) {
      LOG.error("Resource " + resourceId + " not present on the cluster");
      return null;
    }

    ExternalView externalView =
        _accessor.getProperty(_keyBuilder.externalView(resourceId.stringify()));
    ResourceAssignment resourceAssignment =
        _accessor.getProperty(_keyBuilder.resourceAssignment(resourceId.stringify()));
    return createResource(resourceId, config, idealState, externalView, resourceAssignment);
  }

  /**
   * Update a resource configuration
   * @param resourceId the resource id to update
   * @param resourceDelta changes to the resource
   * @return ResourceConfig, or null if the resource is not persisted
   */
  public ResourceConfig updateResource(ResourceId resourceId, ResourceConfig.Delta resourceDelta) {
    Resource resource = readResource(resourceId);
    if (resource == null) {
      LOG.error("Resource " + resourceId + " does not exist, cannot be updated");
      return null;
    }
    ResourceConfig config = resourceDelta.mergeInto(resource.getConfig());
    setResource(config);
    return config;
  }

  /**
   * save resource assignment
   * @param resourceId
   * @param resourceAssignment
   * @return true if set, false otherwise
   */
  public boolean setResourceAssignment(ResourceId resourceId, ResourceAssignment resourceAssignment) {
    return _accessor.setProperty(_keyBuilder.resourceAssignment(resourceId.stringify()),
        resourceAssignment);
  }

  /**
   * get resource assignment
   * @param resourceId
   * @return resource assignment or null
   */
  public ResourceAssignment getResourceAssignment(ResourceId resourceId) {
    return _accessor.getProperty(_keyBuilder.resourceAssignment(resourceId.stringify()));
  }

  /**
   * Set a physical resource configuration, which may include user-defined configuration, as well as
   * rebalancer configuration
   * @param resourceId
   * @param configuration
   * @return true if set, false otherwise
   */
  private boolean setConfiguration(ResourceId resourceId, ResourceConfiguration configuration,
      RebalancerConfig rebalancerConfig) {
    boolean status =
        _accessor.setProperty(_keyBuilder.resourceConfig(resourceId.stringify()), configuration);
    // set an ideal state if the resource supports it
    IdealState idealState =
        rebalancerConfigToIdealState(rebalancerConfig, configuration.getBucketSize(),
            configuration.getBatchMessageMode());
    if (idealState != null) {
      _accessor.setProperty(_keyBuilder.idealStates(resourceId.stringify()), idealState);
    }
    return status;
  }

  /**
   * Set the config of the rebalancer. This includes all properties required for rebalancing this
   * resource
   * @param resourceId the resource to update
   * @param config the new rebalancer config
   * @return true if the config was set, false otherwise
   */
  public boolean setRebalancerConfig(ResourceId resourceId, RebalancerConfig config) {
    ResourceConfiguration resourceConfig = new ResourceConfiguration(resourceId);
    PartitionedRebalancerConfig partitionedConfig = PartitionedRebalancerConfig.from(config);
    if (partitionedConfig == null
        || partitionedConfig.getRebalanceMode() == RebalanceMode.USER_DEFINED) {
      // only persist if this is not easily convertible to an ideal state
      resourceConfig.addNamespacedConfig(new RebalancerConfigHolder(config).toNamespacedConfig());
    }

    // update the ideal state if applicable
    IdealState oldIdealState =
        _accessor.getProperty(_keyBuilder.idealStates(resourceId.stringify()));
    if (oldIdealState != null) {
      IdealState idealState =
          rebalancerConfigToIdealState(config, oldIdealState.getBucketSize(),
              oldIdealState.getBatchMessageMode());
      if (idealState != null) {
        _accessor.setProperty(_keyBuilder.idealStates(resourceId.stringify()), idealState);
      }
    }

    return _accessor.updateProperty(_keyBuilder.resourceConfig(resourceId.stringify()),
        resourceConfig);
  }

  /**
   * Read the user config of the resource
   * @param resourceId the resource to to look up
   * @return UserConfig, or null
   */
  public UserConfig readUserConfig(ResourceId resourceId) {
    ResourceConfiguration resourceConfig =
        _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    return resourceConfig != null ? UserConfig.from(resourceConfig) : null;
  }

  /**
   * Read the rebalancer config of the resource
   * @param resourceId the resource to to look up
   * @return RebalancerConfig, or null
   */
  public RebalancerConfig readRebalancerConfig(ResourceId resourceId) {
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealStates(resourceId.stringify()));
    if (idealState != null && idealState.getRebalanceMode() != RebalanceMode.USER_DEFINED) {
      return PartitionedRebalancerConfig.from(idealState);
    }
    ResourceConfiguration resourceConfig =
        _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    return resourceConfig.getRebalancerConfig(RebalancerConfig.class);
  }

  /**
   * Set the user config of the resource, overwriting existing user configs
   * @param resourceId the resource to update
   * @param userConfig the new user config
   * @return true if the user config was set, false otherwise
   */
  public boolean setUserConfig(ResourceId resourceId, UserConfig userConfig) {
    ResourceConfig.Delta delta = new ResourceConfig.Delta(resourceId).setUserConfig(userConfig);
    return updateResource(resourceId, delta) != null;
  }

  /**
   * Add user configuration to the existing resource user configuration. Overwrites properties with
   * the same key
   * @param resourceId the resource to update
   * @param userConfig the user config key-value pairs to add
   * @return true if the user config was updated, false otherwise
   */
  public boolean updateUserConfig(ResourceId resourceId, UserConfig userConfig) {
    ResourceConfiguration resourceConfig = new ResourceConfiguration(resourceId);
    resourceConfig.addNamespacedConfig(userConfig);
    return _accessor.updateProperty(_keyBuilder.resourceConfig(resourceId.stringify()),
        resourceConfig);
  }

  /**
   * Clear any user-specified configuration from the resource
   * @param resourceId the resource to update
   * @return true if the config was cleared, false otherwise
   */
  public boolean dropUserConfig(ResourceId resourceId) {
    return setUserConfig(resourceId, new UserConfig(Scope.resource(resourceId)));
  }

  /**
   * Persist an existing resource's logical configuration
   * @param resourceConfig logical resource configuration
   * @return true if resource is set, false otherwise
   */
  public boolean setResource(ResourceConfig resourceConfig) {
    if (resourceConfig == null || resourceConfig.getRebalancerConfig() == null) {
      LOG.error("Resource not fully defined with a rebalancer context");
      return false;
    }
    ResourceId resourceId = resourceConfig.getId();
    ResourceConfiguration config = new ResourceConfiguration(resourceId);
    config.addNamespacedConfig(resourceConfig.getUserConfig());
    PartitionedRebalancerConfig partitionedConfig =
        PartitionedRebalancerConfig.from(resourceConfig.getRebalancerConfig());
    if (partitionedConfig == null
        || partitionedConfig.getRebalanceMode() == RebalanceMode.USER_DEFINED) {
      // only persist if this is not easily convertible to an ideal state
      config.addNamespacedConfig(new RebalancerConfigHolder(resourceConfig.getRebalancerConfig())
          .toNamespacedConfig());
    }
    config.setBucketSize(resourceConfig.getBucketSize());
    config.setBatchMessageMode(resourceConfig.getBatchMessageMode());
    setConfiguration(resourceId, config, resourceConfig.getRebalancerConfig());
    return true;
  }

  /**
   * Get a resource configuration, which may include user-defined configuration, as well as
   * rebalancer configuration
   * @param resourceId
   * @return configuration or null
   */
  public ResourceConfiguration getConfiguration(ResourceId resourceId) {
    return _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
  }

  /**
   * set external view of a resource
   * @param resourceId
   * @param extView
   * @return true if set, false otherwise
   */
  public boolean setExternalView(ResourceId resourceId, ExternalView extView) {
    return _accessor.setProperty(_keyBuilder.externalView(resourceId.stringify()), extView);
  }

  /**
   * get the external view of a resource
   * @param resourceId the resource to look up
   * @return external view or null
   */
  public ExternalView readExternalView(ResourceId resourceId) {
    return _accessor.getProperty(_keyBuilder.externalView(resourceId.stringify()));
  }

  /**
   * drop external view of a resource
   * @param resourceId
   * @return true if dropped, false otherwise
   */
  public boolean dropExternalView(ResourceId resourceId) {
    return _accessor.removeProperty(_keyBuilder.externalView(resourceId.stringify()));
  }

  /**
   * reset resources for all participants
   * @param resetResourceIdSet the resources to reset
   * @return true if they were reset, false otherwise
   */
  public boolean resetResources(Set<ResourceId> resetResourceIdSet) {
    ParticipantAccessor accessor = participantAccessor();
    List<ExternalView> extViews = _accessor.getChildValues(_keyBuilder.externalViews());
    for (ExternalView extView : extViews) {
      if (!resetResourceIdSet.contains(extView.getResourceId())) {
        continue;
      }

      Map<ParticipantId, Set<PartitionId>> resetPartitionIds = Maps.newHashMap();
      for (PartitionId partitionId : extView.getPartitionIdSet()) {
        Map<ParticipantId, State> stateMap = extView.getStateMap(partitionId);
        for (ParticipantId participantId : stateMap.keySet()) {
          State state = stateMap.get(participantId);
          if (state.equals(State.from(HelixDefinedState.ERROR))) {
            if (!resetPartitionIds.containsKey(participantId)) {
              resetPartitionIds.put(participantId, new HashSet<PartitionId>());
            }
            resetPartitionIds.get(participantId).add(partitionId);
          }
        }
      }
      for (ParticipantId participantId : resetPartitionIds.keySet()) {
        accessor.resetPartitionsForParticipant(participantId, extView.getResourceId(),
            resetPartitionIds.get(participantId));
      }
    }
    return true;
  }

  /**
   * Generate a default assignment for partitioned resources
   * @param resourceId the resource to update
   * @param replicaCount the new replica count (or -1 to use the existing one)
   * @param participantGroupTag the new participant group tag (or null to use the existing one)
   * @return true if assignment successful, false otherwise
   */
  public boolean generateDefaultAssignment(ResourceId resourceId, int replicaCount,
      String participantGroupTag) {
    Resource resource = readResource(resourceId);
    RebalancerConfig config = resource.getRebalancerConfig();
    PartitionedRebalancerConfig partitionedConfig = PartitionedRebalancerConfig.from(config);
    if (partitionedConfig == null) {
      LOG.error("Only partitioned resource types are supported");
      return false;
    }
    if (replicaCount != -1) {
      partitionedConfig.setReplicaCount(replicaCount);
    }
    if (participantGroupTag != null) {
      partitionedConfig.setParticipantGroupTag(participantGroupTag);
    }
    StateModelDefinition stateModelDef =
        _accessor.getProperty(_keyBuilder.stateModelDef(partitionedConfig.getStateModelDefId()
            .stringify()));
    List<InstanceConfig> participantConfigs =
        _accessor.getChildValues(_keyBuilder.instanceConfigs());
    Set<ParticipantId> participantSet = Sets.newHashSet();
    for (InstanceConfig participantConfig : participantConfigs) {
      participantSet.add(participantConfig.getParticipantId());
    }
    partitionedConfig.generateDefaultConfiguration(stateModelDef, participantSet);
    setRebalancerConfig(resourceId, partitionedConfig);
    return true;
  }

  /**
   * Get an ideal state from a rebalancer config if the resource is partitioned
   * @param config RebalancerConfig instance
   * @param bucketSize bucket size to use
   * @param batchMessageMode true if batch messaging allowed, false otherwise
   * @return IdealState, or null
   */
  public static IdealState rebalancerConfigToIdealState(RebalancerConfig config, int bucketSize,
      boolean batchMessageMode) {
    PartitionedRebalancerConfig partitionedConfig = PartitionedRebalancerConfig.from(config);
    if (partitionedConfig != null) {
      IdealState idealState = new IdealState(partitionedConfig.getResourceId());
      idealState.setRebalanceMode(partitionedConfig.getRebalanceMode());
      idealState.setRebalancerRef(partitionedConfig.getRebalancerRef());
      String replicas = null;
      if (partitionedConfig.anyLiveParticipant()) {
        replicas = StateModelToken.ANY_LIVEINSTANCE.toString();
      } else {
        replicas = Integer.toString(partitionedConfig.getReplicaCount());
      }
      idealState.setReplicas(replicas);
      idealState.setNumPartitions(partitionedConfig.getPartitionSet().size());
      idealState.setInstanceGroupTag(partitionedConfig.getParticipantGroupTag());
      idealState.setMaxPartitionsPerInstance(partitionedConfig.getMaxPartitionsPerParticipant());
      idealState.setStateModelDefId(partitionedConfig.getStateModelDefId());
      idealState.setStateModelFactoryId(partitionedConfig.getStateModelFactoryId());
      idealState.setBucketSize(bucketSize);
      idealState.setBatchMessageMode(batchMessageMode);
      if (partitionedConfig.getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
        SemiAutoRebalancerConfig semiAutoConfig =
            BasicRebalancerConfig.convert(config, SemiAutoRebalancerConfig.class);
        for (PartitionId partitionId : semiAutoConfig.getPartitionSet()) {
          idealState.setPreferenceList(partitionId, semiAutoConfig.getPreferenceList(partitionId));
        }
      } else if (partitionedConfig.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
        CustomRebalancerConfig customConfig =
            BasicRebalancerConfig.convert(config, CustomRebalancerConfig.class);
        for (PartitionId partitionId : customConfig.getPartitionSet()) {
          idealState
              .setParticipantStateMap(partitionId, customConfig.getPreferenceMap(partitionId));
        }
      } else {
        for (PartitionId partitionId : partitionedConfig.getPartitionSet()) {
          List<ParticipantId> preferenceList = Collections.emptyList();
          idealState.setPreferenceList(partitionId, preferenceList);
          Map<ParticipantId, State> participantStateMap = Collections.emptyMap();
          idealState.setParticipantStateMap(partitionId, participantStateMap);
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
    UserConfig userConfig;
    RebalancerConfig rebalancerConfig = null;
    ResourceType type = ResourceType.DATA;
    if (resourceConfiguration != null) {
      userConfig = resourceConfiguration.getUserConfig();
      type = resourceConfiguration.getType();
    } else {
      userConfig = new UserConfig(Scope.resource(resourceId));
    }
    int bucketSize = 0;
    boolean batchMessageMode = false;
    if (idealState != null) {
      if (resourceConfiguration != null
          && idealState.getRebalanceMode() == RebalanceMode.USER_DEFINED) {
        // prefer rebalancer config for user_defined data rebalancing
        rebalancerConfig =
            resourceConfiguration.getRebalancerConfig(PartitionedRebalancerConfig.class);
      }
      if (rebalancerConfig == null) {
        // prefer ideal state for non-user_defined data rebalancing
        rebalancerConfig = PartitionedRebalancerConfig.from(idealState);
      }
      bucketSize = idealState.getBucketSize();
      batchMessageMode = idealState.getBatchMessageMode();
      idealState.updateUserConfig(userConfig);
    } else if (resourceConfiguration != null) {
      bucketSize = resourceConfiguration.getBucketSize();
      batchMessageMode = resourceConfiguration.getBatchMessageMode();
      rebalancerConfig = resourceConfiguration.getRebalancerConfig(RebalancerConfig.class);
    }
    if (rebalancerConfig == null) {
      rebalancerConfig = new PartitionedRebalancerConfig();
    }
    return new Resource(resourceId, type, idealState, resourceAssignment, externalView,
        rebalancerConfig, userConfig, bucketSize, batchMessageMode);
  }

  /**
   * Get a ParticipantAccessor instance
   * @return ParticipantAccessor
   */
  protected ParticipantAccessor participantAccessor() {
    return new ParticipantAccessor(_accessor);
  }
}
