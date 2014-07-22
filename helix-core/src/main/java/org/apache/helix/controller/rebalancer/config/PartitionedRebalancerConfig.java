package org.apache.helix.controller.rebalancer.config;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.api.Partition;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.CustomRebalancer;
import org.apache.helix.controller.rebalancer.FullAutoRebalancer;
import org.apache.helix.controller.rebalancer.HelixRebalancer;
import org.apache.helix.controller.rebalancer.RebalancerRef;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskRebalancer;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
 * RebalancerConfig for a resource whose subunits are partitions. In addition, these partitions can
 * be replicated.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionedRebalancerConfig extends BasicRebalancerConfig implements
    ReplicatedRebalancerConfig {
  private Map<PartitionId, Partition> _partitionMap;
  private boolean _anyLiveParticipant;
  private int _replicaCount;
  private int _maxPartitionsPerParticipant;
  private RebalanceMode _rebalanceMode;

  @JsonIgnore
  private static final Set<Class<? extends RebalancerConfig>> BUILTIN_CONFIG_CLASSES = Sets
      .newHashSet();
  @JsonIgnore
  private static final Set<Class<? extends HelixRebalancer>> BUILTIN_REBALANCER_CLASSES = Sets
      .newHashSet();
  static {
    BUILTIN_CONFIG_CLASSES.add(PartitionedRebalancerConfig.class);
    BUILTIN_CONFIG_CLASSES.add(FullAutoRebalancerConfig.class);
    BUILTIN_CONFIG_CLASSES.add(SemiAutoRebalancerConfig.class);
    BUILTIN_CONFIG_CLASSES.add(CustomRebalancerConfig.class);
    BUILTIN_REBALANCER_CLASSES.add(FullAutoRebalancer.class);
    BUILTIN_REBALANCER_CLASSES.add(SemiAutoRebalancer.class);
    BUILTIN_REBALANCER_CLASSES.add(CustomRebalancer.class);
    BUILTIN_REBALANCER_CLASSES.add(TaskRebalancer.class);
  }

  /**
   * Instantiate a PartitionedRebalancerConfig
   */
  public PartitionedRebalancerConfig() {
    _partitionMap = Collections.emptyMap();
    _replicaCount = 1;
    _anyLiveParticipant = false;
    _maxPartitionsPerParticipant = Integer.MAX_VALUE;
    _rebalanceMode = RebalanceMode.USER_DEFINED;
  }

  /**
   * Get a map from partition id to partition
   * @return partition map (mutable)
   */
  public Map<PartitionId, Partition> getPartitionMap() {
    return _partitionMap;
  }

  /**
   * Set a map of partition id to partition
   * @param partitionMap partition map
   */
  public void setPartitionMap(Map<PartitionId, Partition> partitionMap) {
    _partitionMap = Maps.newHashMap(partitionMap);
  }

  /**
   * Get the set of partitions for this resource
   * @return set of partition ids
   */
  @JsonIgnore
  public Set<PartitionId> getPartitionSet() {
    return _partitionMap.keySet();
  }

  /**
   * Get a partition
   * @param partitionId id of the partition to get
   * @return Partition object, or null if not present
   */
  @JsonIgnore
  public Partition getPartition(PartitionId partitionId) {
    return _partitionMap.get(partitionId);
  }

  @Override
  public boolean anyLiveParticipant() {
    return _anyLiveParticipant;
  }

  /**
   * Indicate if this resource should be assigned to any live participant
   * @param anyLiveParticipant true if any live participant expected, false otherwise
   */
  public void setAnyLiveParticipant(boolean anyLiveParticipant) {
    _anyLiveParticipant = anyLiveParticipant;
  }

  @Override
  public int getReplicaCount() {
    return _replicaCount;
  }

  /**
   * Set the number of replicas that each partition should have
   * @param replicaCount
   */
  public void setReplicaCount(int replicaCount) {
    _replicaCount = replicaCount;
  }

  /**
   * Get the maximum number of partitions that a participant can serve
   * @return maximum number of partitions per participant
   */
  public int getMaxPartitionsPerParticipant() {
    return _maxPartitionsPerParticipant;
  }

  /**
   * Set the maximum number of partitions that a participant can serve
   * @param maxPartitionsPerParticipant maximum number of partitions per participant
   */
  public void setMaxPartitionsPerParticipant(int maxPartitionsPerParticipant) {
    _maxPartitionsPerParticipant = maxPartitionsPerParticipant;
  }

  /**
   * Set the rebalancer mode of the partitioned resource
   * @param rebalanceMode {@link RebalanceMode} enum value
   */
  public void setRebalanceMode(RebalanceMode rebalanceMode) {
    _rebalanceMode = rebalanceMode;
  }

  /**
   * Get the rebalancer mode of the resource
   * @return RebalanceMode
   */
  public RebalanceMode getRebalanceMode() {
    return _rebalanceMode;
  }

  @Override
  @JsonIgnore
  public Map<PartitionId, Partition> getSubUnitMap() {
    return getPartitionMap();
  }

  /**
   * Generate a default configuration given the state model and a participant.
   * @param stateModelDef the state model definition to follow
   * @param participantSet the set of participant ids to configure for
   */
  @JsonIgnore
  public void generateDefaultConfiguration(StateModelDefinition stateModelDef,
      Set<ParticipantId> participantSet) {
    // the base config does not understand enough to know do to anything
  }

  /**
   * Safely get a {@link PartitionedRebalancerConfig} from a {@link RebalancerConfig}
   * @param config the base config
   * @return a {@link PartitionedRebalancerConfig}, or null if the conversion is not possible
   */
  public static PartitionedRebalancerConfig from(RebalancerConfig config) {
    try {
      return PartitionedRebalancerConfig.class.cast(config);
    } catch (ClassCastException e) {
      return null;
    }
  }

  /**
   * Check if the given class is compatible with an {@link IdealState}
   * @param clazz the PartitionedRebalancerConfig subclass
   * @return true if IdealState can be used to describe this config, false otherwise
   */
  public static boolean isBuiltinConfig(Class<? extends RebalancerConfig> clazz) {
    return BUILTIN_CONFIG_CLASSES.contains(clazz);
  }

  /**
   * Check if the given class is a built-in rebalancer class
   * @param clazz the HelixRebalancer subclass
   * @return true if the rebalancer class is built in, false otherwise
   */
  public static boolean isBuiltinRebalancer(Class<? extends HelixRebalancer> clazz) {
    return BUILTIN_REBALANCER_CLASSES.contains(clazz);
  }

  /**
   * Convert a physically-stored IdealState into a rebalancer config for a partitioned resource
   * @param idealState populated IdealState
   * @return PartitionedRebalancerConfig
   */
  public static PartitionedRebalancerConfig from(IdealState idealState) {
    PartitionedRebalancerConfig config;
    RebalanceMode mode = idealState.getRebalanceMode();
    if (mode == RebalanceMode.USER_DEFINED) {
      Class<? extends RebalancerConfig> configClass = idealState.getRebalancerConfigClass();
      if (configClass.equals(FullAutoRebalancerConfig.class)) {
        mode = RebalanceMode.FULL_AUTO;
      } else if (configClass.equals(SemiAutoRebalancerConfig.class)) {
        mode = RebalanceMode.SEMI_AUTO;
      } else if (configClass.equals(CustomRebalancerConfig.class)) {
        mode = RebalanceMode.CUSTOMIZED;
      }
    }
    switch (mode) {
    case FULL_AUTO:
      FullAutoRebalancerConfig.Builder fullAutoBuilder =
          new FullAutoRebalancerConfig.Builder(idealState.getResourceId());
      populateConfig(fullAutoBuilder, idealState);
      config = fullAutoBuilder.build();
      break;
    case SEMI_AUTO:
      SemiAutoRebalancerConfig.Builder semiAutoBuilder =
          new SemiAutoRebalancerConfig.Builder(idealState.getResourceId());
      for (PartitionId partitionId : idealState.getPartitionIdSet()) {
        semiAutoBuilder.preferenceList(partitionId, idealState.getPreferenceList(partitionId));
      }
      populateConfig(semiAutoBuilder, idealState);
      config = semiAutoBuilder.build();
      break;
    case CUSTOMIZED:
      CustomRebalancerConfig.Builder customBuilder =
          new CustomRebalancerConfig.Builder(idealState.getResourceId());
      for (PartitionId partitionId : idealState.getPartitionIdSet()) {
        customBuilder.preferenceMap(partitionId, idealState.getParticipantStateMap(partitionId));
      }
      populateConfig(customBuilder, idealState);
      config = customBuilder.build();
      break;
    default:
      Builder baseBuilder = new Builder(idealState.getResourceId());
      populateConfig(baseBuilder, idealState);
      config = baseBuilder.build();
      break;
    }
    return config;
  }

  /**
   * Update a builder subclass with all the fields of the ideal state
   * @param builder builder that extends AbstractBuilder
   * @param idealState populated IdealState
   */
  private static <T extends AbstractBuilder<T>> void populateConfig(T builder, IdealState idealState) {
    String replicas = idealState.getReplicas();
    int replicaCount = 0;
    boolean anyLiveParticipant = false;
    if (replicas.equals(StateModelToken.ANY_LIVEINSTANCE.toString())) {
      anyLiveParticipant = true;
    } else {
      replicaCount = Integer.parseInt(replicas);
    }
    if (idealState.getNumPartitions() > 0 && idealState.getPartitionIdSet().size() == 0) {
      // backwards compatibility: partition sets were based on pref lists/maps previously
      builder.addPartitions(idealState.getNumPartitions());
    } else {
      for (PartitionId partitionId : idealState.getPartitionIdSet()) {
        builder.addPartition(new Partition(partitionId));
      }
    }
    builder.anyLiveParticipant(anyLiveParticipant).replicaCount(replicaCount)
        .maxPartitionsPerParticipant(idealState.getMaxPartitionsPerInstance())
        .participantGroupTag(idealState.getInstanceGroupTag())
        .stateModelDefId(idealState.getStateModelDefId())
        .stateModelFactoryId(idealState.getStateModelFactoryId())
        .rebalanceMode(idealState.getRebalanceMode());
    RebalancerRef rebalancerRef = idealState.getRebalancerRef();
    if (rebalancerRef != null) {
      builder.rebalancerRef(rebalancerRef);
    }
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
      if (!PartitionedRebalancerConfig.isBuiltinConfig(partitionedConfig.getClass())) {
        // don't proceed if this resource cannot be described by an ideal state
        return null;
      }
      IdealState idealState = new IdealState(partitionedConfig.getResourceId());
      idealState.setRebalanceMode(partitionedConfig.getRebalanceMode());

      RebalancerRef ref = partitionedConfig.getRebalancerRef();
      if (ref != null) {
        idealState.setRebalancerRef(partitionedConfig.getRebalancerRef());
      }
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
      idealState.setRebalancerConfigClass(config.getClass());
      if (SemiAutoRebalancerConfig.class.equals(config.getClass())) {
        SemiAutoRebalancerConfig semiAutoConfig =
            BasicRebalancerConfig.convert(config, SemiAutoRebalancerConfig.class);
        for (PartitionId partitionId : semiAutoConfig.getPartitionSet()) {
          idealState.setPreferenceList(partitionId, semiAutoConfig.getPreferenceList(partitionId));
        }
      } else if (CustomRebalancerConfig.class.equals(config.getClass())) {
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
   * Builder for a basic data rebalancer config
   */
  public static final class Builder extends AbstractBuilder<Builder> {
    /**
     * Instantiate with a resource
     * @param resourceId resource id
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public PartitionedRebalancerConfig build() {
      PartitionedRebalancerConfig config = new PartitionedRebalancerConfig();
      super.update(config);
      return config;
    }
  }

  /**
   * Abstract builder for a generic partitioned resource rebalancer config
   */
  public static abstract class AbstractBuilder<T extends BasicRebalancerConfig.AbstractBuilder<T>>
      extends BasicRebalancerConfig.AbstractBuilder<T> {
    private final ResourceId _resourceId;
    private final Map<PartitionId, Partition> _partitionMap;
    private RebalanceMode _rebalanceMode;
    private boolean _anyLiveParticipant;
    private int _replicaCount;
    private int _maxPartitionsPerParticipant;

    /**
     * Instantiate with a resource
     * @param resourceId resource id
     */
    public AbstractBuilder(ResourceId resourceId) {
      super(resourceId);
      _resourceId = resourceId;
      _partitionMap = Maps.newHashMap();
      _rebalanceMode = RebalanceMode.USER_DEFINED;
      _anyLiveParticipant = false;
      _replicaCount = 1;
      _maxPartitionsPerParticipant = Integer.MAX_VALUE;
    }

    /**
     * Set the rebalance mode for a partitioned rebalancer config
     * @param rebalanceMode {@link RebalanceMode} enum value
     * @return Builder
     */
    public T rebalanceMode(RebalanceMode rebalanceMode) {
      _rebalanceMode = rebalanceMode;
      return self();
    }

    /**
     * Add a partition that the resource serves
     * @param partition fully-qualified partition
     * @return Builder
     */
    public T addPartition(Partition partition) {
      _partitionMap.put(partition.getId(), partition);
      return self();
    }

    /**
     * Add a collection of partitions
     * @param partitions any collection of Partition objects
     * @return Builder
     */
    public T addPartitions(Collection<Partition> partitions) {
      for (Partition partition : partitions) {
        addPartition(partition);
      }
      return self();
    }

    /**
     * Add a specified number of partitions with a default naming scheme, namely
     * resourceId_partitionNumber where partitionNumber starts at 0
     * @param partitionCount number of partitions to add
     * @return Builder
     */
    public T addPartitions(int partitionCount) {
      for (int i = 0; i < partitionCount; i++) {
        addPartition(new Partition(PartitionId.from(_resourceId, Integer.toString(i))));
      }
      return self();
    }

    /**
     * Set whether any live participant should be used in rebalancing
     * @param anyLiveParticipant true if any live participant can be used, false otherwise
     * @return Builder
     */
    public T anyLiveParticipant(boolean anyLiveParticipant) {
      _anyLiveParticipant = anyLiveParticipant;
      return self();
    }

    /**
     * Set the number of replicas
     * @param replicaCount number of replicas
     * @return Builder
     */
    public T replicaCount(int replicaCount) {
      _replicaCount = replicaCount;
      return self();
    }

    /**
     * Set the maximum number of partitions to assign to any participant
     * @param maxPartitionsPerParticipant the maximum
     * @return Builder
     */
    public T maxPartitionsPerParticipant(int maxPartitionsPerParticipant) {
      _maxPartitionsPerParticipant = maxPartitionsPerParticipant;
      return self();
    }

    /**
     * Update a PartitionedRebalancerConfig with fields from this builder level
     * @param config PartitionedRebalancerConfig
     */
    protected final void update(PartitionedRebalancerConfig config) {
      super.update(config);
      // enforce at least one partition
      if (_partitionMap.isEmpty()) {
        addPartitions(1);
      }
      config.setRebalanceMode(_rebalanceMode);
      config.setPartitionMap(_partitionMap);
      config.setAnyLiveParticipant(_anyLiveParticipant);
      config.setMaxPartitionsPerParticipant(_maxPartitionsPerParticipant);
      config.setReplicaCount(_replicaCount);
    }
  }
}
