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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceConfiguration;

import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Captures the configuration properties necessary for rebalancing
 */
public class RebalancerConfig extends NamespacedConfig {

  /**
   * Fields used by the base RebalancerConfig
   */
  public enum Fields {
    ANY_LIVE_PARTICIPANT,
    MAX_PARTITIONS_PER_PARTICIPANT,
    PARTICIPANT_GROUP_TAG,
    REBALANCE_MODE,
    REPLICA_COUNT,
    STATE_MODEL_DEFINITION,
    STATE_MODEL_FACTORY
  }

  private final ResourceId _resourceId;
  private final Set<String> _fieldsSet;
  private final Map<PartitionId, Partition> _partitionMap;

  /**
   * Instantiate a RebalancerConfig.
   * @param resourceId the resource to rebalance
   * @param rebalancerMode the mode to rebalance with
   * @param stateModelDefId the state model that the resource uses
   * @param partitionMap partitions of the resource
   */
  public RebalancerConfig(ResourceId resourceId, RebalanceMode rebalancerMode,
      StateModelDefId stateModelDefId, Map<PartitionId, Partition> partitionMap) {
    super(resourceId, RebalancerConfig.class.getSimpleName());
    _resourceId = resourceId;
    _fieldsSet =
        ImmutableSet.copyOf(Lists.transform(Arrays.asList(Fields.values()),
            Functions.toStringFunction()));
    setEnumField(Fields.REBALANCE_MODE.toString(), rebalancerMode);
    setSimpleField(Fields.STATE_MODEL_DEFINITION.toString(), stateModelDefId.stringify());
    _partitionMap = ImmutableMap.copyOf(partitionMap);
  }

  /**
   * Extract rebalancer-specific configuration from a physical resource config
   * @param resourceConfiguration resource config
   * @param partitionMap map of partition id to partition object
   */
  protected RebalancerConfig(ResourceConfiguration resourceConfiguration,
      Map<PartitionId, Partition> partitionMap) {
    super(resourceConfiguration, RebalancerConfig.class.getSimpleName());
    _resourceId = resourceConfiguration.getResourceId();
    _fieldsSet =
        ImmutableSet.copyOf(Lists.transform(Arrays.asList(Fields.values()),
            Functions.toStringFunction()));
    _partitionMap = ImmutableMap.copyOf(partitionMap);
  }

  /**
   * Copy a RebalancerConfig from an existing one
   * @param config populated RebalancerConfig
   */
  protected RebalancerConfig(RebalancerConfig config) {
    super(config.getResourceId(), RebalancerConfig.class.getSimpleName());
    _resourceId = config.getResourceId();
    _partitionMap = config.getPartitionMap();
    _fieldsSet =
        ImmutableSet.copyOf(Lists.transform(Arrays.asList(Fields.values()),
            Functions.toStringFunction()));
    super.setSimpleFields(config.getRawSimpleFields());
    super.setListFields(config.getRawListFields());
    super.setMapFields(config.getRawMapFields());
    if (config.getRawPayload() != null && config.getRawPayload().length > 0) {
      setRawPayload(config.getRawPayload());
      setPayloadSerializer(config.getPayloadSerializer());
    }
  }

  /**
   * Get the resource id
   * @return ResourceId
   */
  public ResourceId getResourceId() {
    return _resourceId;
  }

  /**
   * Get the partitions of the resource
   * @return map of partition id to partition or empty map if none
   */
  public Map<PartitionId, Partition> getPartitionMap() {
    return _partitionMap;
  }

  /**
   * Get a partition that the resource contains
   * @param partitionId the partition id to look up
   * @return Partition or null if none is present with the given id
   */
  public Partition getPartition(PartitionId partitionId) {
    return _partitionMap.get(partitionId);
  }

  /**
   * Get the set of partition ids that the resource contains
   * @return partition id set, or empty if none
   */
  public Set<PartitionId> getPartitionSet() {
    Set<PartitionId> partitionSet = new HashSet<PartitionId>();
    partitionSet.addAll(_partitionMap.keySet());
    return ImmutableSet.copyOf(partitionSet);
  }

  /**
   * Get the rebalancer mode
   * @return rebalancer mode
   */
  public RebalanceMode getRebalancerMode() {
    return getEnumField(Fields.REBALANCE_MODE.toString(), RebalanceMode.class, RebalanceMode.NONE);
  }

  /**
   * Get state model definition name of the resource
   * @return state model definition
   */
  public StateModelDefId getStateModelDefId() {
    return Id.stateModelDef(getStringField(Fields.STATE_MODEL_DEFINITION.toString(), null));
  }

  /**
   * Get the number of replicas each partition should have. This function will return 0 if some
   * policy overrides the replica count, for instance if any live participant can accept a replica.
   * @return replica count
   */
  public int getReplicaCount() {
    return getIntField(Fields.REPLICA_COUNT.toString(), 0);
  }

  /**
   * Set the number of replicas each partition should have.
   * @param replicaCount replica count
   */
  public void setReplicaCount(int replicaCount) {
    setIntField(Fields.REPLICA_COUNT.toString(), replicaCount);
  }

  /**
   * Get the number of partitions of this resource that a given participant can accept
   * @return maximum number of partitions
   */
  public int getMaxPartitionsPerParticipant() {
    return getIntField(Fields.MAX_PARTITIONS_PER_PARTICIPANT.toString(), Integer.MAX_VALUE);
  }

  /**
   * Set the number of partitions of this resource that a given participant can accept
   * @param maxPartitionsPerParticipant maximum number of partitions
   */
  public void setMaxPartitionsPerParticipant(int maxPartitionsPerParticipant) {
    setIntField(Fields.MAX_PARTITIONS_PER_PARTICIPANT.toString(), maxPartitionsPerParticipant);
  }

  /**
   * Get the tag, if any, which must be present on assignable instances
   * @return group tag, or null if it is not present
   */
  public String getParticipantGroupTag() {
    return getStringField(Fields.PARTICIPANT_GROUP_TAG.toString(), null);
  }

  /**
   * Set the tag, if any, which must be present on assignable instances
   * @param participantGroupTag group tag
   */
  public void setParticipantGroupTag(String participantGroupTag) {
    setSimpleField(Fields.PARTICIPANT_GROUP_TAG.toString(), participantGroupTag);
  }

  /**
   * Get state model factory id
   * @return state model factory id
   */
  public StateModelFactoryId getStateModelFactoryId() {
    return Id.stateModelFactory(getStringField(Fields.STATE_MODEL_FACTORY.toString(), null));
  }

  /**
   * Set state model factory id
   * @param factoryId state model factory id
   */
  public void setStateModelFactoryId(StateModelFactoryId factoryId) {
    if (factoryId != null) {
      setSimpleField(Fields.STATE_MODEL_FACTORY.toString(), factoryId.stringify());
    }
  }

  /**
   * Check if replicas can be assigned to any live participant
   * @return true if they can, false if they cannot
   */
  public boolean canAssignAnyLiveParticipant() {
    return getBooleanField(Fields.ANY_LIVE_PARTICIPANT.toString(), false);
  }

  /**
   * Specify if replicas can be assigned to any live participant
   * @param canAssignAny true if they can, false if they cannot
   */
  public void setCanAssignAnyLiveParticipant(boolean canAssignAny) {
    setBooleanField(Fields.ANY_LIVE_PARTICIPANT.toString(), canAssignAny);
  }

  /*
   * Override: removes from view fields set by RebalancerConfig
   */
  @Override
  public Map<String, String> getSimpleFields() {
    return Maps.filterKeys(super.getSimpleFields(), filterBaseConfigFields());
  }

  /*
   * Override: makes sure that updated simple fields include those from this class
   */
  @Override
  public void setSimpleFields(Map<String, String> simpleFields) {
    Map<String, String> copySimpleFields = new HashMap<String, String>();
    copySimpleFields.putAll(simpleFields);
    for (String field : _fieldsSet) {
      String value = getStringField(field, null);
      if (value != null) {
        copySimpleFields.put(field, value);
      }
    }
    super.setSimpleFields(copySimpleFields);
  }

  /**
   * Get a predicate that can checks if a key is used by this config
   * @return Guava Predicate
   */
  private Predicate<String> filterBaseConfigFields() {
    return new Predicate<String>() {
      @Override
      public boolean apply(String key) {
        return !_fieldsSet.contains(key);
      }
    };
  }

  /**
   * Get simple fields without filtering out base fields
   * @return simple fields
   */
  private Map<String, String> getRawSimpleFields() {
    return super.getSimpleFields();
  }

  /**
   * Get list fields without filtering out base fields
   * @return list fields
   */
  private Map<String, List<String>> getRawListFields() {
    return super.getListFields();
  }

  /**
   * Get map fields without filtering out base fields
   * @return map fields
   */
  private Map<String, Map<String, String>> getRawMapFields() {
    return super.getMapFields();
  }

  /**
   * Get a RebalancerConfig from a physical resource configuration
   * @param config resource configuration
   * @return RebalancerConfig
   */
  public static RebalancerConfig from(ResourceConfiguration config,
      Map<PartitionId, UserConfig> partitionConfigs) {
    Map<PartitionId, Partition> partitionMap = new HashMap<PartitionId, Partition>();
    for (PartitionId partitionId : config.getPartitionIds()) {
      if (partitionConfigs.containsKey(partitionId)) {
        partitionMap
            .put(partitionId, new Partition(partitionId, partitionConfigs.get(partitionId)));
      } else {
        partitionMap.put(partitionId, new Partition(partitionId));
      }
    }
    return new RebalancerConfig(config, partitionMap);
  }

  /**
   * Assembles a RebalancerConfig
   */
  public static abstract class Builder<T extends Builder<T>> {
    protected final ResourceId _resourceId;
    protected final Map<PartitionId, Partition> _partitionMap;
    protected StateModelDefId _stateModelDefId;
    private StateModelFactoryId _stateModelFactoryId;
    private boolean _anyLiveParticipant;
    private int _replicaCount;
    private int _maxPartitionsPerParticipant;

    /**
     * Configure the rebalancer for a resource
     * @param resourceId the resource to rebalance
     */
    public Builder(ResourceId resourceId) {
      _resourceId = resourceId;
      _anyLiveParticipant = false;
      _replicaCount = 0;
      _maxPartitionsPerParticipant = Integer.MAX_VALUE;
      _partitionMap = new HashMap<PartitionId, Partition>();
    }

    /**
     * Set the state model definition
     * @param stateModelDefId state model identifier
     * @return Builder
     */
    public T stateModelDef(StateModelDefId stateModelDefId) {
      _stateModelDefId = stateModelDefId;
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
     * @param maxPartitions
     * @return Builder
     */
    public T maxPartitionsPerParticipant(int maxPartitions) {
      _maxPartitionsPerParticipant = maxPartitions;
      return self();
    }

    /**
     * Set state model factory
     * @param stateModelFactoryId
     * @return Builder
     */
    public T stateModelFactoryId(StateModelFactoryId stateModelFactoryId) {
      _stateModelFactoryId = stateModelFactoryId;
      return self();
    }

    /**
     * Set whether any live participant should be used in rebalancing
     * @param useAnyParticipant true if any live participant can be used, false otherwise
     * @return Builder
     */
    public T anyLiveParticipant(boolean useAnyParticipant) {
      _anyLiveParticipant = useAnyParticipant;
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
     * @param partitions
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
     * These partitions are added without any user configuration properties
     * @param partitionCount number of partitions to add
     * @return Builder
     */
    public T addPartitions(int partitionCount) {
      for (int i = 0; i < partitionCount; i++) {
        addPartition(new Partition(Id.partition(_resourceId, Integer.toString(i)), null));
      }
      return self();
    }

    /**
     * Update a RebalancerConfig with built fields
     */
    protected void update(RebalancerConfig rebalancerConfig) {
      rebalancerConfig.setReplicaCount(_replicaCount);
      rebalancerConfig.setCanAssignAnyLiveParticipant(_anyLiveParticipant);
      rebalancerConfig.setMaxPartitionsPerParticipant(_maxPartitionsPerParticipant);
      if (_stateModelFactoryId != null) {
        rebalancerConfig.setStateModelFactoryId(_stateModelFactoryId);
      }
    }

    /**
     * Get a reference to the actual builder class
     * @return Builder
     */
    protected abstract T self();

    /**
     * Get a fully-initialized RebalancerConfig instance
     * @return RebalancerConfig based on what was built
     */
    public abstract RebalancerConfig build();
  }
}
