package org.apache.helix.controller.rebalancer.context;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.api.Partition;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.google.common.collect.Maps;

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
 * RebalancerContext for a resource whose subunits are partitions. In addition, these partitions can
 * be replicated.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionedRebalancerContext extends BasicRebalancerContext implements
    ReplicatedRebalancerContext {
  private Map<PartitionId, Partition> _partitionMap;
  private boolean _anyLiveParticipant;
  private int _replicaCount;
  private int _maxPartitionsPerParticipant;
  private final RebalanceMode _rebalanceMode;

  /**
   * Instantiate a DataRebalancerContext
   */
  public PartitionedRebalancerContext(RebalanceMode rebalanceMode) {
    _partitionMap = Collections.emptyMap();
    _replicaCount = 1;
    _anyLiveParticipant = false;
    _maxPartitionsPerParticipant = Integer.MAX_VALUE;
    _rebalanceMode = rebalanceMode;
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
    // the base context does not understand enough to know do to anything
  }

  /**
   * Convert a physically-stored IdealState into a rebalancer context for a partitioned resource
   * @param idealState populated IdealState
   * @return PartitionedRebalancerContext
   */
  public static PartitionedRebalancerContext from(IdealState idealState) {
    PartitionedRebalancerContext context;
    switch (idealState.getRebalanceMode()) {
    case FULL_AUTO:
      FullAutoRebalancerContext.Builder fullAutoBuilder =
          new FullAutoRebalancerContext.Builder(idealState.getResourceId());
      populateContext(fullAutoBuilder, idealState);
      context = fullAutoBuilder.build();
      break;
    case SEMI_AUTO:
      SemiAutoRebalancerContext.Builder semiAutoBuilder =
          new SemiAutoRebalancerContext.Builder(idealState.getResourceId());
      for (PartitionId partitionId : idealState.getPartitionIdSet()) {
        semiAutoBuilder.preferenceList(partitionId, idealState.getPreferenceList(partitionId));
      }
      populateContext(semiAutoBuilder, idealState);
      context = semiAutoBuilder.build();
      break;
    case CUSTOMIZED:
      CustomRebalancerContext.Builder customBuilder =
          new CustomRebalancerContext.Builder(idealState.getResourceId());
      for (PartitionId partitionId : idealState.getPartitionIdSet()) {
        customBuilder.preferenceMap(partitionId, idealState.getParticipantStateMap(partitionId));
      }
      populateContext(customBuilder, idealState);
      context = customBuilder.build();
      break;
    default:
      Builder baseBuilder = new Builder(idealState.getResourceId());
      populateContext(baseBuilder, idealState);
      context = baseBuilder.build();
      break;
    }
    return context;
  }

  /**
   * Update a builder subclass with all the fields of the ideal state
   * @param builder builder that extends AbstractBuilder
   * @param idealState populated IdealState
   */
  private static <T extends AbstractBuilder<T>> void populateContext(T builder,
      IdealState idealState) {
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
        .stateModelFactoryId(idealState.getStateModelFactoryId());
    RebalancerRef rebalancerRef = idealState.getRebalancerRef();
    if (rebalancerRef != null) {
      builder.rebalancerRef(rebalancerRef);
    }
  }

  /**
   * Builder for a basic data rebalancer context
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
    public PartitionedRebalancerContext build() {
      PartitionedRebalancerContext context =
          new PartitionedRebalancerContext(RebalanceMode.USER_DEFINED);
      super.update(context);
      return context;
    }
  }

  /**
   * Abstract builder for a generic partitioned resource rebalancer context
   */
  public static abstract class AbstractBuilder<T extends BasicRebalancerContext.AbstractBuilder<T>>
      extends BasicRebalancerContext.AbstractBuilder<T> {
    private final ResourceId _resourceId;
    private final Map<PartitionId, Partition> _partitionMap;
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
      _anyLiveParticipant = false;
      _replicaCount = 1;
      _maxPartitionsPerParticipant = Integer.MAX_VALUE;
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
     * Update a DataRebalancerContext with fields from this builder level
     * @param context DataRebalancerContext
     */
    protected final void update(PartitionedRebalancerContext context) {
      super.update(context);
      // enforce at least one partition
      if (_partitionMap.isEmpty()) {
        addPartitions(1);
      }
      context.setPartitionMap(_partitionMap);
      context.setAnyLiveParticipant(_anyLiveParticipant);
      context.setMaxPartitionsPerParticipant(_maxPartitionsPerParticipant);
      context.setReplicaCount(_replicaCount);
    }
  }
}
