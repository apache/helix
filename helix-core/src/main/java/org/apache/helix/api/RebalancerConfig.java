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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Captures the configuration properties necessary for rebalancing
 */
public class RebalancerConfig extends NamespacedConfig {
  private final RebalanceMode _rebalancerMode;
  private final RebalancerRef _rebalancerRef;
  private final StateModelDefId _stateModelDefId;
  private final Map<PartitionId, List<ParticipantId>> _preferenceLists;
  private final Map<PartitionId, Map<ParticipantId, State>> _preferenceMaps;
  private final ResourceAssignment _resourceAssignment;
  private final int _replicaCount;
  private final boolean _anyLiveParticipant;
  private final String _participantGroupTag;
  private final int _maxPartitionsPerParticipant;
  private final StateModelFactoryId _stateModelFactoryId;
  private final Map<PartitionId, Partition> _partitionMap;

  /**
   * Instantiate the configuration of a rebalance task
   * @param idealState the physical ideal state
   * @param resourceAssignment last mapping of a resource
   */
  public RebalancerConfig(Map<PartitionId, Partition> partitionMap, IdealState idealState,
      ResourceAssignment resourceAssignment, int liveParticipantCount) {
    super(idealState.getResourceId(), RebalancerConfig.class.getSimpleName());
    _partitionMap = ImmutableMap.copyOf(partitionMap);
    _rebalancerMode = idealState.getRebalanceMode();
    _rebalancerRef = idealState.getRebalancerRef();
    _stateModelDefId = idealState.getStateModelDefId();
    String replicaCount = idealState.getReplicas();
    if (replicaCount.equals(HelixConstants.StateModelToken.ANY_LIVEINSTANCE.toString())) {
      _replicaCount = liveParticipantCount;
      _anyLiveParticipant = true;
    } else {
      _replicaCount = Integer.parseInt(idealState.getReplicas());
      _anyLiveParticipant = false;
    }
    _participantGroupTag = idealState.getInstanceGroupTag();
    _maxPartitionsPerParticipant = idealState.getMaxPartitionsPerInstance();
    _stateModelFactoryId = idealState.getStateModelFactoryId();

    // Build preference lists and maps
    ImmutableMap.Builder<PartitionId, List<ParticipantId>> preferenceLists =
        new ImmutableMap.Builder<PartitionId, List<ParticipantId>>();
    ImmutableMap.Builder<PartitionId, Map<ParticipantId, State>> preferenceMaps =
        new ImmutableMap.Builder<PartitionId, Map<ParticipantId, State>>();
    for (PartitionId partitionId : idealState.getPartitionSet()) {
      List<ParticipantId> preferenceList = idealState.getPreferenceList(partitionId);
      if (preferenceList != null) {
        preferenceLists.put(partitionId, ImmutableList.copyOf(preferenceList));
      }
      Map<ParticipantId, State> preferenceMap = idealState.getParticipantStateMap(partitionId);
      if (preferenceMap != null) {
        preferenceMaps.put(partitionId, ImmutableMap.copyOf(preferenceMap));
      }
    }
    _preferenceLists = preferenceLists.build();
    _preferenceMaps = preferenceMaps.build();

    // Leave the resource assignment as is
    _resourceAssignment = resourceAssignment;
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
    return _rebalancerMode;
  }

  /**
   * Get the rebalancer class name
   * @return rebalancer class name or null if not exist
   */
  public RebalancerRef getRebalancerRef() {
    return _rebalancerRef;
  }

  /**
   * Get state model definition name of the resource
   * @return state model definition
   */
  public StateModelDefId getStateModelDefId() {
    return _stateModelDefId;
  }

  /**
   * Get the ideal node and state assignment of the resource
   * @return resource assignment
   */
  public ResourceAssignment getResourceAssignment() {
    return _resourceAssignment;
  }

  /**
   * Get the preference list of participants for a given partition
   * @param partitionId the partition to look up
   * @return the ordered preference list (early entries are more preferred)
   */
  public List<ParticipantId> getPreferenceList(PartitionId partitionId) {
    if (_preferenceLists.containsKey(partitionId)) {
      return _preferenceLists.get(partitionId);
    }
    return Collections.emptyList();
  }

  /**
   * Get the preference map of participants and states for a given partition
   * @param partitionId the partition to look up
   * @return a mapping of participant to state for each replica
   */
  public Map<ParticipantId, State> getPreferenceMap(PartitionId partitionId) {
    if (_preferenceMaps.containsKey(partitionId)) {
      return _preferenceMaps.get(partitionId);
    }
    return Collections.emptyMap();
  }

  /**
   * Get the number of replicas each partition should have
   * @return replica count
   */
  public int getReplicaCount() {
    return _replicaCount;
  }

  /**
   * Get the number of partitions of this resource that a given participant can accept
   * @return maximum number of partitions
   */
  public int getMaxPartitionsPerParticipant() {
    return _maxPartitionsPerParticipant;
  }

  /**
   * Get the tag, if any, which must be present on assignable instances
   * @return group tag
   */
  public String getParticipantGroupTag() {
    return _participantGroupTag;
  }

  /**
   * Get state model factory id
   * @return state model factory id
   */
  public StateModelFactoryId getStateModelFactoryId() {
    return _stateModelFactoryId;
  }

  /**
   * Check if replicas can be assigned to any live participant
   * @return true if they can, false if they cannot
   */
  public boolean canAssignAnyLiveParticipant() {
    return _anyLiveParticipant;
  }

  /**
   * Assembles a RebalancerConfig
   */
  public static class Builder {
    private final ResourceId _id;
    private final IdealState _idealState;
    private boolean _anyLiveParticipant;
    private ResourceAssignment _resourceAssignment;
    private final Map<PartitionId, Partition> _partitionMap;

    /**
     * Configure the rebalancer for a resource
     * @param resourceId the resource to rebalance
     */
    public Builder(ResourceId resourceId) {
      _id = resourceId;
      _idealState = new IdealState(resourceId);
      _anyLiveParticipant = false;
      _partitionMap = new HashMap<PartitionId, Partition>();
    }

    /**
     * Set the rebalancer mode
     * @param mode {@link RebalanceMode}
     */
    public Builder rebalancerMode(RebalanceMode mode) {
      _idealState.setRebalanceMode(mode);
      return this;
    }

    /**
     * Set a user-defined rebalancer
     * @param rebalancerRef a reference to the rebalancer
     * @return Builder
     */
    public Builder rebalancer(RebalancerRef rebalancerRef) {
      _idealState.setRebalancerRef(rebalancerRef);
      return this;
    }

    /**
     * Set the state model definition
     * @param stateModelDefId state model identifier
     * @return Builder
     */
    public Builder stateModelDef(StateModelDefId stateModelDefId) {
      _idealState.setStateModelDefId(stateModelDefId);
      return this;
    }

    /**
     * Set the full assignment of partitions to nodes and corresponding states
     * @param resourceAssignment resource assignment
     * @return Builder
     */
    public Builder resourceAssignment(ResourceAssignment resourceAssignment) {
      _resourceAssignment = resourceAssignment;
      return this;
    }

    /**
     * Set the number of replicas
     * @param replicaCount number of replicas
     * @return Builder
     */
    public Builder replicaCount(int replicaCount) {
      _idealState.setReplicas(Integer.toString(replicaCount));
      return this;
    }

    /**
     * Set the maximum number of partitions to assign to any participant
     * @param maxPartitions
     * @return Builder
     */
    public Builder maxPartitionsPerParticipant(int maxPartitions) {
      _idealState.setMaxPartitionsPerInstance(maxPartitions);
      return this;
    }

    /**
     * Set state model factory
     * @param stateModelFactoryId
     * @return Builder
     */
    public Builder stateModelFactoryId(StateModelFactoryId stateModelFactoryId) {
      _idealState.setStateModelFactoryId(stateModelFactoryId);
      return this;
    }

    /**
     * Set whether any live participant should be used in rebalancing
     * @param useAnyParticipant true if any live participant can be used, false otherwise
     * @return
     */
    public Builder anyLiveParticipant(boolean useAnyParticipant) {
      _anyLiveParticipant = true;
      return this;
    }

    /**
     * Add a partition that the resource serves
     * @param partition fully-qualified partition
     * @return Builder
     */
    public Builder addPartition(Partition partition) {
      _partitionMap.put(partition.getId(), partition);
      return this;
    }

    /**
     * Add a collection of partitions
     * @param partitions
     * @return Builder
     */
    public Builder addPartitions(Collection<Partition> partitions) {
      for (Partition partition : partitions) {
        addPartition(partition);
      }
      return this;
    }

    /**
     * Add a specified number of partitions with a default naming scheme, namely
     * resourceId_partitionNumber where partitionNumber starts at 0
     * These partitions are added without any user configuration properties
     * @param partitionCount number of partitions to add
     * @return Builder
     */
    public Builder addPartitions(int partitionCount) {
      for (int i = 0; i < partitionCount; i++) {
        addPartition(new Partition(Id.partition(_id, Integer.toString(i)), null));
      }
      return this;
    }

    /**
     * Assemble a RebalancerConfig
     * @return a fully defined rebalancer configuration
     */
    public RebalancerConfig build() {
      // add a single partition if one hasn't been added yet since 1 partition is default
      if (_partitionMap.isEmpty()) {
        addPartitions(1);
      }
      if (_anyLiveParticipant) {
        return new RebalancerConfig(_partitionMap, _idealState, _resourceAssignment,
            Integer.parseInt(_idealState.getReplicas()));
      } else {
        return new RebalancerConfig(_partitionMap, _idealState, _resourceAssignment, -1);
      }
    }
  }
}
