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

import java.util.List;
import java.util.Map;

import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Captures the configuration properties necessary for rebalancing
 */
public class RebalancerConfig {
  private final RebalanceMode _rebalancerMode;
  private final RebalancerRef _rebalancerRef;
  private final StateModelDefId _stateModelDefId;
  private final Map<PartitionId, List<ParticipantId>> _preferenceLists;
  private final Map<PartitionId, Map<ParticipantId, State>> _preferenceMaps;
  private final ResourceAssignment _resourceAssignment;
  private final int _replicaCount;
  private final String _participantGroupTag;
  private final int _maxPartitionsPerParticipant;
  private final int _bucketSize;
  private final boolean _batchMessageMode;
  private final StateModelFactoryId _stateModelFactoryId;

  /**
   * Instantiate the configuration of a rebalance task
   * @param idealState the physical ideal state
   * @param resourceAssignment last mapping of a resource
   */
  public RebalancerConfig(IdealState idealState, ResourceAssignment resourceAssignment) {
    _rebalancerMode = idealState.getRebalanceMode();
    _rebalancerRef = idealState.getRebalancerRef();
    _stateModelDefId = idealState.getStateModelDefId();
    _replicaCount = Integer.parseInt(idealState.getReplicas());
    _participantGroupTag = idealState.getInstanceGroupTag();
    _maxPartitionsPerParticipant = idealState.getMaxPartitionsPerInstance();
    _bucketSize = idealState.getBucketSize();
    _batchMessageMode = idealState.getBatchMessageMode();
    _stateModelFactoryId = idealState.getStateModelFactoryId();

    // Build preference lists and maps
    ImmutableMap.Builder<PartitionId, List<ParticipantId>> preferenceLists =
        new ImmutableMap.Builder<PartitionId, List<ParticipantId>>();
    ImmutableMap.Builder<PartitionId, Map<ParticipantId, State>> preferenceMaps =
        new ImmutableMap.Builder<PartitionId, Map<ParticipantId, State>>();
    for (PartitionId partitionId : idealState.getPartitionSet()) {
      preferenceLists.put(partitionId,
          ImmutableList.copyOf(idealState.getPreferenceList(partitionId)));
      preferenceMaps.put(partitionId,
          ImmutableMap.copyOf(idealState.getParticipantStateMap(partitionId)));
    }
    _preferenceLists = preferenceLists.build();
    _preferenceMaps = preferenceMaps.build();

    // Leave the resource assignment as is
    _resourceAssignment = resourceAssignment;
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
    return _preferenceLists.get(partitionId);
  }

  /**
   * Get the preference map of participants and states for a given partition
   * @param partitionId the partition to look up
   * @return a mapping of participant to state for each replica
   */
  public Map<ParticipantId, State> getPreferenceMap(PartitionId partitionId) {
    return _preferenceMaps.get(partitionId);
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
   * Get bucket size
   * @return bucket size
   */
  public int getBucketSize() {
    return _bucketSize;
  }

  /**
   * Get batch message mode
   * @return true if in batch message mode, false otherwise
   */
  public boolean getBatchMessageMode() {
    return _batchMessageMode;
  }

  /**
   * Get state model factory id
   * @return state model factory id
   */
  public StateModelFactoryId getStateModelFactoryId() {
    return _stateModelFactoryId;
  }

  /**
   * Assembles a RebalancerConfig
   */
  public static class Builder {
    private final IdealState _idealState;
    private ResourceAssignment _resourceAssignment;

    /**
     * Configure the rebalancer for a resource
     * @param resourceId the resource to rebalance
     */
    public Builder(ResourceId resourceId) {
      _idealState = new IdealState(resourceId);
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
     * Set bucket size
     * @param bucketSize
     * @return Builder
     */
    public Builder bucketSize(int bucketSize) {
      _idealState.setBucketSize(bucketSize);
      return this;
    }

    /**
     * Set batch message mode
     * @param batchMessageMode
     * @return Builder
     */
    public Builder batchMessageMode(boolean batchMessageMode) {
      _idealState.setBatchMessageMode(batchMessageMode);
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
     * Assemble a RebalancerConfig
     * @return a fully defined rebalancer configuration
     */
    public RebalancerConfig build() {
      return new RebalancerConfig(_idealState, _resourceAssignment);
    }
  }
}
