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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;

public class RebalancerConfig {
  private final RebalanceMode _rebalancerMode;
  private final RebalancerRef _rebalancerRef;
  private final StateModelDefId _stateModelDefId;
  private final Map<PartitionId, List<ParticipantId>> _preferenceLists;
  private final ResourceAssignment _resourceAssignment;
  private final int _replicaCount;
  private final String _participantGroupTag;
  private final int _maxPartitionsPerParticipant;

  public RebalancerConfig(RebalanceMode mode, RebalancerRef rebalancerRef,
      StateModelDefId stateModelDefId, ResourceAssignment resourceAssignment) {
    _rebalancerMode = mode;
    _rebalancerRef = rebalancerRef;
    _stateModelDefId = stateModelDefId;
    _resourceAssignment = resourceAssignment;
    _preferenceLists = Collections.emptyMap(); // TODO: stub
    _replicaCount = 0; // TODO: stub
    _participantGroupTag = null; // TODO: stub
    _maxPartitionsPerParticipant = Integer.MAX_VALUE; // TODO: stub
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
   * Assembles a RebalancerConfig
   */
  public static class Builder {
    private RebalanceMode _mode = RebalanceMode.NONE;
    private RebalancerRef _rebalancerRef;
    private StateModelDefId _stateModelDefId;
    private ResourceAssignment _resourceAssignment;

    /**
     * Set the rebalancer mode
     * @param mode {@link RebalanceMode}
     */
    public Builder rebalancerMode(RebalanceMode mode) {
      _mode = mode;
      return this;
    }

    /**
     * Set a user-defined rebalancer
     * @param rebalancerRef a reference to the rebalancer
     * @return Builder
     */
    public Builder rebalancer(RebalancerRef rebalancerRef) {
      _rebalancerRef = rebalancerRef;
      return this;
    }

    /**
     * Set the state model definition
     * @param stateModelDefId state model identifier
     * @return Builder
     */
    public Builder stateModelDef(StateModelDefId stateModelDefId) {
      _stateModelDefId = stateModelDefId;
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
     * Assemble a RebalancerConfig
     * @return a fully defined rebalancer configuration
     */
    public RebalancerConfig build() {
      return new RebalancerConfig(_mode, _rebalancerRef, _stateModelDefId, _resourceAssignment);
    }
  }
}
