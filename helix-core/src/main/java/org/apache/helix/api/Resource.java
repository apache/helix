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

import java.util.HashSet;
import java.util.Set;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.ImmutableSet;

/**
 * Represent a resource entity in helix cluster
 */
public class Resource {
  private final ResourceId _id;
  private final RebalancerConfig _rebalancerConfig;

  private final Set<Partition> _partitionSet;

  private final ExternalView _externalView;
  private final ExternalView _pendingExternalView;

  // TODO move construct logic to ResourceAccessor
  /**
   * Construct a resource
   * @param idealState
   * @param currentStateMap map of participant-id to current state
   */
  public Resource(ResourceId id, IdealState idealState, ResourceAssignment rscAssignment) {
    _id = id;
    // _rebalancerMode = idealState.getRebalanceMode();
    // _rebalancerRef = new RebalancerRef(idealState.getRebalancerClassName());
    // _stateModelDefId = new StateModelDefId(idealState.getStateModelDefRef());
    _rebalancerConfig = null;

    Set<Partition> partitionSet = new HashSet<Partition>();
    for (PartitionId partitionId : idealState.getPartitionSet()) {
      partitionSet.add(new Partition(partitionId));
    }
    _partitionSet = ImmutableSet.copyOf(partitionSet);

    // TODO
    // _resourceAssignment = null;

    _externalView = null;
    _pendingExternalView = null; // TODO: stub
  }

  /**
   * Construct a Resource
   * @param id resource identifier
   * @param partitionSet disjoint partitions of the resource
   * @param externalView external view of the resource
   * @param pendingExternalView pending external view based on unprocessed messages
   * @param rebalancerConfig configuration properties for rebalancing this resource
   */
  public Resource(ResourceId id, Set<Partition> partitionSet, ExternalView externalView,
      ExternalView pendingExternalView, RebalancerConfig rebalancerConfig) {
    _id = id;
    _partitionSet = ImmutableSet.copyOf(partitionSet);
    _externalView = externalView;
    _pendingExternalView = pendingExternalView;
    _rebalancerConfig = rebalancerConfig;
  }

  /**
   * Get the set of partitions of the resource
   * @return set of partitions or empty set if none
   */
  public Set<Partition> getPartitionSet() {
    return _partitionSet;
  }

  /**
   * Get the external view of the resource
   * @return the external view of the resource
   */
  public ExternalView getExternalView() {
    return _externalView;
  }

  /**
   * Get the pending external view of the resource based on unprocessed messages
   * @return the external view of the resource
   */
  public ExternalView getPendingExternalView() {
    return _pendingExternalView;
  }

  public RebalancerConfig getRebalancerConfig() {
    return _rebalancerConfig;
  }

  public ResourceId getId() {
    return _id;
  }

  /**
   * Assembles a Resource
   */
  public static class Builder {
    private final ResourceId _id;
    private final Set<Partition> _partitionSet;
    private ExternalView _externalView;
    private ExternalView _pendingExternalView;
    private RebalancerConfig _rebalancerConfig;

    /**
     * Build a Resource with an id
     * @param id resource id
     */
    public Builder(ResourceId id) {
      _id = id;
      _partitionSet = new HashSet<Partition>();
    }

    /**
     * Add a partition that the resource serves
     * @param partition fully-qualified partition
     * @return Builder
     */
    public Builder addPartition(Partition partition) {
      _partitionSet.add(partition);
      return this;
    }

    /**
     * Set the external view of this resource
     * @param extView currently served replica placement and state
     * @return Builder
     */
    public Builder externalView(ExternalView extView) {
      _externalView = extView;
      return this;
    }

    /**
     * Set the pending external view of this resource
     * @param extView replica placements as a result of pending messages
     * @return Builder
     */
    public Builder pendingExternalView(ExternalView pendingExtView) {
      _pendingExternalView = pendingExtView;
      return this;
    }

    /**
     * Set the rebalancer configuration
     * @param rebalancerConfig properties of interest for rebalancing
     * @return Builder
     */
    public Builder rebalancerConfig(RebalancerConfig rebalancerConfig) {
      _rebalancerConfig = rebalancerConfig;
      return this;
    }

    /**
     * Create a Resource object
     * @return instantiated Resource
     */
    public Resource build() {
      return new Resource(_id, _partitionSet, _externalView, _pendingExternalView,
          _rebalancerConfig);
    }
  }
}
