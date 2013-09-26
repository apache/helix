package org.apache.helix.controller.rebalancer.context;

import java.util.List;
import java.util.Map;

import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

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
 * RebalancerContext for SEMI_AUTO rebalancer mode. It indicates the preferred locations of each
 * partition replica. By default, it corresponds to {@link SemiAutoRebalancer}
 */
public final class SemiAutoRebalancerContext extends PartitionedRebalancerContext {
  @JsonProperty("preferenceLists")
  private Map<PartitionId, List<ParticipantId>> _preferenceLists;

  /**
   * Instantiate a SemiAutoRebalancerContext
   */
  public SemiAutoRebalancerContext() {
    super(RebalanceMode.SEMI_AUTO);
    setRebalancerRef(RebalancerRef.from(SemiAutoRebalancer.class));
    _preferenceLists = Maps.newHashMap();
  }

  /**
   * Get the preference lists of all partitions of the resource
   * @return map of partition id to list of participant ids
   */
  public Map<PartitionId, List<ParticipantId>> getPreferenceLists() {
    return _preferenceLists;
  }

  /**
   * Set the preference lists of all partitions of the resource
   * @param preferenceLists
   */
  public void setPreferenceLists(Map<PartitionId, List<ParticipantId>> preferenceLists) {
    _preferenceLists = preferenceLists;
  }

  /**
   * Get the preference list of a partition
   * @param partitionId the partition to look up
   * @return list of participant ids
   */
  @JsonIgnore
  public List<ParticipantId> getPreferenceList(PartitionId partitionId) {
    return _preferenceLists.get(partitionId);
  }

  /**
   * Build a SemiAutoRebalancerContext. By default, it corresponds to {@link SemiAutoRebalancer}
   */
  public static final class Builder extends PartitionedRebalancerContext.AbstractBuilder<Builder> {
    private final Map<PartitionId, List<ParticipantId>> _preferenceLists;

    /**
     * Instantiate for a resource
     * @param resourceId resource id
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
      super.rebalancerRef(RebalancerRef.from(SemiAutoRebalancer.class));
      _preferenceLists = Maps.newHashMap();
    }

    /**
     * Add a preference list for a partition
     * @param partitionId partition to set
     * @param preferenceList ordered list of participants who can serve the partition
     * @return Builder
     */
    public Builder preferenceList(PartitionId partitionId, List<ParticipantId> preferenceList) {
      _preferenceLists.put(partitionId, preferenceList);
      return self();
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public SemiAutoRebalancerContext build() {
      SemiAutoRebalancerContext context = new SemiAutoRebalancerContext();
      super.update(context);
      context.setPreferenceLists(_preferenceLists);
      return context;
    }
  }
}
