package org.apache.helix.controller.rebalancer.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.CustomRebalancer;
import org.apache.helix.controller.rebalancer.RebalancerRef;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.DefaultPlacementScheme;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.ReplicaPlacementScheme;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.codehaus.jackson.annotate.JsonIgnore;

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
 * RebalancerConfig for a resource that should be rebalanced in CUSTOMIZED mode. By default, it
 * corresponds to {@link CustomRebalancer}
 */
public class CustomRebalancerConfig extends PartitionedRebalancerConfig {
  private Map<PartitionId, Map<ParticipantId, State>> _preferenceMaps;

  /**
   * Instantiate a CustomRebalancerConfig
   */
  public CustomRebalancerConfig() {
    setRebalanceMode(RebalanceMode.CUSTOMIZED);
    setRebalancerRef(RebalancerRef.from(CustomRebalancer.class));
    _preferenceMaps = Maps.newHashMap();
  }

  /**
   * Get the preference maps of the partitions and replicas of the resource
   * @return map of partition to participant and state
   */
  public Map<PartitionId, Map<ParticipantId, State>> getPreferenceMaps() {
    return _preferenceMaps;
  }

  /**
   * Set the preference maps of the partitions and replicas of the resource
   * @param preferenceMaps map of partition to participant and state
   */
  public void setPreferenceMaps(Map<PartitionId, Map<ParticipantId, State>> preferenceMaps) {
    _preferenceMaps = preferenceMaps;
  }

  /**
   * Get the preference map of a partition
   * @param partitionId the partition to look up
   * @return map of participant to state
   */
  @JsonIgnore
  public Map<ParticipantId, State> getPreferenceMap(PartitionId partitionId) {
    return _preferenceMaps.get(partitionId);
  }

  /**
   * Generate preference maps based on a default cluster setup
   * @param stateModelDef the state model definition to follow
   * @param participantSet the set of participant ids to configure for
   */
  @Override
  @JsonIgnore
  public void generateDefaultConfiguration(StateModelDefinition stateModelDef,
      Set<ParticipantId> participantSet) {
    // compute default upper bounds
    Map<State, String> upperBounds = Maps.newHashMap();
    for (State state : stateModelDef.getTypedStatesPriorityList()) {
      upperBounds.put(state, stateModelDef.getNumParticipantsPerState(state));
    }

    // determine the current mapping
    Map<PartitionId, Map<ParticipantId, State>> currentMapping = getPreferenceMaps();

    // determine the preference maps
    LinkedHashMap<State, Integer> stateCounts =
        ConstraintBasedAssignment.stateCount(upperBounds, stateModelDef, participantSet.size(),
            getReplicaCount());
    ReplicaPlacementScheme placementScheme = new DefaultPlacementScheme();
    List<ParticipantId> participantList = new ArrayList<ParticipantId>(participantSet);
    List<PartitionId> partitionList = new ArrayList<PartitionId>(getPartitionSet());
    AutoRebalanceStrategy strategy =
        new AutoRebalanceStrategy(ResourceId.from(""), partitionList, stateCounts,
            getMaxPartitionsPerParticipant(), placementScheme);
    Map<String, Map<String, String>> rawPreferenceMaps =
        strategy.typedComputePartitionAssignment(participantList, currentMapping, participantList)
            .getMapFields();
    Map<PartitionId, Map<ParticipantId, State>> preferenceMaps =
        Maps.newHashMap(ResourceAssignment.replicaMapsFromStringMaps(rawPreferenceMaps));
    setPreferenceMaps(preferenceMaps);
  }

  /**
   * Build a CustomRebalancerConfig. By default, it corresponds to {@link CustomRebalancer}
   */
  public static final class Builder extends PartitionedRebalancerConfig.AbstractBuilder<Builder> {
    private final Map<PartitionId, Map<ParticipantId, State>> _preferenceMaps;

    /**
     * Instantiate for a resource
     * @param resourceId resource id
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
      super.rebalancerRef(RebalancerRef.from(CustomRebalancer.class));
      super.rebalanceMode(RebalanceMode.CUSTOMIZED);
      _preferenceMaps = Maps.newHashMap();
    }

    /**
     * Add a preference map for a partition
     * @param partitionId partition to set
     * @param preferenceList map of participant id to state indicating where replicas are served
     * @return Builder
     */
    public Builder preferenceMap(PartitionId partitionId, Map<ParticipantId, State> preferenceMap) {
      _preferenceMaps.put(partitionId, preferenceMap);
      return self();
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public CustomRebalancerConfig build() {
      CustomRebalancerConfig config = new CustomRebalancerConfig();
      super.update(config);
      config.setPreferenceMaps(_preferenceMaps);
      return config;
    }
  }
}
