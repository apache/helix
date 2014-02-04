package org.apache.helix.controller.rebalancer.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.RebalancerRef;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.DefaultPlacementScheme;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.ReplicaPlacementScheme;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
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
 * RebalancerConfig for SEMI_AUTO rebalancer mode. It indicates the preferred locations of each
 * partition replica. By default, it corresponds to {@link SemiAutoRebalancer}
 */
public final class SemiAutoRebalancerConfig extends PartitionedRebalancerConfig {
  @JsonProperty("preferenceLists")
  private Map<PartitionId, List<ParticipantId>> _preferenceLists;

  /**
   * Instantiate a SemiAutoRebalancerConfig
   */
  public SemiAutoRebalancerConfig() {
    if (getClass().equals(SemiAutoRebalancerConfig.class)) {
      // only mark this as semi auto mode if this specifc config is used
      setRebalanceMode(RebalanceMode.SEMI_AUTO);
    } else {
      setRebalanceMode(RebalanceMode.USER_DEFINED);
    }
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
   * Generate preference lists based on a default cluster setup
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
    Map<PartitionId, Map<ParticipantId, State>> currentMapping = Maps.newHashMap();
    for (PartitionId partitionId : getPartitionSet()) {
      List<ParticipantId> preferenceList = getPreferenceList(partitionId);
      if (preferenceList != null && !preferenceList.isEmpty()) {
        Set<ParticipantId> disabledParticipants = Collections.emptySet();
        Map<ParticipantId, State> emptyCurrentState = Collections.emptyMap();
        Map<ParticipantId, State> initialMap =
            ConstraintBasedAssignment.computeAutoBestStateForPartition(upperBounds, participantSet,
                stateModelDef, preferenceList, emptyCurrentState, disabledParticipants);
        currentMapping.put(partitionId, initialMap);
      }
    }

    // determine the preference
    LinkedHashMap<State, Integer> stateCounts =
        ConstraintBasedAssignment.stateCount(upperBounds, stateModelDef, participantSet.size(),
            getReplicaCount());
    ReplicaPlacementScheme placementScheme = new DefaultPlacementScheme();
    List<ParticipantId> participantList = new ArrayList<ParticipantId>(participantSet);
    List<PartitionId> partitionList = new ArrayList<PartitionId>(getPartitionSet());
    AutoRebalanceStrategy strategy =
        new AutoRebalanceStrategy(ResourceId.from(""), partitionList, stateCounts,
            getMaxPartitionsPerParticipant(), placementScheme);
    Map<String, List<String>> rawPreferenceLists =
        strategy.typedComputePartitionAssignment(participantList, currentMapping, participantList)
            .getListFields();
    Map<PartitionId, List<ParticipantId>> preferenceLists =
        Maps.newHashMap(IdealState.preferenceListsFromStringLists(rawPreferenceLists));
    setPreferenceLists(preferenceLists);
  }

  /**
   * Build a SemiAutoRebalancerConfig. By default, it corresponds to {@link SemiAutoRebalancer}
   */
  public static final class Builder extends PartitionedRebalancerConfig.AbstractBuilder<Builder> {
    private final Map<PartitionId, List<ParticipantId>> _preferenceLists;

    /**
     * Instantiate for a resource
     * @param resourceId resource id
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
      super.rebalancerRef(RebalancerRef.from(SemiAutoRebalancer.class));
      super.rebalanceMode(RebalanceMode.SEMI_AUTO);
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
    public SemiAutoRebalancerConfig build() {
      SemiAutoRebalancerConfig config = new SemiAutoRebalancerConfig();
      super.update(config);
      config.setPreferenceLists(_preferenceLists);
      return config;
    }
  }
}
