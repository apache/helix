package org.apache.helix.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;

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
 * Configuration properties for the CUSTOMIZED rebalancer
 */
public final class CustomRebalancerConfig extends RebalancerConfig {
  /**
   * Instantiate a new config for CUSTOMIZED
   * @param resourceId the resource to rebalance
   * @param stateModelDefId the state model that the resource follows
   * @param partitionMap map of partition id to partition
   */
  public CustomRebalancerConfig(ResourceId resourceId, StateModelDefId stateModelDefId,
      Map<PartitionId, Partition> partitionMap) {
    super(resourceId, RebalanceMode.CUSTOMIZED, stateModelDefId, partitionMap);
  }

  /**
   * Instantiate from a base RebalancerConfig
   * @param config populated rebalancer config
   */
  private CustomRebalancerConfig(RebalancerConfig config) {
    super(config);
  }

  /**
   * Get the preference map for a partition
   * @param partitionId the partition to look up
   * @return preference map of participant to state for each replica
   */
  public Map<ParticipantId, State> getPreferenceMap(PartitionId partitionId) {
    Map<String, String> rawPreferenceMap = getMapField(partitionId.stringify());
    if (rawPreferenceMap != null) {
      return IdealState.participantStateMapFromStringMap(rawPreferenceMap);
    }
    return Collections.emptyMap();
  }

  /**
   * Set the preference map for a partition
   * @param partitionId the partition to set
   * @param preferenceMap map of participant to state for each replica
   */
  public void setPreferenceMap(PartitionId partitionId, Map<ParticipantId, State> preferenceMap) {
    setMapField(partitionId.toString(), IdealState.stringMapFromParticipantStateMap(preferenceMap));
  }

  /**
   * Get all the preference maps for a partition
   * @return map of partition id to map of participant id to state for each replica
   */
  public Map<PartitionId, Map<ParticipantId, State>> getPreferenceMaps() {
    Map<String, Map<String, String>> rawPreferenceMaps = getMapFields();
    return IdealState.participantStateMapsFromStringMaps(rawPreferenceMaps);
  }

  /**
   * Set all the preference maps for a partition
   * @param preferenceMaps map of partition id to map of participant id to state for each replica
   */
  public void setPreferenceMaps(Map<PartitionId, Map<ParticipantId, State>> preferenceMaps) {
    setMapFields(IdealState.stringMapsFromParticipantStateMaps(preferenceMaps));
  }

  /**
   * Get a CustomRebalancerConfig from a RebalancerConfig
   * @param config populated RebalancerConfig
   * @return CustomRebalancerConfig
   */
  public static CustomRebalancerConfig from(RebalancerConfig config) {
    return new CustomRebalancerConfig(config);
  }

  /**
   * Assembler for a CUSTOMIZED configuration
   */
  public static class Builder extends RebalancerConfig.Builder<Builder> {
    private final Map<PartitionId, Map<ParticipantId, State>> _preferenceMaps;

    /**
     * Build for a specific resource
     * @param resourceId the resource to rebalance
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
      _preferenceMaps = new HashMap<PartitionId, Map<ParticipantId, State>>();
    }

		/**
		 * Construct builder using an existing custom rebalancer config
		 * @param config
		 */
		public Builder(CustomRebalancerConfig config) {
			super(config);
			_preferenceMaps = new HashMap<PartitionId, Map<ParticipantId, State>>();
			_preferenceMaps.putAll(config.getPreferenceMaps());
		}

    /**
     * Add a preference map of a partition
     * @param partitionId the partition to set
     * @param preferenceMap map of participant id to state
     * @return Builder
     */
    public Builder preferenceMap(PartitionId partitionId, Map<ParticipantId, State> preferenceMap) {
      _preferenceMaps.put(partitionId, preferenceMap);
      return this;
    }

    @Override
    public CustomRebalancerConfig build() {
      if (_partitionMap.isEmpty()) {
        addPartitions(1);
      }
      CustomRebalancerConfig config =
          new CustomRebalancerConfig(_resourceId, _stateModelDefId, _partitionMap);
      update(config);
      config.setPreferenceMaps(_preferenceMaps);
      return config;
    }

    @Override
    protected Builder self() {
      return this;
    }
  }
}
