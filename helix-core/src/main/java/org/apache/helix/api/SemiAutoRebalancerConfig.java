package org.apache.helix.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
 * Configuration properties for the SEMI_AUTO rebalancer
 */
public final class SemiAutoRebalancerConfig extends RebalancerConfig {
  /**
   * Instantiate a new config for SEMI_AUTO
   * @param resourceId the resource to rebalance
   * @param stateModelDefId the state model that the resource follows
   * @param partitionMap map of partition id to partition
   */
  public SemiAutoRebalancerConfig(ResourceId resourceId, StateModelDefId stateModelDefId,
      Map<PartitionId, Partition> partitionMap) {
    super(resourceId, RebalanceMode.SEMI_AUTO, stateModelDefId, partitionMap);
  }

  /**
   * Instantiate from a base RebalancerConfig
   * @param config populated rebalancer config
   */
  private SemiAutoRebalancerConfig(RebalancerConfig config) {
    super(config);
  }

  /**
   * Get the preference list for a partition
   * @param partitionId the partition to look up
   * @return preference list ordered from most preferred to least preferred
   */
  public List<ParticipantId> getPreferenceList(PartitionId partitionId) {
    List<String> rawPreferenceList = getListField(partitionId.stringify());
    if (rawPreferenceList != null) {
      return IdealState.preferenceListFromStringList(rawPreferenceList);
    }
    return Collections.emptyList();
  }

  /**
   * Set the preference list for a partition
   * @param partitionId the partition to set
   * @param preferenceList preference list ordered from most preferred to least preferred
   */
  public void setPreferenceList(PartitionId partitionId, List<ParticipantId> preferenceList) {
    setListField(partitionId.toString(), IdealState.stringListFromPreferenceList(preferenceList));
  }

  /**
   * Get all the preference lists for a partition
   * @return map of partition id to list of participants ordered from most preferred to least
   *         preferred
   */
  public Map<PartitionId, List<ParticipantId>> getPreferenceLists() {
    Map<String, List<String>> rawPreferenceLists = getListFields();
    return IdealState.preferenceListsFromStringLists(rawPreferenceLists);
  }

  /**
   * Set all the preference lists for a partition
   * @param preferenceLists map of partition id to list of participants ordered from most preferred
   *          to least preferred
   */
  public void setPreferenceLists(Map<PartitionId, List<ParticipantId>> preferenceLists) {
    setListFields(IdealState.stringListsFromPreferenceLists(preferenceLists));
  }

  /**
   * Get a SemiAutoRebalancerConfig from a RebalancerConfig
   * @param config populated RebalancerConfig
   * @return SemiAutoRebalancerConfig
   */
  public static SemiAutoRebalancerConfig from(RebalancerConfig config) {
    return new SemiAutoRebalancerConfig(config);
  }

  /**
   * Assembler for a SEMI_AUTO configuration
   */
  public static class Builder extends RebalancerConfig.Builder<Builder> {
    private final Map<PartitionId, List<ParticipantId>> _preferenceLists;

    /**
     * Build for a specific resource
     * @param resourceId the resource to rebalance
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
      _preferenceLists = new HashMap<PartitionId, List<ParticipantId>>();
    }

    /**
     * Construct a builder from an existing semi-auto rebalancer config
     * @param config
     */
    public Builder(SemiAutoRebalancerConfig config) {
    	super(config);
        _preferenceLists = new HashMap<PartitionId, List<ParticipantId>>();
        _preferenceLists.putAll(config.getPreferenceLists());
    }
    
    /**
     * Add a preference list of a partition
     * @param partitionId the partition to set
     * @param preferenceList list of participant ids, most preferred first
     * @return Builder
     */
    public Builder preferenceList(PartitionId partitionId, List<ParticipantId> preferenceList) {
      _preferenceLists.put(partitionId, preferenceList);
      return this;
    }

    @Override
    public SemiAutoRebalancerConfig build() {
      if (_partitionMap.isEmpty()) {
        addPartitions(1);
      }
      SemiAutoRebalancerConfig config =
          new SemiAutoRebalancerConfig(_resourceId, _stateModelDefId, _partitionMap);
      update(config);
      config.setPreferenceLists(_preferenceLists);
      return config;
    }

    @Override
    protected Builder self() {
      return this;
    }
  }
}
