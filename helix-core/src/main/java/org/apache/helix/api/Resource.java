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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;

/**
 * Represent a resource entity in helix cluster
 */
public class Resource {
  private final ResourceConfig _config;
  private final ExternalView _externalView;

  /**
   * Construct a resource
   * @param id resource id
   * @param idealState ideal state of the resource
   * @param currentStateMap map of participant-id to current state
   * @param liveParticipantCount number of live participants in the system
   */
  public Resource(ResourceId id, IdealState idealState, ResourceAssignment resourceAssignment,
      ExternalView externalView, int liveParticipantCount) {
    Map<PartitionId, Partition> partitionMap = new HashMap<PartitionId, Partition>();
    Map<PartitionId, Map<String, String>> schedulerTaskConfigMap =
        new HashMap<PartitionId, Map<String, String>>();
    Map<String, Integer> transitionTimeoutMap = new HashMap<String, Integer>();
    for (PartitionId partitionId : idealState.getPartitionSet()) {
      partitionMap.put(partitionId, new Partition(partitionId));

      // TODO refactor it
      Map<String, String> taskConfigMap = idealState.getInstanceStateMap(partitionId.stringify());
      if (taskConfigMap != null) {
        schedulerTaskConfigMap.put(partitionId, taskConfigMap);
      }

      // TODO refactor it
      for (String simpleKey : idealState.getRecord().getSimpleFields().keySet()) {
        if (simpleKey.indexOf("_" + Message.Attributes.TIMEOUT) != -1) {
          try {
            int timeout = Integer.parseInt(idealState.getRecord().getSimpleField(simpleKey));
            transitionTimeoutMap.put(simpleKey, timeout);
          } catch (Exception e) {
            // ignore
          }
        }
      }
    }
    SchedulerTaskConfig schedulerTaskConfig =
        new SchedulerTaskConfig(transitionTimeoutMap, schedulerTaskConfigMap);
    RebalancerConfig rebalancerConfig =
        new RebalancerConfig(idealState, resourceAssignment, liveParticipantCount);

    _config = new ResourceConfig(id, partitionMap, schedulerTaskConfig, rebalancerConfig);
    _externalView = externalView;
  }

  /**
   * Get the partitions of the resource
   * @return map of partition id to partition or empty map if none
   */
  public Map<PartitionId, Partition> getPartitionMap() {
    return _config.getPartitionMap();
  }

  /**
   * Get a partition that the resource contains
   * @param partitionId the partition id to look up
   * @return Partition or null if none is present with the given id
   */
  public Partition getPartition(PartitionId partitionId) {
    return _config.getPartition(partitionId);
  }

  /**
   * Get the set of partition ids that the resource contains
   * @return partition id set, or empty if none
   */
  public Set<PartitionId> getPartitionSet() {
    return _config.getPartitionSet();
  }

  /**
   * Get the external view of the resource
   * @return the external view of the resource
   */
  public ExternalView getExternalView() {
    return _externalView;
  }

  /**
   * Get the resource properties configuring rebalancing
   * @return RebalancerConfig properties
   */
  public RebalancerConfig getRebalancerConfig() {
    return _config.getRebalancerConfig();
  }

  /**
   * Get the resource id
   * @return ResourceId
   */
  public ResourceId getId() {
    return _config.getId();
  }

  /**
   * Get the properties configuring scheduler tasks
   * @return SchedulerTaskConfig properties
   */
  public SchedulerTaskConfig getSchedulerTaskConfig() {
    return _config.getSchedulerTaskConfig();
  }

  /**
   * Get the configuration of this resource
   * @return ResourceConfig that backs this Resource
   */
  public ResourceConfig getConfig() {
    return _config;
  }
}
