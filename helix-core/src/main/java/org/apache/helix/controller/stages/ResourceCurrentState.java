package org.apache.helix.controller.stages;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.CurrentState;

import com.google.common.collect.Sets;

public class ResourceCurrentState {
  /**
   * map of resource-id to map of partition-id to map of participant-id to state
   * represent current-state for the participant
   */
  private final Map<ResourceId, Map<PartitionId, Map<ParticipantId, State>>> _currentStateMap;

  /**
   * map of resource-id to map of partition-id to map of participant-id to state
   * represent pending messages for the participant
   */
  private final Map<ResourceId, Map<PartitionId, Map<ParticipantId, State>>> _pendingStateMap;

  /**
   * map of resource-id to state model definition id
   */
  private final Map<ResourceId, StateModelDefId> _resourceStateModelMap;

  /**
   * map of resource-id to current-state config's
   */
  private final Map<ResourceId, CurrentState> _curStateMetaMap;

  /**
   * Contains per-resource maps of partition -> (instance, requested_state). This corresponds to the
   * REQUESTED_STATE field in the CURRENTSTATES node.
   */
  private final Map<ResourceId, Map<PartitionId, Map<ParticipantId, State>>> _requestedStateMap;

  /**
   * Contains per-resource maps of partition -> (instance, info). This corresponds to the INFO field
   * in the CURRENTSTATES node. This is information returned by state transition methods on the
   * participants. It may be used by the rebalancer.
   */
  private final Map<ResourceId, Map<PartitionId, Map<ParticipantId, String>>> _infoMap;

  /**
   * construct
   */
  public ResourceCurrentState() {
    _currentStateMap = new HashMap<ResourceId, Map<PartitionId, Map<ParticipantId, State>>>();
    _pendingStateMap = new HashMap<ResourceId, Map<PartitionId, Map<ParticipantId, State>>>();
    _resourceStateModelMap = new HashMap<ResourceId, StateModelDefId>();
    _curStateMetaMap = new HashMap<ResourceId, CurrentState>();

    _requestedStateMap = new HashMap<ResourceId, Map<PartitionId, Map<ParticipantId, State>>>();
    _infoMap = new HashMap<ResourceId, Map<PartitionId, Map<ParticipantId, String>>>();

  }

  /**
   * Get all the resources seen in the aggregated current state
   * @return set of ResourceId
   */
  public Set<ResourceId> getResourceIds() {
    Set<ResourceId> allResources = Sets.newHashSet();
    allResources.addAll(_currentStateMap.keySet());
    allResources.addAll(_pendingStateMap.keySet());
    return allResources;
  }

  /**
   * @param resourceId
   * @param stateModelDefId
   */
  public void setResourceStateModelDef(ResourceId resourceId, StateModelDefId stateModelDefId) {
    _resourceStateModelMap.put(resourceId, stateModelDefId);
  }

  /**
   * @param resourceId
   * @return
   */
  public StateModelDefId getResourceStateModelDef(ResourceId resourceId) {
    return _resourceStateModelMap.get(resourceId);
  }

  /**
   * @param resourceId
   * @param bucketSize
   */
  public void setBucketSize(ResourceId resourceId, int bucketSize) {
    CurrentState curStateMeta = _curStateMetaMap.get(resourceId);
    if (curStateMeta == null) {
      curStateMeta = new CurrentState(resourceId);
      _curStateMetaMap.put(resourceId, curStateMeta);
    }
    curStateMeta.setBucketSize(bucketSize);
  }

  /**
   * @param resourceId
   * @return
   */
  public int getBucketSize(ResourceId resourceId) {
    int bucketSize = 0;
    CurrentState curStateMeta = _curStateMetaMap.get(resourceId);
    if (curStateMeta != null) {
      bucketSize = curStateMeta.getBucketSize();
    }

    return bucketSize;
  }

  /**
   * @param stateMap
   * @param resourceId
   * @param partitionId
   * @param participantId
   * @param state
   */
  static void setStateMap(Map<ResourceId, Map<PartitionId, Map<ParticipantId, State>>> stateMap,
      ResourceId resourceId, PartitionId partitionId, ParticipantId participantId, State state) {
    if (!stateMap.containsKey(resourceId)) {
      stateMap.put(resourceId, new HashMap<PartitionId, Map<ParticipantId, State>>());
    }

    if (!stateMap.get(resourceId).containsKey(partitionId)) {
      stateMap.get(resourceId).put(partitionId, new HashMap<ParticipantId, State>());
    }
    stateMap.get(resourceId).get(partitionId).put(participantId, state);
  }

  /**
   * @param stateMap
   * @param resourceId
   * @param partitionId
   * @param participantId
   * @return state
   */
  static State getState(Map<ResourceId, Map<PartitionId, Map<ParticipantId, State>>> stateMap,
      ResourceId resourceId, PartitionId partitionId, ParticipantId participantId) {
    Map<PartitionId, Map<ParticipantId, State>> map = stateMap.get(resourceId);
    if (map != null) {
      Map<ParticipantId, State> instanceStateMap = map.get(partitionId);
      if (instanceStateMap != null) {
        return instanceStateMap.get(participantId);
      }
    }
    return null;

  }

  /**
   * @param stateMap
   * @param resourceId
   * @param partitionId
   * @return
   */
  static Map<ParticipantId, State> getStateMap(
      Map<ResourceId, Map<PartitionId, Map<ParticipantId, State>>> stateMap, ResourceId resourceId,
      PartitionId partitionId) {
    if (stateMap.containsKey(resourceId)) {
      Map<PartitionId, Map<ParticipantId, State>> map = stateMap.get(resourceId);
      if (map.containsKey(partitionId)) {
        return map.get(partitionId);
      }
    }
    return Collections.emptyMap();
  }

  /**
   * @param resourceId
   * @param partitionId
   * @param participantId
   * @param state
   */
  public void setCurrentState(ResourceId resourceId, PartitionId partitionId,
      ParticipantId participantId, State state) {
    setStateMap(_currentStateMap, resourceId, partitionId, participantId, state);
  }

  /**
   * @param resourceId
   * @param partitionId
   * @param participantId
   * @param state
   */
  public void setPendingState(ResourceId resourceId, PartitionId partitionId,
      ParticipantId participantId, State state) {
    setStateMap(_pendingStateMap, resourceId, partitionId, participantId, state);
  }

  /**
   * given (resource, partition, instance), returns currentState
   * @param resourceName
   * @param partition
   * @param instanceName
   * @return
   */
  public State getCurrentState(ResourceId resourceId, PartitionId partitionId,
      ParticipantId participantId) {
    return getState(_currentStateMap, resourceId, partitionId, participantId);
  }

  /**
   * given (resource, partition, instance), returns toState
   * @param resourceName
   * @param partition
   * @param instanceName
   * @return
   */
  public State getPendingState(ResourceId resourceId, PartitionId partitionId,
      ParticipantId participantId) {
    return getState(_pendingStateMap, resourceId, partitionId, participantId);
  }

  /**
   * @param resourceId
   * @param partitionId
   * @return
   */
  public Map<ParticipantId, State> getCurrentStateMap(ResourceId resourceId, PartitionId partitionId) {
    return getStateMap(_currentStateMap, resourceId, partitionId);
  }

  /**
   * Get the partitions mapped in the current state
   * @param resourceId resource to look up
   * @return set of mapped partitions, or empty set if there are none
   */
  public Set<PartitionId> getCurrentStateMappedPartitions(ResourceId resourceId) {
    Map<PartitionId, Map<ParticipantId, State>> currentStateMap = _currentStateMap.get(resourceId);
    Map<PartitionId, Map<ParticipantId, State>> pendingStateMap = _pendingStateMap.get(resourceId);
    Set<PartitionId> partitionSet = Sets.newHashSet();
    if (currentStateMap != null) {
      partitionSet.addAll(currentStateMap.keySet());
    }
    if (pendingStateMap != null) {
      partitionSet.addAll(pendingStateMap.keySet());
    }
    return partitionSet;
  }

  /**
   * @param resourceId
   * @param partitionId
   * @return
   */
  public Map<ParticipantId, State> getPendingStateMap(ResourceId resourceId, PartitionId partitionId) {
    return getStateMap(_pendingStateMap, resourceId, partitionId);

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("current state= ").append(_currentStateMap);
    sb.append(", pending state= ").append(_pendingStateMap);
    return sb.toString();

  }

  /**
   * @param resourceId
   * @param partitionId
   * @param participantId
   * @param state
   */
  public void setRequestedState(ResourceId resourceId, PartitionId partitionId,
      ParticipantId participantId, State state) {
    if (!_requestedStateMap.containsKey(resourceId)) {
      _requestedStateMap.put(resourceId, new HashMap<PartitionId, Map<ParticipantId, State>>());
    }
    if (!_requestedStateMap.get(resourceId).containsKey(partitionId)) {
      _requestedStateMap.get(resourceId).put(partitionId, new HashMap<ParticipantId, State>());
    }
    _requestedStateMap.get(resourceId).get(partitionId).put(participantId, state);
  }

  /**
   * @param resourceId
   * @param partitionId
   * @param participantId
   * @param state
   */
  public void setInfo(ResourceId resourceId, PartitionId partitionId, ParticipantId participantId,
      String info) {
    if (!_infoMap.containsKey(resourceId)) {
      _infoMap.put(resourceId, new HashMap<PartitionId, Map<ParticipantId, String>>());
    }
    if (!_infoMap.get(resourceId).containsKey(partitionId)) {
      _infoMap.get(resourceId).put(partitionId, new HashMap<ParticipantId, String>());
    }
    _infoMap.get(resourceId).get(partitionId).put(participantId, info);
  }

  /**
   * @param resourceId
   * @param partitionId
   * @param participantId
   * @return
   */
  public State getRequestedState(ResourceId resourceId, PartitionId partitionId,
      ParticipantId participantId) {
    Map<PartitionId, Map<ParticipantId, State>> map = _requestedStateMap.get(resourceId);
    if (map != null) {
      Map<ParticipantId, State> instanceStateMap = map.get(partitionId);
      if (instanceStateMap != null) {
        return instanceStateMap.get(participantId);
      }
    }
    return null;
  }

  /**
   * @param resourceId
   * @param partitionId
   * @param participantId
   * @return
   */
  public String getInfo(ResourceId resourceId, PartitionId partitionId, ParticipantId participantId) {
    Map<PartitionId, Map<ParticipantId, String>> map = _infoMap.get(resourceId);
    if (map != null) {
      Map<ParticipantId, String> instanceStateMap = map.get(partitionId);
      if (instanceStateMap != null) {
        return instanceStateMap.get(participantId);
      }
    }
    return null;
  }
}
