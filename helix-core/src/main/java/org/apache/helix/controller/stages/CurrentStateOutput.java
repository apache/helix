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
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;

import com.google.common.collect.Sets;

/**
 * The current state includes both current state and pending messages
 * For pending messages, we consider both toState and fromState
 * Pending message prevents controller sending transitions that may potentially violate state
 * constraints @see HELIX-541
 */
public class CurrentStateOutput {
  private final Map<String, Map<Partition, Map<String, String>>> _currentStateMap;
  private final Map<String, Map<Partition, Map<String, Message>>> _pendingStateMap;
  private final Map<String, Map<Partition, Map<String, Message>>> _cancellationStateMap;

  // resourceName -> (Partition -> (instanceName -> endTime))
  // Note that startTime / endTime in CurrentState marks that of state transition
  // and therefore endTime is the starting timestamp of the partition being in the
  // current state
  private final Map<String, Map<Partition, Map<String, Long>>> _currentStateEndTimeMap;

  // Contains per-resource maps of partition -> (instance, requested_state). This corresponds to the
  // REQUESTED_STATE
  // field in the CURRENTSTATES node.
  private final Map<String, Map<Partition, Map<String, String>>> _requestedStateMap;
  // Contains per-resource maps of partition -> (instance, info). This corresponds to the INFO field
  // in the
  // CURRENTSTATES node. This is information returned by state transition methods on the
  // participants. It may be used
  // by the rebalancer.
  private final Map<String, Map<Partition, Map<String, String>>> _infoMap;
  private final Map<String, String> _resourceStateModelMap;
  private final Map<String, CurrentState> _curStateMetaMap;

  public CurrentStateOutput() {
    _currentStateMap = new HashMap<>();
    _pendingStateMap = new HashMap<>();
    _cancellationStateMap = new HashMap<>();
    _currentStateEndTimeMap = new HashMap<>();
    _resourceStateModelMap = new HashMap<>();
    _curStateMetaMap = new HashMap<>();
    _requestedStateMap = new HashMap<>();
    _infoMap = new HashMap<>();
  }

  public void setResourceStateModelDef(String resourceName, String stateModelDefName) {
    _resourceStateModelMap.put(resourceName, stateModelDefName);
  }

  public String getResourceStateModelDef(String resourceName) {
    return _resourceStateModelMap.get(resourceName);
  }

  public void setBucketSize(String resource, int bucketSize) {
    CurrentState curStateMeta = _curStateMetaMap.get(resource);
    if (curStateMeta == null) {
      curStateMeta = new CurrentState(resource);
      _curStateMetaMap.put(resource, curStateMeta);
    }
    curStateMeta.setBucketSize(bucketSize);
  }

  public int getBucketSize(String resource) {
    int bucketSize = 0;
    CurrentState curStateMeta = _curStateMetaMap.get(resource);
    if (curStateMeta != null) {
      bucketSize = curStateMeta.getBucketSize();
    }

    return bucketSize;
  }

  public void setCurrentState(String resourceName, Partition partition, String instanceName,
      String state) {
    if (!_currentStateMap.containsKey(resourceName)) {
      _currentStateMap.put(resourceName, new HashMap<Partition, Map<String, String>>());
    }
    if (!_currentStateMap.get(resourceName).containsKey(partition)) {
      _currentStateMap.get(resourceName).put(partition, new HashMap<String, String>());
    }
    _currentStateMap.get(resourceName).get(partition).put(instanceName, state);
  }

  public void setEndTime(String resourceName, Partition partition,
      String instanceName, Long timestamp) {
    if (!_currentStateEndTimeMap.containsKey(resourceName)) {
      _currentStateEndTimeMap.put(resourceName, new HashMap<Partition, Map<String, Long>>());
    }
    if (!_currentStateEndTimeMap.get(resourceName).containsKey(partition)) {
      _currentStateEndTimeMap.get(resourceName).put(partition, new HashMap<String, Long>());
    }
    _currentStateEndTimeMap.get(resourceName).get(partition).put(instanceName, timestamp);
  }

  public void setRequestedState(String resourceName, Partition partition, String instanceName,
      String state) {
    if (!_requestedStateMap.containsKey(resourceName)) {
      _requestedStateMap.put(resourceName, new HashMap<Partition, Map<String, String>>());
    }
    if (!_requestedStateMap.get(resourceName).containsKey(partition)) {
      _requestedStateMap.get(resourceName).put(partition, new HashMap<String, String>());
    }
    _requestedStateMap.get(resourceName).get(partition).put(instanceName, state);
  }

  public void setInfo(String resourceName, Partition partition, String instanceName, String state) {
    if (!_infoMap.containsKey(resourceName)) {
      _infoMap.put(resourceName, new HashMap<Partition, Map<String, String>>());
    }
    if (!_infoMap.get(resourceName).containsKey(partition)) {
      _infoMap.get(resourceName).put(partition, new HashMap<String, String>());
    }
    _infoMap.get(resourceName).get(partition).put(instanceName, state);
  }

  public void setPendingState(String resourceName, Partition partition, String instanceName,
      Message message) {
    setStateMessage(resourceName, partition, instanceName, message, _pendingStateMap);
  }

  /**
   * Update the cancellation messages per resource per partition
   * @param resourceName
   * @param partition
   * @param instanceName
   * @param message
   */
  public void setCancellationState(String resourceName, Partition partition, String instanceName,
      Message message) {
    setStateMessage(resourceName, partition, instanceName, message, _cancellationStateMap);
  }

  private void setStateMessage(String resourceName, Partition partition, String instanceName,
      Message message, Map<String, Map<Partition, Map<String, Message>>> stateMessageMap) {
    if (!stateMessageMap.containsKey(resourceName)) {
      stateMessageMap.put(resourceName, new HashMap<Partition, Map<String, Message>>());
    }
    if (!stateMessageMap.get(resourceName).containsKey(partition)) {
      stateMessageMap.get(resourceName).put(partition, new HashMap<String, Message>());
    }
    stateMessageMap.get(resourceName).get(partition).put(instanceName, message);
  }

  /**
   * given (resource, partition, instance), returns currentState
   * @param resourceName
   * @param partition
   * @param instanceName
   * @return
   */
  public String getCurrentState(String resourceName, Partition partition, String instanceName) {
    Map<Partition, Map<String, String>> map = _currentStateMap.get(resourceName);
    if (map != null) {
      Map<String, String> instanceStateMap = map.get(partition);
      if (instanceStateMap != null) {
        return instanceStateMap.get(instanceName);
      }
    }
    return null;
  }

  public Long getEndTime(String resourceName, Partition partition,
      String instanceName) {
    Map<Partition, Map<String, Long>> partitionInfo = _currentStateEndTimeMap.get(resourceName);
    if (partitionInfo != null) {
      Map<String, Long> instanceInfo = partitionInfo.get(partition);
      if (instanceInfo != null && instanceInfo.get(instanceName) != null) {
        return instanceInfo.get(instanceName);
      }
    }
    return -1L;
  }

  public String getRequestedState(String resourceName, Partition partition, String instanceName) {
    Map<Partition, Map<String, String>> map = _requestedStateMap.get(resourceName);
    if (map != null) {
      Map<String, String> instanceStateMap = map.get(partition);
      if (instanceStateMap != null) {
        return instanceStateMap.get(instanceName);
      }
    }
    return null;
  }

  public String getInfo(String resourceName, Partition partition, String instanceName) {
    Map<Partition, Map<String, String>> map = _infoMap.get(resourceName);
    if (map != null) {
      Map<String, String> instanceStateMap = map.get(partition);
      if (instanceStateMap != null) {
        return instanceStateMap.get(instanceName);
      }
    }
    return null;
  }

  /**
   * given (resource, partition, instance), returns toState
   * @param resourceName
   * @param partition
   * @param instanceName
   * @return pending message
   */
  // TODO: this should return toState, not pending message, create a separate method
  public Message getPendingState(String resourceName, Partition partition, String instanceName) {
    return getStateMessage(resourceName, partition, instanceName, _pendingStateMap);
  }

  /**
   * Fetch cancellation message per resource, partition, instance
   * @param resourceName
   * @param partition
   * @param instanceName
   * @return
   */
  public Message getCancellationState(String resourceName, Partition partition,
      String instanceName) {
    return getStateMessage(resourceName, partition, instanceName, _cancellationStateMap);
  }

  private Message getStateMessage(String resourceName, Partition partition, String instanceName,
      Map<String, Map<Partition, Map<String, Message>>> stateMessageMap) {
    Map<Partition, Map<String, Message>> map = stateMessageMap.get(resourceName);
    if (map != null) {
      Map<String, Message> instanceStateMap = map.get(partition);
      if (instanceStateMap != null) {
        return instanceStateMap.get(instanceName);
      }
    }
    return null;
  }

  /**
   * Given resource, returns current state map (parition -> instance -> currentState)
   * @param resourceName
   * @return
   */
  public Map<Partition, Map<String, String>> getCurrentStateMap(String resourceName) {
    if (_currentStateMap.containsKey(resourceName)) {
      return  _currentStateMap.get(resourceName);
    }
    return Collections.emptyMap();
  }

  /**
   * given (resource, partition), returns (instance->currentState) map
   * @param resourceName
   * @param partition
   * @return
   */
  public Map<String, String> getCurrentStateMap(String resourceName, Partition partition) {
    if (_currentStateMap.containsKey(resourceName)) {
      Map<Partition, Map<String, String>> map = _currentStateMap.get(resourceName);
      if (map.containsKey(partition)) {
        return map.get(partition);
      }
    }
    return Collections.emptyMap();
  }

  /**
   * Given (resource, partition), returns (instance->toState) map
   * @param resourceName
   * @param partition
   * @return pending target state map
   */
  public Map<String, String> getPendingStateMap(String resourceName, Partition partition) {
    if (_pendingStateMap.containsKey(resourceName)) {
      Map<Partition, Map<String, Message>> map = _pendingStateMap.get(resourceName);
      if (map.containsKey(partition)) {
        Map<String, Message> pendingMsgMap = map.get(partition);
        Map<String, String> pendingStateMap = new HashMap<String, String>();
        for (String instance : pendingMsgMap.keySet()) {
          pendingStateMap.put(instance, pendingMsgMap.get(instance).getToState());
        }
        return pendingStateMap;
      }
    }
    return Collections.emptyMap();
  }

  /**
   * Given (resource, partition), returns (instance->pendingMessage) map
   * @param resourceName
   * @param partition
   * @return pending messages map
   */
  public Map<String, Message> getPendingMessageMap(String resourceName, Partition partition) {
    if (_pendingStateMap.containsKey(resourceName)) {
      Map<Partition, Map<String, Message>> map = _pendingStateMap.get(resourceName);
      if (map.containsKey(partition)) {
        return map.get(partition);
      }
    }
    return Collections.emptyMap();
  }

  /**
   * Get the partitions mapped in the current state
   * @param resourceId resource to look up
   * @return set of mapped partitions, or empty set if there are none
   */
  public Set<Partition> getCurrentStateMappedPartitions(String resourceId) {
    Map<Partition, Map<String, String>> currentStateMap = _currentStateMap.get(resourceId);
    Map<Partition, Map<String, Message>> pendingStateMap = _pendingStateMap.get(resourceId);
    Set<Partition> partitionSet = Sets.newHashSet();
    if (currentStateMap != null) {
      partitionSet.addAll(currentStateMap.keySet());
    }
    if (pendingStateMap != null) {
      partitionSet.addAll(pendingStateMap.keySet());
    }
    return partitionSet;
  }

  /**
   * Get the partitions count for each participant with the pending state and given resource state model
   * @param resourceStateModel specified resource state model to look up
   * @param state specified pending resource state to look up
   * @return set of participants to partitions mapping
   */
  public Map<String, Integer> getPartitionCountWithPendingState(String resourceStateModel, String state) {
    return getPartitionCountWithState(resourceStateModel, state, (Map) _pendingStateMap);
  }

  /**
   * Get the partitions count for each participant in the current state and with given resource state model
   * @param resourceStateModel specified resource state model to look up
   * @param state specified current resource state to look up
   * @return set of participants to partitions mapping
   */
  public Map<String, Integer> getPartitionCountWithCurrentState(String resourceStateModel, String state) {
    return getPartitionCountWithState(resourceStateModel, state, (Map) _currentStateMap);
  }

  private Map<String, Integer> getPartitionCountWithState(String resourceStateModel, String state,
      Map<String, Map<Partition, Map<String, Object>>> stateMap) {
    Map<String, Integer> currentPartitionCount = new HashMap<>();
    for (String resource : stateMap.keySet()) {
      String stateModel = _resourceStateModelMap.get(resource);
      if ((stateModel != null && stateModel.equals(resourceStateModel)) || (stateModel == null
          && resourceStateModel == null)) {
        for (Partition partition : stateMap.get(resource).keySet()) {
          Map<String, Object> partitionMessage = stateMap.get(resource).get(partition);
          for (Map.Entry<String, Object> participantMap : partitionMessage.entrySet()) {
            String participant = participantMap.getKey();
            if (!currentPartitionCount.containsKey(participant)) {
              currentPartitionCount.put(participant, 0);
            }

            Object curStateObj = participantMap.getValue();
            String currState = null;
            if (curStateObj != null) {
              if (curStateObj instanceof Message) {
                currState = ((Message) curStateObj).getToState();
              } else if (curStateObj instanceof String) {
                currState = curStateObj.toString();
              }
            }
            if ((currState != null && currState.equals(state)) || (currState == null && state == null)) {
              currentPartitionCount.put(participant, currentPartitionCount.get(participant) + 1);
            }
          }
        }
      }
    }
    return currentPartitionCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("current state= ").append(_currentStateMap);
    sb.append(", pending state= ").append(_pendingStateMap);
    return sb.toString();
  }

}
