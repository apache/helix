package org.apache.helix.gateway.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class GatewayCurrentStateCache {
  String _clusterName;

  // A cache of current state. It should be updated by the HelixGatewayServiceChannel
  // instance -> instances assignments
  Map<String, Map<String, String>> _currentStateMap;

  // A cache of target state.
  // instance -> assignments
  Map<String, Map<String, String>> _targetStateMap;

  boolean _targetStateChanged = false;

  public GatewayCurrentStateCache(String clusterName) {
    _clusterName = clusterName;
    _currentStateMap = new ConcurrentHashMap<>();
    _targetStateMap = new ConcurrentHashMap<>();
  }

  public String getCurrentState(String instance, String shard) {
    return _currentStateMap.get(instance).get(shard);
  }

  /**
   * Update the cached current state of instances in a cluster, and return the diff of the change.
   * @param newCurrentStateMap The new current state map of instances in the cluster
   * @return
   */
  public Map<String, Map<String, String>> updateCacheWithNewCurrentStateAndGetDiff(
      Map<String, Map<String, String>> newCurrentStateMap) {
    Map<String, Map<String, String>> diff = null;
    for (Map.Entry<String, Map<String, String>> entry : newCurrentStateMap.entrySet()) {
      String instance = entry.getKey();
      Map<String, String> newCurrentState = entry.getValue();
      Map<String, String> oldCurrentState = _currentStateMap.get(instance);
      if (oldCurrentState == null || !oldCurrentState.equals(newCurrentState)) {
        if (diff == null) {
          diff = new HashMap<>();
        }
        if (oldCurrentState == null) {
          diff.put(instance, newCurrentState);
          continue;
        }
        for (String shard : newCurrentState.keySet()) {
          if (oldCurrentState.get(shard) == null || !oldCurrentState.get(shard).equals(newCurrentState.get(shard))) {
            diff.computeIfAbsent(instance, k -> new HashMap<>()).put(shard, newCurrentState.get(shard));
          }
        }
      }
    }
    _currentStateMap = newCurrentStateMap;
    return diff;
  }

  public void updateCacheWithCurrentStateDiff(Map<String, Map<String, String>> currentStateDiff) {
    updateShardStateMapWithDiff(currentStateDiff, _currentStateMap);
  }

  /**
   * Udate the target state with the changed target state maps.
   * All existing target states remains the same
   * @param targetStateChangeMap
   */
  public void updateTargetStateWithDiff(Map<String, Map<String, String>> targetStateChangeMap) {
    _targetStateChanged = updateShardStateMapWithDiff(targetStateChangeMap, _targetStateMap);
  }

  public boolean isTargetStateChanged() {
    return _targetStateChanged;
  }

  public void resetTargetStateChanged() {
    _targetStateChanged = false;
  }

  public Map<String, Map<String, String>> getTargetStateMap() {
    return _targetStateMap;
  }

  public String serializeTargetAssignments() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = mapper.createObjectNode();
    for (Map.Entry<String, Map<String, String>> entry : _targetStateMap.entrySet()) {
      String instance = entry.getKey();
      Map<String, String> assignments = entry.getValue();
      ObjectNode instanceNode = mapper.createObjectNode();
      for (Map.Entry<String, String> assignment : assignments.entrySet()) {
        instanceNode.put(assignment.getKey(), assignment.getValue());
      }
      root.set(instance, instanceNode);
    }
    return root.toString();
  }

  private boolean updateShardStateMapWithDiff(Map<String, Map<String, String>> diffMap,
      Map<String, Map<String, String>> currentMap) {
    if (diffMap == null || diffMap.isEmpty()) {
      return false;
    }
    for (Map.Entry<String, Map<String, String>> entry : diffMap.entrySet()) {
      String instance = entry.getKey();
      Map<String, String> currentState = entry.getValue();
      if (currentMap.get(instance) == null) {
        currentMap.put(instance, currentState);
      } else {
        currentMap.get(instance).entrySet().stream().forEach(e -> {
          if (currentState.get(e.getKey()) != null) {
            e.setValue(currentState.get(e.getKey()));
          }
        });
      }
    }
    return true;
  }
}

