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


/**
 * A cache to store the current target assignment, and the reported current state of the instances in a cluster.
 */
public class GatewayCurrentStateCache {
  static ObjectMapper mapper = new ObjectMapper();
  String _clusterName;

  // A cache of current state. It should be updated by the HelixGatewayServiceChannel
  // instance -> resource state (resource -> shard -> target state)
  Map<String, ShardStateMap> _currentStateMap;

  // A cache of target state.
  // instance -> resource state (resource -> shard -> target state)
  Map<String, ShardStateMap> _targetStateMap;

  boolean _targetStateChanged = false;
  final Object _targetStateLock;

  public GatewayCurrentStateCache(String clusterName) {
    _clusterName = clusterName;
    _currentStateMap = new HashMap<>();
    _targetStateMap = new HashMap<>();
    _targetStateLock = new Object();
  }

  public String getCurrentState(String instance, String resource, String shard) {
    return _currentStateMap.get(instance).getState(resource, shard);
  }

  /**
   * Update the cached current state of instances in a cluster, and return the diff of the change.
   * @param newCurrentStateMap The new current state map of instances in the cluster
   * @return
   */
  public Map<String, Map<String, Map<String, String>>> updateCacheWithNewCurrentStateAndGetDiff(
      Map<String, Map<String, Map<String, String>>> newCurrentStateMap) {
    Map<String, Map<String, Map<String, String>>> diff = new HashMap<>();
    for(String instance : newCurrentStateMap.keySet()) {
      Map<String, Map<String, String>> newCurrentState = newCurrentStateMap.get(instance);
      diff.put(instance, _currentStateMap.computeIfAbsent(instance, k -> new ShardStateMap(new HashMap<>()))
          .updateMapAndGetDiff(newCurrentState));
    }
    return diff;
  }

  /**
   * Update the cache with the current state diff.
   * All existing target states remains the same
   * @param currentStateDiff
   */
  public void updateCacheWithCurrentStateDiff(Map<String, Map<String, Map<String, String>>> currentStateDiff) {
    for (String instance : currentStateDiff.keySet()) {
      Map<String, Map<String, String>> currentStateDiffMap = currentStateDiff.get(instance);
      _currentStateMap.computeIfAbsent(instance, k -> new ShardStateMap(new HashMap<>()))
          .updateShardStateMapWithDiff(currentStateDiffMap);
    }
  }

  /**
   * Update the target state with the changed target state maps.
   * All existing target states remains the same
   * @param targetStateChangeMap
   */
  public void updateTargetStateWithDiff(String instance, Map<String, Map<String, String>> targetStateChangeMap) {
    synchronized (_targetStateLock) {
      _targetStateChanged = _targetStateMap.get(instance).updateShardStateMapWithDiff(targetStateChangeMap);
    }
  }

  /**
   * Get the target state map if it has changed since last time and reset the _targetStateChanged flag in.
   * @return The target state map if it has changed, otherwise return null.
   */
  public Map<String, ShardStateMap> getTargetStateMapIfChanged() {
    synchronized (_targetStateLock) {
      if (_targetStateChanged) {
        _targetStateChanged = false;
        return _targetStateMap;
      }
      return null;
    }
  }

  /**
   * Serialize the target state assignments to a JSON string.
   * example ： {"instance1":{"resource1":{"shard1":"ONLINE","shard2":"OFFLINE"}}}}
   */
  public String serializeTargetAssignments() {
    ObjectNode root = mapper.createObjectNode();
    for (Map.Entry<String, ShardStateMap> entry : _targetStateMap.entrySet()) {
      root.set(entry.getKey(), entry.getValue().serialize());
    }
    return root.toString();
  }


  public class ShardStateMap {
    Map<String, Map<String, String>> _stateMap;
    public ShardStateMap(Map<String, Map<String, String>> stateMap) {
      _stateMap = stateMap;
    }
    public String getState(String instance, String shard) {
      return _stateMap.get(instance).get(shard);
    }

    private boolean updateShardStateMapWithDiff(Map<String, Map<String, String>> diffMap) {
      if (diffMap == null || diffMap.isEmpty()) {
        return false;
      }
      for (Map.Entry<String, Map<String, String>> diffEntry : diffMap.entrySet()) {
        String resource = diffEntry.getKey();
        Map<String, String> diffCurrentState = diffEntry.getValue();
        if (_stateMap.get(resource) != null) {
          _stateMap.get(resource).entrySet().forEach(currentMapEntry -> {
            String shard = currentMapEntry.getKey();
            if (diffCurrentState.get(shard) != null) {
              currentMapEntry.setValue(diffCurrentState.get(shard));
            }
          });
        } else {
          _stateMap.put(resource, diffCurrentState);
        }
      }
      return true;
    }

    private Map<String, Map<String, String>> updateMapAndGetDiff(
         Map<String, Map<String, String>> newCurrentStateMap) {
      Map<String, Map<String, String>> diff = new HashMap<>();
      for (Map.Entry<String, Map<String, String>> entry : newCurrentStateMap.entrySet()) {
        String instance = entry.getKey();
        Map<String, String> newCurrentState = entry.getValue();
        Map<String, String> oldCurrentState = _stateMap.get(instance);
        if (oldCurrentState == null) {
          diff.put(instance, newCurrentState);
          continue;
        }
        if (!oldCurrentState.equals(newCurrentState)) {
          for (String shard : newCurrentState.keySet()) {
            if (oldCurrentState.get(shard) == null || !oldCurrentState.get(shard).equals(newCurrentState.get(shard))) {
              diff.computeIfAbsent(instance, k -> new HashMap<>()).put(shard, newCurrentState.get(shard));
            }
          }
        }
      }
      _stateMap = newCurrentStateMap;
      return diff;
    }

    /**
     * Serialize the shard state map to a JSON object.
     * @return a JSON object representing the shard state map. Example: {"shard1":"ONLINE","shard2":"OFFLINE"}
     */
    public ObjectNode serialize() {
      ObjectNode root = mapper.createObjectNode();
      for (Map.Entry<String, Map<String, String>> entry : _stateMap.entrySet()) {
        String resource = entry.getKey();
        ObjectNode resourceNode = mapper.createObjectNode();
        for (Map.Entry<String, String> shardEntry : entry.getValue().entrySet()) {
          resourceNode.put(shardEntry.getKey(), shardEntry.getValue());
        }
        root.set(resource, resourceNode);
      }
      return root;
    }

  }
}

