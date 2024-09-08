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
import org.apache.helix.gateway.channel.HelixGatewayServiceGrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A cache to store the current target assignment, and the reported current state of the instances in a cluster.
 */
public class GatewayCurrentStateCache {
  private static final Logger logger = LoggerFactory.getLogger(GatewayCurrentStateCache.class);
  static ObjectMapper mapper = new ObjectMapper();
  String _clusterName;

  // A cache of current state. It should be updated by the HelixGatewayServiceChannel
  // instance -> resource state (resource -> shard -> target state)
  final Map<String, ShardStateMap> _currentStateMap;

  // A cache of target state.
  // instance -> resource state (resource -> shard -> target state)
  final Map<String, ShardStateMap> _targetStateMap;

  public GatewayCurrentStateCache(String clusterName) {
    _clusterName = clusterName;
    _currentStateMap = new ConcurrentHashMap<>();
    _targetStateMap = new ConcurrentHashMap<>();
  }

  public String getCurrentState(String instance, String resource, String shard) {
    ShardStateMap shardStateMap = _currentStateMap.get(instance);
    return shardStateMap == null ? null : shardStateMap.getState(resource, shard);
  }

  public String getTargetState(String instance, String resource, String shard) {
    ShardStateMap shardStateMap = _targetStateMap.get(instance);
    return shardStateMap == null ? null : shardStateMap.getState(resource, shard);
  }

  /**
   * Update the cached current state of instances in a cluster, and return the diff of the change.
   * @param newCurrentStateMap The new current state map of instances in the cluster
   * @return
   */
  public Map<String, Map<String, Map<String, String>>> updateCacheWithNewCurrentStateAndGetDiff(
      Map<String, Map<String, Map<String, String>>> newCurrentStateMap) {
    Map<String, Map<String, Map<String, String>>> diff = new HashMap<>();
    for (String instance : newCurrentStateMap.keySet()) {
      if (!_currentStateMap.containsKey(instance)) {
        logger.warn("Instance {} is not in the state map, skip updating", instance);
        continue;
      }
      Map<String, Map<String, String>> newCurrentState = newCurrentStateMap.get(instance);
      Map<String, Map<String, String>> resourceStateDiff =
          _currentStateMap.computeIfAbsent(instance, k -> new ShardStateMap(new HashMap<>()))
              .updateAndGetDiff(newCurrentState);
      if (resourceStateDiff != null && !resourceStateDiff.isEmpty()) {
        diff.put(instance, resourceStateDiff);
      }
    }
    return diff;
  }

  /**
   * Update the current state with the changed current state maps.
   */
  public void updateCurrentStateOfExistingInstance(String instance, String resource, String shard, String shardState) {
    updateShardStateMapWithDiff(_currentStateMap, instance, Map.of(resource, Map.of(shard, shardState)));
  }

  /**
   * Update the target state with the changed target state maps.
   * All existing target states remains the same
   * @param diff
   */
  public void updateTargetStateWithDiff(String instance, Map<String, Map<String, String>> diff) {
      updateShardStateMapWithDiff(_targetStateMap, instance, diff);
  }

  /**
   * Serialize the target state assignments to a JSON Node.
   * example ： {"instance1":{"resource1":{"shard1":"ONLINE","shard2":"OFFLINE"}}}}
   */
  public ObjectNode serializeTargetAssignmentsToJSONNode() {
    ObjectNode root = mapper.createObjectNode();
    for (Map.Entry<String, ShardStateMap> entry : _targetStateMap.entrySet()) {
      root.set(entry.getKey(), entry.getValue().toJSONNode());
    }
    return root;
  }

  public void removeInstanceFromCache(String instance) {
    _currentStateMap.remove(instance);
    _targetStateMap.remove(instance);
  }

  public void addInstanceToCache(String instance) {
    _currentStateMap.put(instance, new ShardStateMap(new HashMap<>()));
    _targetStateMap.put(instance, new ShardStateMap(new HashMap<>()));
  }

  private void updateShardStateMapWithDiff(Map<String, ShardStateMap> stateMap, String instance,
      Map<String, Map<String, String>> diffMap) {
    if (diffMap == null || diffMap.isEmpty()) {
      return;
    }
    if (!stateMap.containsKey(instance)) {
      logger.warn("Instance {} is not in the state map, skip updating", instance);
    }
    stateMap.get(instance).updateWithDiff(diffMap);
  }

  public void resetTargetStateCache(String instance) {
    _targetStateMap.put(instance, new ShardStateMap(new HashMap<>()));
  }

  public static class ShardStateMap {
    Map<String, Map<String, String>> _stateMap;

    public ShardStateMap(Map<String, Map<String, String>> stateMap) {
      _stateMap = new ConcurrentHashMap<>(stateMap);
    }

    public String getState(String instance, String shard) {
      Map<String, String> shardStateMap = _stateMap.get(instance);
      return shardStateMap == null ? null : shardStateMap.get(shard);
    }

    private void updateWithDiff(Map<String, Map<String, String>> diffMap) {
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
    }

    private Map<String, Map<String, String>> updateAndGetDiff(Map<String, Map<String, String>> newCurrentStateMap) {
      Map<String, Map<String, String>> diff = new HashMap<>();
      for (Map.Entry<String, Map<String, String>> entry : newCurrentStateMap.entrySet()) {
        String resource = entry.getKey();
        Map<String, String> newCurrentState = entry.getValue();
        Map<String, String> oldCurrentState = _stateMap.get(resource);
        if (oldCurrentState == null) {
          diff.put(resource, newCurrentState);
          continue;
        }
        if (!oldCurrentState.equals(newCurrentState)) {
          for (String shard : newCurrentState.keySet()) {
            if (oldCurrentState.get(shard) == null || !oldCurrentState.get(shard).equals(newCurrentState.get(shard))) {
              diff.computeIfAbsent(resource, k -> new HashMap<>()).put(shard, newCurrentState.get(shard));
            }
          }
        }
      }
      _stateMap = new ConcurrentHashMap<>(newCurrentStateMap);
      return diff;
    }

    /**
     * Serialize the shard state map to a JSON object.
     * @return a JSON object representing the shard state map. Example: {"shard1":"ONLINE","shard2":"OFFLINE"}
     */
    public ObjectNode toJSONNode() {
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
