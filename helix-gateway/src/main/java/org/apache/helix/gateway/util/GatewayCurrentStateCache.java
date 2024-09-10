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
  Map<String, ShardStateMap> _currentStateMap;

  // A cache of target state.
  // instance -> resource state (resource -> shard -> target state)
  final Map<String, ShardStateMap> _targetStateMap;

  public GatewayCurrentStateCache(String clusterName) {
    _clusterName = clusterName;
    _currentStateMap = new HashMap<>();
    _targetStateMap = new HashMap<>();
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
   * @param userCurrentStateMap The new current state map of instances in the cluster
   * @return
   */
  public Map<String, Map<String, Map<String, String>>> updateCacheWithNewCurrentStateAndGetDiff(
      Map<String, Map<String, Map<String, String>>> userCurrentStateMap) {
    Map<String, ShardStateMap> newCurrentStateMap = new HashMap<>(_currentStateMap);
    Map<String, Map<String, Map<String, String>>> diff = new HashMap<>();
    for (String instance : userCurrentStateMap.keySet()) {
      ShardStateMap oldStateMap = _currentStateMap.get(instance);
      Map<String, Map<String, String>> instanceDiff = oldStateMap == null ? userCurrentStateMap.get(instance)
          : oldStateMap.getDiff(userCurrentStateMap.get(instance));
      if (!instanceDiff.isEmpty()) {
        diff.put(instance, instanceDiff);
      }
      newCurrentStateMap.put(instance, new ShardStateMap(userCurrentStateMap.get(instance)));
    }
    logger.info("Update current state cache for instances: {}", diff.keySet());
    _currentStateMap = newCurrentStateMap;
    return diff;
  }

  /**
   * Update the current state with the changed current state maps.
   */
  public void updateCurrentStateOfExistingInstance(String instance, String resource, String shard, String shardState) {
    logger.info("Update current state of instance: {}, resource: {}, shard: {}, state: {}", instance, resource, shard,
        shardState);
    updateShardStateMapWithDiff(_currentStateMap, instance, resource, shard, shardState);
  }

  /**
   * Update the target state with the changed target state maps.
   * All existing target states remains the same
   */
  public void updateTargetStateOfExistingInstance(String instance, String resource, String shard, String shardState) {
    logger.info("Update target state of instance: {}, resource: {}, shard: {}, state: {}", instance, resource, shard,
        shardState);
    updateShardStateMapWithDiff(_targetStateMap, instance, resource, shard, shardState);
  }

  private void updateShardStateMapWithDiff(Map<String, ShardStateMap> stateMap, String instance, String resource,
      String shard, String shardState) {
    ShardStateMap curStateMap = stateMap.get(instance);
    if (curStateMap == null) {
      logger.warn("Instance {} is not in the state map, skip updating", instance);
      return;
    }
    curStateMap.updateWithShardState(resource, shard, shardState);
  }

  /**
   * Serialize the target state assignments to a JSON Node.
   * example ： {"instance1":{"resource1":{"shard1":"ONLINE","shard2":"OFFLINE"}}}}
   */
  public synchronized ObjectNode serializeTargetAssignmentsToJSONNode() {
    ObjectNode root = mapper.createObjectNode();
    for (Map.Entry<String, ShardStateMap> entry : _targetStateMap.entrySet()) {
      root.set(entry.getKey(), entry.getValue().toJSONNode());
    }
    return root;
  }

  /**
   * Remove the target state data of an instance from the cache.
   */
  public synchronized void removeInstanceTargetDataFromCache(String instance) {
    logger.info("Remove instance target data from cache for instance: {}", instance);
    _targetStateMap.remove(instance);
  }

  /**
   * Remove the current state data of an instance from the cache to an empty map.
   */
  public synchronized void resetTargetStateCache(String instance) {
    logger.info("Reset target state cache for instance: {}", instance);
    _targetStateMap.put(instance, new ShardStateMap(new HashMap<>()));
  }

  public static class ShardStateMap {
    Map<String, Map<String, String>> _stateMap;

    public ShardStateMap(Map<String, Map<String, String>> stateMap) {
      _stateMap = new HashMap<>(stateMap);
    }

    public String getState(String resource, String shard) {
      Map<String, String> shardStateMap = _stateMap.get(resource);
      return shardStateMap == null ? null : shardStateMap.get(shard);
    }

    public synchronized void updateWithShardState(String resource, String shard, String shardState) {
      logger.info("Update ShardStateMap of resource: {}, shard: {}, state: {}", resource, shard, shardState);
      _stateMap.computeIfAbsent(resource, k -> new HashMap<>()).put(shard, shardState);
    }

    private Map<String, Map<String, String>> getDiff(Map<String, Map<String, String>> newCurrentStateMap) {
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
      return diff;
    }

    /**
     * Serialize the shard state map to a JSON object.
     * @return a JSON object representing the shard state map. Example: {"shard1":"ONLINE","shard2":"OFFLINE"}
     */
    public synchronized ObjectNode toJSONNode() {
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
