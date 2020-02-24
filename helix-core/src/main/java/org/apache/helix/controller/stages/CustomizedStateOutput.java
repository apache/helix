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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.Partition;

public class CustomizedStateOutput {
  // stateType -> (resourceName -> (Partition -> (instanceName -> customizedState)))
  private final Map<String, Map<String, Map<Partition, Map<String, String>>>> _customizedStateMap;

  private final Map<String, Map<String, Map<Partition, Map<String, Long>>>> _customziedStateEndTimeMap;

  private final Map<String, CustomizedState> _customizedStateMetaMap;

  public CustomizedStateOutput() {
    _customizedStateMap = new HashMap<>();
    _customziedStateEndTimeMap = new HashMap<>();
    _customizedStateMetaMap = new HashMap<>();
  }

  public void setCustomizedState(String stateType, String resourceName, Partition partition,
      String instanceName, String state) {
    if (!_customizedStateMap.containsKey(stateType)) {
      _customizedStateMap.put(stateType,
          new HashMap<String, Map<Partition, Map<String, String>>>());
    }
    if (!_customizedStateMap.get(stateType).containsKey(resourceName)) {
      _customizedStateMap.get(stateType).put(resourceName,
          new HashMap<Partition, Map<String, String>>());
    }
    if (!_customizedStateMap.get(stateType).get(resourceName).containsKey(partition)) {
      _customizedStateMap.get(stateType).get(resourceName).put(partition,
          new HashMap<String, String>());
    }
    _customizedStateMap.get(stateType).get(resourceName).get(partition).put(instanceName, state);
  }

  public void setEndTime(String stateType, String resourceName, Partition partition,
      String instanceName, Long timestamp) {
    if (!_customziedStateEndTimeMap.containsKey(stateType)) {
      _customziedStateEndTimeMap.put(stateType,
          new HashMap<String, Map<Partition, Map<String, Long>>>());
    }
    if (!_customziedStateEndTimeMap.get(stateType).containsKey(resourceName)) {
      _customziedStateEndTimeMap.get(stateType).put(resourceName,
          new HashMap<Partition, Map<String, Long>>());
    }
    if (!_customziedStateEndTimeMap.get(stateType).get(resourceName).containsKey(partition)) {
      _customziedStateEndTimeMap.get(stateType).get(resourceName).put(partition,
          new HashMap<String, Long>());
    }
    _customziedStateEndTimeMap.get(stateType).get(resourceName).get(partition).put(instanceName,
        timestamp);
  }

  /**
   * given (stateType, resource, partition, instance), returns customized state
   * @param stateType
   * @param resourceName
   * @param partition
   * @param instanceName
   * @return
   */
  public String getCustomizedState(String stateType, String resourceName, Partition partition,
      String instanceName) {
    Map<String, Map<Partition, Map<String, String>>> map = _customizedStateMap.get(stateType);
    if (map != null) {
      Map<Partition, Map<String, String>> resourceMap = map.get(resourceName);
      if (resourceMap != null) {
        Map<String, String> instanceStateMap = resourceMap.get(partition);
        if (instanceStateMap != null) {
          return instanceStateMap.get(instanceName);
        }
      }
    }
    return null;
  }

  /**
   * Given stateType, returns resource customized state map (resource -> parition -> instance ->
   * customizedState)
   * @param stateType
   * @return
   */
  public Map<String, Map<Partition, Map<String, String>>> getResourceCustomizedStateMap(
      String stateType) {
    if (_customizedStateMap.containsKey(stateType)) {
      return _customizedStateMap.get(stateType);
    }
    return Collections.emptyMap();
  }

  /**
   * given (stateType, resource), returns (partition -> instance-> customizedState) map
   * @param stateType
   * @param resourceName
   * @return
   */
  public Map<Partition, Map<String, String>> getCustomizedStateMap(String stateType,
      String resourceName) {
    if (_customizedStateMap.containsKey(stateType)) {
      Map<String, Map<Partition, Map<String, String>>> map = _customizedStateMap.get(stateType);
      if (map.containsKey(resourceName)) {
        return map.get(resourceName);
      }
    }
    return Collections.emptyMap();
  }

  /**
   * given (stateType, resource, partition), returns (instance-> customizedState) map
   * @param stateType
   * @param resourceName
   * @param partition
   * @return
   */
  public Map<String, String> getCustomizedStateMap(String stateType, String resourceName,
      Partition partition) {
    if (_customizedStateMap.containsKey(stateType)) {
      Map<String, Map<Partition, Map<String, String>>> map = _customizedStateMap.get(stateType);
      if (map.containsKey(resourceName)) {
        Map<Partition, Map<String, String>> partitionMap = map.get(resourceName);
        if (partitionMap != null) {
          return partitionMap.get(partition);
        }
      }
    }
    return Collections.emptyMap();
  }

  public Long getEndTime(String stateType, String resourceName, Partition partition, String instanceName) {
    Map<String, Map<Partition, Map<String, Long>>> resourceMap = _customziedStateEndTimeMap.get(stateType);
    if (resourceMap != null) {
      Map<Partition, Map<String, Long>> partitionMap = resourceMap.get(resourceName);
      if (partitionMap != null) {
        Map<String, Long> instanceMap = partitionMap.get(partition);
        if (instanceMap != null && instanceMap.get(instanceName) != null) {
          return instanceMap.get(instanceName);
        }
      }
    }
      return -1L;
  }

  public Set<String> getAllStateTypes() {
    return _customizedStateMap.keySet();
  }
}
