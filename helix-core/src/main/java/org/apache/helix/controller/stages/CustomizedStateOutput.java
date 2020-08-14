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

import org.apache.helix.model.Partition;

public class CustomizedStateOutput {
  // stateType -> (resourceName -> (Partition -> (instanceName -> customizedState)))
  private final Map<String, Map<String, Map<Partition, Map<String, String>>>> _customizedStateMap;
  // stateType -> (resourceName -> (Partition -> (instanceName -> startTime)))
  private final Map<String, Map<String, Map<Partition, Map<String, Long>>>> _startTimeMap;

  public CustomizedStateOutput() {
    _customizedStateMap = new HashMap<>();
    _startTimeMap = new HashMap<>();
  }

  public void setCustomizedState(String stateType, String resourceName, Partition partition,
      String instanceName, String state, Long startTime) {
    setCurrentState(stateType, resourceName, partition, instanceName, state);
    setStartTime(stateType, resourceName, partition, instanceName, startTime);
  }

  private void setCurrentState(String stateType, String resourceName, Partition partition,
      String instanceName, String state) {
    _customizedStateMap.computeIfAbsent(stateType, k -> new HashMap<>())
        .computeIfAbsent(resourceName, k -> new HashMap<>())
        .computeIfAbsent(partition, k -> new HashMap<>()).put(instanceName, state);
  }

  private void setStartTime(String stateType, String resourceName, Partition partition,
      String instanceName, Long startTime) {
    _startTimeMap.computeIfAbsent(stateType, k -> new HashMap<>())
        .computeIfAbsent(resourceName, k -> new HashMap<>())
        .computeIfAbsent(partition, k -> new HashMap<>()).put(instanceName, startTime);
  }

  /**
   * Given stateType, returns resource customized state map (resource -> parition -> instance ->
   * customizedState)
   * @param stateType
   * @return
   */
  public Map<String, Map<Partition, Map<String, String>>> getCustomizedStateMap(String stateType) {
    if (_customizedStateMap.containsKey(stateType)) {
      return Collections.unmodifiableMap(_customizedStateMap.get(stateType));
    }
    return Collections.emptyMap();
  }

  private Map<String, Map<Partition, Map<String, Long>>> getStartTimeMap(String stateType) {
    return _startTimeMap.getOrDefault(stateType, Collections.emptyMap());
  }

  /**
   * given (stateType, resource), returns (partition -> instance-> customizedState) map
   * @param stateType
   * @param resourceName
   * @return
   */
  public Map<Partition, Map<String, String>> getResourceCustomizedStateMap(String stateType,
      String resourceName) {
    if (getCustomizedStateMap(stateType).containsKey(resourceName)) {
      return Collections.unmodifiableMap(getCustomizedStateMap(stateType).get(resourceName));
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
  public Map<String, String> getPartitionCustomizedStateMap(String stateType, String resourceName,
      Partition partition) {
    if (getCustomizedStateMap(stateType).containsKey(resourceName) && getResourceCustomizedStateMap(
        stateType, resourceName).containsKey(partition)) {
      return Collections
          .unmodifiableMap(getResourceCustomizedStateMap(stateType, resourceName).get(partition));
    }
    return Collections.emptyMap();
  }

  /**
   * given (stateType, resource, partition, instance), returns customized state
   * @param stateType
   * @param resourceName
   * @param partition
   * @param instanceName
   * @return
   */
  public String getPartitionCustomizedState(String stateType, String resourceName,
      Partition partition, String instanceName) {
    if (getCustomizedStateMap(stateType).containsKey(resourceName) && getResourceCustomizedStateMap(
        stateType, resourceName).containsKey(partition) && getPartitionCustomizedStateMap(stateType,
        resourceName, partition).containsKey(instanceName)) {
      return getPartitionCustomizedStateMap(stateType, resourceName, partition).get(instanceName);
    }
    return null;
  }

  public Map<Partition, Map<String, Long>> getResourceStartTimeMap(String stateType,
      String resourceName) {
    return Collections.unmodifiableMap(
        getStartTimeMap(stateType).getOrDefault(resourceName, Collections.emptyMap()));
  }

  public Set<String> getAllStateTypes() {
    return _customizedStateMap.keySet();
  }
}
