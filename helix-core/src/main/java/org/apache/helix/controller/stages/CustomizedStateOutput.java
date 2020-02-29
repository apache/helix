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

  private final Map<String, CustomizedState> _customizedStateMetaMap;

  public CustomizedStateOutput() {
    _customizedStateMap = new HashMap<>();
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

  /**
   * Given stateType, returns resource customized state map (resource -> parition -> instance ->
   * customizedState)
   * @param stateType
   * @return
   */
  public Map<String, Map<Partition, Map<String, String>>> getResourceCustomizedStateMap(
      String stateType) {
    if (_customizedStateMap.containsKey(stateType)) {
      return Collections.unmodifiableMap(_customizedStateMap.get(stateType));
    }
    return Collections.emptyMap();
  }

  /**
   * given (stateType, resource), returns (partition -> instance-> customizedState) map
   * @param stateType
   * @param resourceName
   * @return
   */
  public Map<Partition, Map<String, String>> getPartitionCustomizedStateMap(String stateType,
      String resourceName) {
    if (getResourceCustomizedStateMap(stateType).containsKey(resourceName)) {
      return Collections
          .unmodifiableMap(getResourceCustomizedStateMap(stateType).get(resourceName));
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
  public Map<String, String> getInstanceCustomizedStateMap(String stateType, String resourceName,
      Partition partition) {
    if (getResourceCustomizedStateMap(stateType).containsKey(resourceName)
        && getPartitionCustomizedStateMap(stateType, resourceName).containsKey(partition)) {
      return Collections
          .unmodifiableMap(getPartitionCustomizedStateMap(stateType, resourceName).get(partition));

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
  public String getCustomizedState(String stateType, String resourceName, Partition partition,
      String instanceName) {
    if (getResourceCustomizedStateMap(stateType).containsKey(resourceName)
        && getPartitionCustomizedStateMap(stateType, resourceName).containsKey(partition)
        && getInstanceCustomizedStateMap(stateType, resourceName, partition)
            .containsKey(instanceName)) {
      return getInstanceCustomizedStateMap(stateType, resourceName, partition).get(instanceName);
    }
    return null;
  }

  public Set<String> getAllStateTypes() {
    return _customizedStateMap.keySet();
  }
}
