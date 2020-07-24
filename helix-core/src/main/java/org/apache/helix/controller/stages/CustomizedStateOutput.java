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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomizedStateOutput {
  private static Logger LOG = LoggerFactory.getLogger(CustomizedStateOutput.class);

  // stateType -> (resourceName -> (Partition -> (instanceName -> customizedState)))
  private final Map<String, Map<String, Map<Partition, Map<String, String>>>> _customizedStateMap;
  // stateType -> (resourceName -> (Partition -> (instanceName -> startTime)))
  private final Map<String, Map<String, Map<Partition, Map<String, String>>>> _startTimeMap;

  public CustomizedStateOutput() {
    _customizedStateMap = new HashMap<>();
    _startTimeMap = new HashMap<>();
  }

  public void setCustomizedState(String stateType, String resourceName, Partition partition,
      String instanceName, String state) {
    setCustomizedStateProperty(CustomizedState.CustomizedStateProperty.CURRENT_STATE, stateType,
        resourceName, partition, instanceName, state);
  }

  public void setStartTime(String stateType, String resourceName, Partition partition,
      String instanceName, String state) {
    setCustomizedStateProperty(CustomizedState.CustomizedStateProperty.START_TIME, stateType,
        resourceName, partition, instanceName, state);
  }

  private void setCustomizedStateProperty(CustomizedState.CustomizedStateProperty propertyName,
      String stateType, String resourceName, Partition partition, String instanceName,
      String state) {
    Map<String, Map<String, Map<Partition, Map<String, String>>>> mapToUpdate;
    switch (propertyName) {
      case CURRENT_STATE:
        mapToUpdate = _customizedStateMap;
        break;
      case START_TIME:
        mapToUpdate = _startTimeMap;
        break;
      default:
        LOG.error(
            "The customized state property is not supported, could not update customized state output.");
        return;
    }
    if (!mapToUpdate.containsKey(stateType)) {
      mapToUpdate.put(stateType, new HashMap<String, Map<Partition, Map<String, String>>>());
    }
    if (!mapToUpdate.get(stateType).containsKey(resourceName)) {
      mapToUpdate.get(stateType).put(resourceName, new HashMap<Partition, Map<String, String>>());
    }
    if (!mapToUpdate.get(stateType).get(resourceName).containsKey(partition)) {
      mapToUpdate.get(stateType).get(resourceName).put(partition, new HashMap<String, String>());
    }
    mapToUpdate.get(stateType).get(resourceName).get(partition).put(instanceName, state);
  }

  /**
   * Given stateType, returns resource customized state map (resource -> parition -> instance ->
   * customizedState)
   * @param stateType
   * @return
   */
  public Map<String, Map<Partition, Map<String, String>>> getCustomizedStateMap(String stateType) {
    return getCustomizedStateProperty(CustomizedState.CustomizedStateProperty.CURRENT_STATE,
        stateType);
  }

  public Map<String, Map<Partition, Map<String, String>>> getStartTimeMap(String stateType) {
    return getCustomizedStateProperty(CustomizedState.CustomizedStateProperty.START_TIME,
        stateType);
  }

  private Map<String, Map<Partition, Map<String, String>>> getCustomizedStateProperty(
      CustomizedState.CustomizedStateProperty propertyName, String stateType) {
    Map<String, Map<String, Map<Partition, Map<String, String>>>> readFromMap;
    switch (propertyName) {
      case CURRENT_STATE:
        readFromMap = _customizedStateMap;
        break;
      case START_TIME:
        readFromMap = _startTimeMap;
        break;
      default:
        LOG.error(
            "The customized state property is not supported, could not read from customized state output.");
        return Collections.emptyMap();
    }
    if (readFromMap.containsKey(stateType)) {
      return Collections.unmodifiableMap(readFromMap.get(stateType));
    }
    return Collections.emptyMap();
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


  public Map<Partition, Map<String, String>> getResourceStartTimeMap(String stateType,
      String resourceName) {
    if (getStartTimeMap(stateType).containsKey(resourceName)) {
      return Collections.unmodifiableMap(getStartTimeMap(stateType).get(resourceName));
    }
    return Collections.emptyMap();
  }

  public Set<String> getAllStateTypes() {
    return _customizedStateMap.keySet();
  }
}
