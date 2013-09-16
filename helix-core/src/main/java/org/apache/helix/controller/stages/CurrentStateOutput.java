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

import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Partition;

@Deprecated
public class CurrentStateOutput {
  private final Map<String, Map<Partition, Map<String, String>>> _currentStateMap;
  private final Map<String, Map<Partition, Map<String, String>>> _pendingStateMap;
  private final Map<String, String> _resourceStateModelMap;
  private final Map<String, CurrentState> _curStateMetaMap;

  public CurrentStateOutput() {
    _currentStateMap = new HashMap<String, Map<Partition, Map<String, String>>>();
    _pendingStateMap = new HashMap<String, Map<Partition, Map<String, String>>>();
    _resourceStateModelMap = new HashMap<String, String>();
    _curStateMetaMap = new HashMap<String, CurrentState>();

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

  public void setPendingState(String resourceName, Partition partition, String instanceName,
      String state) {
    if (!_pendingStateMap.containsKey(resourceName)) {
      _pendingStateMap.put(resourceName, new HashMap<Partition, Map<String, String>>());
    }
    if (!_pendingStateMap.get(resourceName).containsKey(partition)) {
      _pendingStateMap.get(resourceName).put(partition, new HashMap<String, String>());
    }
    _pendingStateMap.get(resourceName).get(partition).put(instanceName, state);
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

  /**
   * given (resource, partition, instance), returns toState
   * @param resourceName
   * @param partition
   * @param instanceName
   * @return
   */
  public String getPendingState(String resourceName, Partition partition, String instanceName) {
    Map<Partition, Map<String, String>> map = _pendingStateMap.get(resourceName);
    if (map != null) {
      Map<String, String> instanceStateMap = map.get(partition);
      if (instanceStateMap != null) {
        return instanceStateMap.get(instanceName);
      }
    }
    return null;
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
   * given (resource, partition), returns (instance->toState) map
   * @param resourceName
   * @param partition
   * @return
   */
  public Map<String, String> getPendingStateMap(String resourceName, Partition partition) {
    if (_pendingStateMap.containsKey(resourceName)) {
      Map<Partition, Map<String, String>> map = _pendingStateMap.get(resourceName);
      if (map.containsKey(partition)) {
        return map.get(partition);
      }
    }
    return Collections.emptyMap();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("current state= ").append(_currentStateMap);
    sb.append(", pending state= ").append(_pendingStateMap);
    return sb.toString();

  }

}
