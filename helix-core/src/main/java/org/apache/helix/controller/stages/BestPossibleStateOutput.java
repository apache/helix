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

import org.apache.helix.model.Partition;

public class BestPossibleStateOutput {
  // Map of resource->partition->instance->state
  Map<String, Map<Partition, Map<String, String>>> _stateMap;

  public BestPossibleStateOutput() {
    _stateMap = new HashMap<String, Map<Partition, Map<String, String>>>();
  }

  public Set<String> resourceSet() {
    return _stateMap.keySet();
  }

  public void setState(String resourceName, Partition resource,
      Map<String, String> bestInstanceStateMappingForResource) {
    if (!_stateMap.containsKey(resourceName)) {
      _stateMap.put(resourceName, new HashMap<Partition, Map<String, String>>());
    }
    Map<Partition, Map<String, String>> map = _stateMap.get(resourceName);
    map.put(resource, bestInstanceStateMappingForResource);
  }

  public void setState(String resourceName, Partition partition, String instance, String state) {
    if (!_stateMap.containsKey(resourceName)) {
      _stateMap.put(resourceName, new HashMap<Partition, Map<String, String>>());
    }
    if (!_stateMap.get(resourceName).containsKey(partition)) {
      _stateMap.get(resourceName).put(partition, new HashMap<String, String>());
    }
    _stateMap.get(resourceName).get(partition).put(instance, state);
  }

  public Map<String, String> getInstanceStateMap(String resourceName, Partition partition) {
    Map<Partition, Map<String, String>> map = _stateMap.get(resourceName);
    if (map != null) {
      return map.get(partition);
    }
    return Collections.emptyMap();
  }

  public Map<Partition, Map<String, String>> getResourceMap(String resourceName) {
    Map<Partition, Map<String, String>> map = _stateMap.get(resourceName);
    if (map != null) {
      return map;
    }
    return Collections.emptyMap();
  }

  public Map<String, Map<Partition, Map<String, String>>> getStateMap() {
    return _stateMap;
  }

  @Override
  public String toString() {
    return _stateMap.toString();
  }
}
