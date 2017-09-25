package org.apache.helix.controller.common;

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

import org.apache.helix.model.Partition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Hold the Resource -> partition -> instance -> state mapping for all resources.
 * This is the base class for BestPossibleStateOutput, IdealState
 */
public class ResourcesStateMap {
  // Map of resource->PartitionStateMap
  protected Map<String, PartitionStateMap> _resourceStateMap;

  public ResourcesStateMap() {
    _resourceStateMap = new HashMap<String, PartitionStateMap>();
  }

  public Set<String> resourceSet() {
    return _resourceStateMap.keySet();
  }

  public void setState(String resourceName, Partition partition,
      Map<String, String> instanceStateMappingForPartition) {
    if (!_resourceStateMap.containsKey(resourceName)) {
      _resourceStateMap.put(resourceName, new PartitionStateMap(resourceName));
    }
    PartitionStateMap partitionStateMap = _resourceStateMap.get(resourceName);
    partitionStateMap.setState(partition, instanceStateMappingForPartition);
  }

  public void setState(String resourceName,
      Map<Partition, Map<String, String>> instanceStateMappingForResource) {
    _resourceStateMap
        .put(resourceName, new PartitionStateMap(resourceName, instanceStateMappingForResource));
  }

  public void setState(String resourceName, PartitionStateMap partitionStateMapForResource) {
    _resourceStateMap.put(resourceName, partitionStateMapForResource);
  }

  public void setState(String resourceName, Partition partition, String instance, String state) {
    if (!_resourceStateMap.containsKey(resourceName)) {
      _resourceStateMap.put(resourceName, new PartitionStateMap(resourceName));
    }
    _resourceStateMap.get(resourceName).setState(partition, instance, state);
  }

  public Map<String, String> getInstanceStateMap(String resourceName, Partition partition) {
    PartitionStateMap stateMap = _resourceStateMap.get(resourceName);
    if (stateMap != null) {
      return stateMap.getPartitionMap(partition);
    }
    return Collections.emptyMap();
  }

  public PartitionStateMap getPartitionStateMap(String resourceName) {
    PartitionStateMap stateMap = _resourceStateMap.get(resourceName);
    if (stateMap != null) {
      return stateMap;
    }
    return new PartitionStateMap(resourceName);
  }

  public Map<String, PartitionStateMap> getResourceStatesMap() {
    return _resourceStateMap;
  }

  @Override
  public String toString() {
    return _resourceStateMap.toString();
  }
}
