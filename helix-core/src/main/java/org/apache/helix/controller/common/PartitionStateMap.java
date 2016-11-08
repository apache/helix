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
 * Hold the partition->{Instance, State} mapping for a resource.
 */
public class PartitionStateMap {
  // Map of partition->instance->state
  private Map<Partition, Map<String, String>> _stateMap;
  private String _resourceName;

  public PartitionStateMap(String resourceName) {
    _resourceName = resourceName;
    _stateMap = new HashMap<Partition, Map<String, String>>();
  }

  public PartitionStateMap(String resourceName,
      Map<Partition, Map<String, String>> partitionStateMap) {
    _resourceName = resourceName;
    _stateMap = partitionStateMap;
  }

  public Set<Partition> partitionSet() {
    return _stateMap.keySet();
  }

  public void setState(Partition partition, Map<String, String> stateMappingForPartition) {
    _stateMap.put(partition, stateMappingForPartition);
  }

  public void setState(Partition partition, String instance, String state) {
    if (!_stateMap.containsKey(partition)) {
      _stateMap.put(partition, new HashMap<String, String>());
    }
    _stateMap.get(partition).put(instance, state);
  }

  public Map<String, String> getPartitionMap(Partition partition) {
    Map<String, String> map = _stateMap.get(partition);
    return map != null ? map : Collections.<String, String>emptyMap();
  }

  public Map<Partition, Map<String, String>> getStateMap() {
    return _stateMap;
  }

  public String getResourceName() {
    return _resourceName;
  }

  @Override public String toString() {
    return _stateMap.toString();
  }
}
