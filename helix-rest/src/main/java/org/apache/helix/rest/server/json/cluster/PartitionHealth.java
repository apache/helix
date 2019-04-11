package org.apache.helix.rest.server.json.cluster;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionHealth {
  // Partition health map stores the global metadata about the partition health in the format of
  // instanceName -> partitionName -> isHealthy
  private Map<String, Map<String, Boolean>> _paritionHealthMap;
  private Map<String, List<String>> _instancesThatNeedDirectCallWithPartitions;

  public PartitionHealth() {
    _paritionHealthMap = new HashMap<>();
    _instancesThatNeedDirectCallWithPartitions = new HashMap<>();
  }

  public void addInstanceThatNeedDirectCallWithPartition(String instanceName,
      String partitionName) {
    _instancesThatNeedDirectCallWithPartitions
        .computeIfAbsent(instanceName, partitions -> new ArrayList<>()).add(partitionName);
  }

  public void setPartitionHealthForInstance(String instanceName,
      Map<String, Boolean> partitionHealth) {
    _paritionHealthMap.put(instanceName, partitionHealth);
  }

  public void addSinglePartitionHealthForInstance(String instanceName, String partitionName,
      Boolean isHealthy) {
    _paritionHealthMap.computeIfAbsent(instanceName, partitionMap -> new HashMap<>())
        .put(partitionName, isHealthy);
  }

  public List<String> getInstanceThatNeedDirectCallWithPartitions(String instanceName) {
    return _instancesThatNeedDirectCallWithPartitions.getOrDefault(instanceName,
        Collections.EMPTY_LIST);
  }

  public Map<String, Map<String, Boolean>> getParitionHealthMap() {
    return _paritionHealthMap;
  }

  public void removePartitionHealthForInstance(String instanceName) {
    _paritionHealthMap.remove(instanceName);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof PartitionHealth)) {
      return false;
    }

    return _paritionHealthMap.equals(((PartitionHealth) o)._paritionHealthMap)
        && _instancesThatNeedDirectCallWithPartitions
        .equals(((PartitionHealth) o)._instancesThatNeedDirectCallWithPartitions);
  }
}
