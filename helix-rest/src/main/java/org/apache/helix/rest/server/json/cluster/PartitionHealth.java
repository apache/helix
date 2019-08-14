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
import java.util.List;
import java.util.Map;


public class PartitionHealth {
  // Partition health map stores the global metadata about the partition health in the format of
  // instanceName -> partitionName -> isHealthy
  private Map<String, Map<String, Boolean>> _instanceToPartitionHealthMap;
  private Map<String, List<String>> _instanceToPartitionsMap;

  public PartitionHealth() {
    _instanceToPartitionHealthMap = new HashMap<>();
    _instanceToPartitionsMap = new HashMap<>();
  }

  public void addInstanceThatNeedDirectCallWithPartition(String instanceName,
      String partitionName) {
    _instanceToPartitionsMap
        .computeIfAbsent(instanceName, partitions -> new ArrayList<>()).add(partitionName);
  }

  public void addInstanceThatNeedDirectCall(String instanceName) {
    _instanceToPartitionsMap.put(instanceName, Collections.EMPTY_LIST);
  }

  public void addSinglePartitionHealthForInstance(String instanceName, String partitionName,
      Boolean isHealthy) {
    _instanceToPartitionHealthMap.computeIfAbsent(instanceName, partitionMap -> new HashMap<>())
        .put(partitionName, isHealthy);
  }

  public Map<String, List<String>> getExpiredRecords() {
      return _instanceToPartitionsMap;
  }

  public void updatePartitionHealth(String instance, String partition, boolean isHealthy) {
    _instanceToPartitionHealthMap.get(instance).put(partition, isHealthy);
  }

  public Map<String, Map<String, Boolean>> getGlobalPartitionHealth() {
    return _instanceToPartitionHealthMap;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof PartitionHealth)) {
      return false;
    }

    return _instanceToPartitionHealthMap.equals(((PartitionHealth) o)._instanceToPartitionHealthMap)
        && _instanceToPartitionsMap
        .equals(((PartitionHealth) o)._instanceToPartitionsMap);
  }
}
