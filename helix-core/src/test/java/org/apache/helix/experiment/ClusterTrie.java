package org.apache.helix.experiment;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.model.ResourceAssignment;

public class ClusterTrie extends TrieNode<FaultZoneTrie> {
  private Map<InstanceTrie, FaultZoneTrie> _instanceToZoneMap = new HashMap<>();
  private ClusterTrie _prevBestAssignment;

  public ClusterTrie(String id) {
    super(id);
  }

  public static ClusterTrie createFromJson(String jsonString) {
    //TODO: read the json file and construct the entire cluster trie
    return null;
  }

  public Map<String, ResourceAssignment> getResourceAssignments() {
    //TODO:
    return new HashMap<>();
  }

  @Override
  public boolean assign(InstanceTrie instanceTrie, PartitionTrie partitionTrie) throws HelixRebalanceException {
    if (!_instanceToZoneMap.containsKey(instanceTrie)) {
        // instance is outside scope of the cluster trie
        return false;
    }
    return _instanceToZoneMap.get(instanceTrie).assign(instanceTrie, partitionTrie);
  }

  @Override
  public void addChildren(FaultZoneTrie child) {
    getChildren().put(child.getId(), child);
    // update instance to zone information
    for (InstanceTrie instanceTrie : child.getChildren().values()) {
      _instanceToZoneMap.put(instanceTrie, child);
    }
  }

  //TODO
  @Override
  public boolean release(InstanceTrie instanceTrie, PartitionTrie partitionTrie) throws HelixRebalanceException {
    return false;
  }
}
