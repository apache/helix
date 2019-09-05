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

import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixRebalanceException;

public class FaultZoneTrie extends TrieNode<InstanceTrie> {
  private Set<String> _partitions = new HashSet<>();

  public FaultZoneTrie(String id) {
    super(id);
  }

  @Override
  public void addChildren(InstanceTrie child) {
    getChildren().put(child.getId(), child);
  }

  @Override
  public boolean release(InstanceTrie instanceTrie, PartitionTrie partitionTrie) throws HelixRebalanceException {
    return false;
  }

  @Override
  public boolean assign(InstanceTrie instanceTrie, PartitionTrie partitionTrie)
      throws HelixRebalanceException {
    if (_partitions.contains(partitionTrie.getId())) {
      // duplicate partition
      return false;
    }

    if (getChildren().containsKey(instanceTrie.getId())) {
      boolean result = getChildren().get(instanceTrie.getId()).assign(instanceTrie, partitionTrie);
      if (result) {
        _partitions.add(partitionTrie.getId());
      }

      return result;
    }

    return false;
  }
}
