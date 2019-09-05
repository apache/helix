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

import org.apache.helix.HelixRebalanceException;

public class InstanceTrie extends TrieNode<PartitionTrie> {
  private CapacitySetting totalCapacity;
  private CapacitySetting remainingCapacities;

  public InstanceTrie(String id, CapacitySetting capacities) {
    super(id);
    this.totalCapacity = capacities;
    this.remainingCapacities = capacities;
  }

  @Override
  public boolean assign(InstanceTrie instanceTrie, PartitionTrie partitionTrie)
      throws HelixRebalanceException {
    if (!instanceTrie.getId().equals(getId())) {
      return false;
    }
    if (!instanceTrie.getChildren().containsKey(partitionTrie.getId())) {
      return false;
    }

    CapacitySetting newCapacity = remainingCapacities.subtract(partitionTrie.getCapacitySetting());
    if (newCapacity.isValidCapacity()) {
      getChildren().put(partitionTrie.getId(), partitionTrie);
      this.remainingCapacities = newCapacity;
      return true;
    }

    return false;
  }

  @Override
  public void addChildren(PartitionTrie child) {
    getChildren().put(child.getId(), child);
  }

  @Override
  public boolean release(InstanceTrie instanceTrie, PartitionTrie partitionTrie) throws HelixRebalanceException {
    return false;
  }
}
