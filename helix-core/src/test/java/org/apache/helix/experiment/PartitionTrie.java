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

public class PartitionTrie extends TrieNode<Void> {
  private String resouce;
  private String state;
  private CapacitySetting capacitySetting;

  public PartitionTrie(String id, String resource, String state, CapacitySetting capacitySetting) {
    super(id);
    this.resouce = resource;
    this.state = state;
    this.capacitySetting = capacitySetting;
  }

  public CapacitySetting getCapacitySetting() {
    return capacitySetting;
  }

  @Override
  public void addChildren(Void child) {

  }

  @Override
  public boolean release(InstanceTrie instanceTrie, PartitionTrie partitionTrie) throws HelixRebalanceException {
    return false;
  }

  @Override
  public boolean assign(InstanceTrie instanceTrie, PartitionTrie partitionTrie)
      throws HelixRebalanceException {
    return true;
  }
}
