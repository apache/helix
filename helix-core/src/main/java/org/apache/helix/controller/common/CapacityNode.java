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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A Node is an entity that can serve capacity recording purpose. It has a capacity and knowledge
 * of partitions assigned to it, so it can decide if it can receive additional partitions.
 */
public class CapacityNode {
  private int _currentlyAssigned;
  private int _capacity;
  private final String _id;
  private final Map<String, Set<String>> _partitionMap;

  public CapacityNode(String id) {
    _partitionMap = new HashMap<>();
    _currentlyAssigned = 0;
    this._id = id;
  }

  /**
   * Check if this replica can be legally added to this node
   *
   * @param resource  The resource to assign
   * @param partition The partition to assign
   * @return true if the assignment can be made, false otherwise
   */
  public boolean canAdd(String resource, String partition) {
    if (_currentlyAssigned >= _capacity || (_partitionMap.containsKey(resource)
        && _partitionMap.get(resource).contains(partition))) {
      return false;
    }

    // Add the partition to the resource's set of partitions in this node
    _partitionMap.computeIfAbsent(resource, k -> new HashSet<>()).add(partition);
    _currentlyAssigned++;
    return true;
  }

  /**
   * Checks if a specific resource + partition is assigned to this node.
   *
   * @param resource  the name of the resource
   * @param partition the partition
   * @return {@code true} if the resource + partition is assigned to this node, {@code false} otherwise
   */
  public boolean hasPartition(String resource, String partition) {
    Set<String> partitions = _partitionMap.get(resource);
    return partitions != null && partitions.contains(partition);
  }

  /**
   * Set the capacity of this node
   * @param capacity  The capacity to set
   */
  public void setCapacity(int capacity) {
    _capacity = capacity;
  }

  /**
   * Get the ID of this node
   * @return The ID of this node
   */
  public String getId() {
    return _id;
  }

  /**
   * Get number of partitions currently assigned to this node
   * @return The number of partitions currently assigned to this node
   */
  public int getCurrentlyAssigned() {
    return _currentlyAssigned;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("##########\nname=").append(_id).append("\nassigned:").append(_currentlyAssigned)
        .append("\ncapacity:").append(_capacity);
    return sb.toString();
  }
}
