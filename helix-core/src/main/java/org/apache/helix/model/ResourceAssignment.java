package org.apache.helix.model;

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
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixProperty;

/**
 * Represents the assignments of replicas for an entire resource, keyed on partitions of the
 * resource. Each partition has its replicas assigned to a node, and each replica is in a state.
 * For example, if there is a partition p with 2 replicas, a valid assignment is:<br />
 * <br />
 * p: {(n1, s1), (n2, s2)}<br />
 * <br />
 * This means one replica of p is located at node n1 and is in state s1, and another is in node n2
 * and is in state s2. n1 cannot be equal to n2, but s1 can be equal to s2 if at least two replicas
 * can be in s1.
 */
public class ResourceAssignment extends HelixProperty {

  /**
   * Initialize an empty mapping
   * @param resourceName the resource being mapped
   */
  public ResourceAssignment(String resourceName) {
    super(resourceName);
  }

  /**
   * Initialize a mapping from an existing ResourceMapping
   * @param existingMapping pre-populated ResourceMapping
   */
  public ResourceAssignment(ResourceAssignment existingMapping) {
    super(existingMapping);
  }

  /**
   * Get the currently mapped partitions
   * @return list of Partition objects
   */
  public List<Partition> getMappedPartitions() {
    List<Partition> partitions = new ArrayList<Partition>();
    for (String partitionName : _record.getMapFields().keySet()) {
      partitions.add(new Partition(partitionName));
    }
    return partitions;
  }

  /**
   * Get the instance, state pairs for a partition
   * @param partition the Partition to look up
   * @return map of (instance name, state)
   */
  public Map<String, String> getReplicaMap(Partition partition) {
    if (_record.getMapFields().containsKey(partition.getPartitionName())) {
      return _record.getMapField(partition.getPartitionName());
    }
    return Collections.emptyMap();
  }

  /**
   * Add instance, state pairs for a partition
   * @param partition the partition to set
   * @param replicaMap map of (instance name, state)
   */
  public void addReplicaMap(Partition partition, Map<String, String> replicaMap) {
    _record.setMapField(partition.getPartitionName(), replicaMap);
  }
}
