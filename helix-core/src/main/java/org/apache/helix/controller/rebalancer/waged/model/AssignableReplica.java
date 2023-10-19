package org.apache.helix.controller.rebalancer.waged.model;

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

import java.util.Map;

import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a partition replication that needs to be allocated.
 *
 * TODO: This class is violating the contracts of {@link Object#hashCode()}
 *   If two objects are equal according to the equals(Object) method, then calling the hashCode method on each of the
 *   two objects must produce the same integer result.
 *   https://github.com/apache/helix/issues/2299
 *   This could be a tricky fix because this bug/feature is deeply coupled with the existing code logic
 */
public class AssignableReplica implements Comparable<AssignableReplica> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignableReplica.class);

  private final String _replicaKey;
  private final String _partitionName;
  private final String _resourceName;
  private final String _resourceInstanceGroupTag;
  private final int _resourceMaxPartitionsPerInstance;
  private final Map<String, Integer> _capacityUsage;
  // The priority of the replica's state
  private final int _statePriority;
  // The state of the replica
  private final String _replicaState;
  private final String _evennessScoringCapacityKey;

  /**
   * @param clusterConfig  The cluster config.
   * @param resourceConfig The resource config for the resource which contains the replication.
   * @param partitionName  The replication's partition name.
   * @param replicaState   The state of the replication.
   * @param statePriority  The priority of the replication's state.
   */
  public AssignableReplica(ClusterConfig clusterConfig, ResourceConfig resourceConfig,
      String partitionName, String replicaState, int statePriority) {
    _partitionName = partitionName;
    _replicaState = replicaState;
    _statePriority = statePriority;
    _resourceName = resourceConfig.getResourceName();
    _capacityUsage = WagedRebalanceUtil.fetchCapacityUsage(partitionName, resourceConfig, clusterConfig);
    _resourceInstanceGroupTag = resourceConfig.getInstanceGroupTag();
    _resourceMaxPartitionsPerInstance = resourceConfig.getMaxPartitionsPerInstance();
    _replicaKey = generateReplicaKey(_resourceName, _partitionName,_replicaState);
    _evennessScoringCapacityKey = resourceConfig.getEvennessScoringCapacityKey();
  }

  public Map<String, Integer> getCapacity() {
    return _capacityUsage;
  }

  public String getPartitionName() {
    return _partitionName;
  }

  public String getReplicaState() {
    return _replicaState;
  }

  public boolean isReplicaTopState() {
    return _statePriority == StateModelDefinition.TOP_STATE_PRIORITY;
  }

  public int getStatePriority() {
    return _statePriority;
  }

  public String getResourceName() {
    return _resourceName;
  }

  public String getResourceInstanceGroupTag() {
    return _resourceInstanceGroupTag;
  }

  public boolean hasResourceInstanceGroupTag() {
    return _resourceInstanceGroupTag != null && !_resourceInstanceGroupTag.isEmpty();
  }

  public int getResourceMaxPartitionsPerInstance() {
    return _resourceMaxPartitionsPerInstance;
  }

  public String getEvennessScoringCapacityKey() {
    return _evennessScoringCapacityKey;
  }

  @Override
  public String toString() {
    return _replicaKey;
  }

  @Override
  public int compareTo(AssignableReplica replica) {
    if (!_resourceName.equals(replica._resourceName)) {
      return _resourceName.compareTo(replica._resourceName);
    }
    if (!_partitionName.equals(replica._partitionName)) {
      return _partitionName.compareTo(replica._partitionName);
    }
    if (!_replicaState.equals(replica._replicaState)) {
      return _replicaState.compareTo(replica._replicaState);
    }
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof AssignableReplica) {
      return compareTo((AssignableReplica) obj) == 0;
    } else {
      return false;
    }
  }

  public static String generateReplicaKey(String resourceName, String partitionName, String state) {
    return String.format("%s-%s-%s", resourceName, partitionName, state);
  }
}
