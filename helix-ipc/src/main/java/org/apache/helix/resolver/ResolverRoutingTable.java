package org.apache.helix.resolver;

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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.spectator.RoutingTableProvider;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A routing table that can also return all resources, partitions, and states in the cluster
 */
public class ResolverRoutingTable extends RoutingTableProvider {
  Map<String, Set<String>> _resourceMap;
  Set<String> _stateSet;

  /**
   * Create the table.
   */
  public ResolverRoutingTable() {
    super();
    _resourceMap = Maps.newHashMap();
    _stateSet = Sets.newHashSet();
  }

  /**
   * Get all resources that are currently served in the cluster.
   * @return set of resource names
   */
  public synchronized Set<String> getResources() {
    return Sets.newHashSet(_resourceMap.keySet());
  }

  /**
   * Get all partitions currently served for a resource.
   * @param resource the resource for which to look up partitions
   * @return set of partition names
   */
  public synchronized Set<String> getPartitions(String resource) {
    if (_resourceMap.containsKey(resource)) {
      return Sets.newHashSet(_resourceMap.get(resource));
    } else {
      return Collections.emptySet();
    }
  }

  /**
   * Get all states that partitions of all resources are currently in
   * @return set of state names
   */
  public synchronized Set<String> getStates() {
    return Sets.newHashSet(_stateSet);
  }

  @Override
  public synchronized void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    super.onExternalViewChange(externalViewList, changeContext);
    _resourceMap.clear();
    _stateSet.clear();
    for (ExternalView externalView : externalViewList) {
      _resourceMap.put(externalView.getResourceName(), externalView.getPartitionSet());
      for (String partition : externalView.getPartitionSet()) {
        _stateSet.addAll(externalView.getStateMap(partition).values());
      }
    }
  }
}
