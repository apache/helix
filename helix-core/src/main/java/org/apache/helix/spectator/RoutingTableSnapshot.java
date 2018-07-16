package org.apache.helix.spectator;

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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;

/**
 * The snapshot of RoutingTable information.  It is immutable, it reflects the routing table
 * information at the time it is generated.
 */
public class RoutingTableSnapshot {
  private final RoutingTable _routingTable;

  public RoutingTableSnapshot(RoutingTable routingTable) {
    _routingTable = routingTable;
  }

  /**
   * returns all instances for {resource} that are in a specific {state}.
   *
   * @param resourceName
   * @param state
   *
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResource(String resourceName, String state) {
    return _routingTable.getInstancesForResource(resourceName, state);
  }

  /**
   * returns the instances for {resource,partition} pair that are in a specific {state}
   *
   * @param resourceName
   * @param partitionName
   * @param state
   *
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResource(String resourceName, String partitionName,
      String state) {
    return _routingTable.getInstancesForResource(resourceName, partitionName, state);
  }

  /**
   * returns all instances for resources contains any given tags in {resource group} that are in a
   * specific {state}
   *
   * @param resourceGroupName
   * @param state
   *
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state,
      List<String> resourceTags) {
    return _routingTable.getInstancesForResourceGroup(resourceGroupName, state, resourceTags);
  }

  /**
   * returns all instances for all resources in {resource group} that are in a specific {state}
   *
   * @param resourceGroupName
   * @param state
   *
   * @return empty set if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state) {
    return _routingTable.getInstancesForResourceGroup(resourceGroupName, state);
  }

  /**
   * returns the instances for {resource group,partition} pair in all resources belongs to the given
   * resource group that are in a specific {state}.
   * The return results aggregate all partition states from all the resources in the given resource
   * group.
   *
   * @param resourceGroupName
   * @param partitionName
   * @param state
   *
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName,
      String partitionName, String state) {
    return _routingTable.getInstancesForResourceGroup(resourceGroupName, partitionName, state);
  }

  /**
   * returns the instances for {resource group,partition} pair contains any of the given tags that
   * are in a specific {state}.
   * Find all resources belongs to the given resource group that have any of the given resource tags
   * and return the aggregated partition states from all these resources.
   *
   * @param resourceGroupName
   * @param partitionName
   * @param state
   * @param resourceTags
   *
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName,
      String partitionName, String state, List<String> resourceTags) {
    return _routingTable
        .getInstancesForResourceGroup(resourceGroupName, partitionName, state, resourceTags);
  }

  /**
   * Return all liveInstances in the cluster now.
   *
   * @return
   */
  public Collection<LiveInstance> getLiveInstances() {
    return _routingTable.getLiveInstances();
  }

  /**
   * Return all instance's config in this cluster.
   *
   * @return
   */
  public Collection<InstanceConfig> getInstanceConfigs() {
    return _routingTable.getInstanceConfigs();
  }

  /**
   * Return names of all resources (shown in ExternalView) in this cluster.
   */
  public Collection<String> getResources() {
    return _routingTable.getResources();
  }

  /**
   * Returns a Collection of latest snapshot of ExternalViews. Note that if the RoutingTable is
   * instantiated using CurrentStates, this Collection will be empty.
   * @return
   */
  public Collection<ExternalView> getExternalViews() {
    return _routingTable.getExternalViews();
  }
}