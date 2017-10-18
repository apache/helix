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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.ConfigChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingTableProvider implements ExternalViewChangeListener, ConfigChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(RoutingTableProvider.class);
  private final AtomicReference<RoutingTable> _routingTableRef;

  public RoutingTableProvider() {
    _routingTableRef = new AtomicReference<RoutingTableProvider.RoutingTable>(new RoutingTable());

  }

  /**
   * returns the instances for {resource,partition} pair that are in a specific
   * {state}
   *
   * This method will be deprecated, please use the
   * {@link #getInstancesForResource(String, String, String)} getInstancesForResource} method.
   * @param resourceName
   *          -
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstances(String resourceName, String partitionName, String state) {
    return getInstancesForResource(resourceName, partitionName, state);
  }

  /**
   * returns the instances for {resource,partition} pair that are in a specific
   * {state}
   * @param resourceName
   *          -
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResource(String resourceName, String partitionName, String state) {
    List<InstanceConfig> instanceList = null;
    RoutingTable _routingTable = _routingTableRef.get();
    ResourceInfo resourceInfo = _routingTable.get(resourceName);
    if (resourceInfo != null) {
      PartitionInfo keyInfo = resourceInfo.get(partitionName);
      if (keyInfo != null) {
        instanceList = keyInfo.get(state);
      }
    }
    if (instanceList == null) {
      instanceList = Collections.emptyList();
    }
    return instanceList;
  }

  /**
   * returns the instances for {resource group,partition} pair in all resources belongs to the given
   * resource group that are in a specific {state}.
   *
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
    List<InstanceConfig> instanceList = null;
    RoutingTable _routingTable = _routingTableRef.get();
    ResourceGroupInfo resourceGroupInfo = _routingTable.getResourceGroup(resourceGroupName);
    if (resourceGroupInfo != null) {
      PartitionInfo keyInfo = resourceGroupInfo.get(partitionName);
      if (keyInfo != null) {
        instanceList = keyInfo.get(state);
      }
    }
    if (instanceList == null) {
      instanceList = Collections.emptyList();
    }
    return instanceList;
  }

  /**
   * returns the instances for {resource group,partition} pair contains any of the given tags
   * that are in a specific {state}.
   *
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
  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String partitionName,
      String state, List<String> resourceTags) {
    RoutingTable _routingTable = _routingTableRef.get();
    ResourceGroupInfo resourceGroupInfo = _routingTable.getResourceGroup(resourceGroupName);
    List<InstanceConfig> instanceList = null;
    if (resourceGroupInfo != null) {
      instanceList = new ArrayList<InstanceConfig>();
      for (String tag : resourceTags) {
        PartitionInfo keyInfo = resourceGroupInfo.get(partitionName, tag);
        if (keyInfo != null && keyInfo.containsState(state)) {
          instanceList.addAll(keyInfo.get(state));
        }
      }
    }
    if (instanceList == null) {
      return Collections.emptyList();
    }

    return instanceList;
  }

  /**
   * returns all instances for {resource} that are in a specific {state}
   *
   * This method will be deprecated, please use the
   * {@link #getInstancesForResource(String, String) getInstancesForResource} method.
   * @param resourceName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstances(String resourceName, String state) {
    return getInstancesForResource(resourceName, state);
  }

  /**
   * returns all instances for {resource} that are in a specific {state}.
   * @param resourceName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResource(String resourceName, String state) {
    Set<InstanceConfig> instanceSet = null;
    RoutingTable routingTable = _routingTableRef.get();
    ResourceInfo resourceInfo = routingTable.get(resourceName);
    if (resourceInfo != null) {
      instanceSet = resourceInfo.getInstances(state);
    }
    if (instanceSet == null) {
      instanceSet = Collections.emptySet();
    }
    return instanceSet;
  }

  /**
   * returns all instances for all resources in {resource group} that are in a specific {state}
   *
   * @param resourceGroupName
   * @param state
   *
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state) {
    Set<InstanceConfig> instanceSet = null;
    RoutingTable _routingTable = _routingTableRef.get();
    ResourceGroupInfo resourceGroupInfo = _routingTable.getResourceGroup(resourceGroupName);
    if (resourceGroupInfo != null) {
      instanceSet = resourceGroupInfo.getInstances(state);
    }
    if (instanceSet == null) {
      instanceSet = Collections.emptySet();
    }
    return instanceSet;
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
    Set<InstanceConfig> instanceSet = null;
    RoutingTable _routingTable = _routingTableRef.get();
    ResourceGroupInfo resourceGroupInfo = _routingTable.getResourceGroup(resourceGroupName);
    if (resourceGroupInfo != null) {
      instanceSet = new HashSet<InstanceConfig>();
      for (String tag : resourceTags) {
        Set<InstanceConfig> instances = resourceGroupInfo.getInstances(state, tag);
        if (instances != null) {
          instanceSet.addAll(resourceGroupInfo.getInstances(state, tag));
        }
      }
    }
    if (instanceSet == null) {
      return Collections.emptySet();
    }
    return instanceSet;
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    // session has expired clean up the routing table
    if (changeContext.getType() == NotificationContext.Type.FINALIZE) {
      logger.info("Resetting the routing table. ");
      RoutingTable newRoutingTable = new RoutingTable();
      _routingTableRef.set(newRoutingTable);
      return;
    }
    refresh(externalViewList, changeContext);
  }

  @Override
  public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    // session has expired clean up the routing table
    if (changeContext.getType() == NotificationContext.Type.FINALIZE) {
      logger.info("Resetting the routing table. ");
      RoutingTable newRoutingTable = new RoutingTable();
      _routingTableRef.set(newRoutingTable);
      return;
    }

    HelixDataAccessor accessor = changeContext.getManager().getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    List<ExternalView> externalViewList = accessor.getChildValues(keyBuilder.externalViews());
    refresh(externalViewList, changeContext);
  }

  private void refresh(List<ExternalView> externalViewList, NotificationContext changeContext) {
    HelixDataAccessor accessor = changeContext.getManager().getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    List<InstanceConfig> configList = accessor.getChildValues(keyBuilder.instanceConfigs());
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<String, InstanceConfig>();
    for (InstanceConfig config : configList) {
      instanceConfigMap.put(config.getId(), config);
    }
    RoutingTable newRoutingTable = new RoutingTable();
    if (externalViewList != null) {
      for (ExternalView extView : externalViewList) {
        String resourceName = extView.getId();
        for (String partitionName : extView.getPartitionSet()) {
          Map<String, String> stateMap = extView.getStateMap(partitionName);
          for (String instanceName : stateMap.keySet()) {
            String currentState = stateMap.get(instanceName);
            if (instanceConfigMap.containsKey(instanceName)) {
              InstanceConfig instanceConfig = instanceConfigMap.get(instanceName);
              if (extView.isGroupRoutingEnabled()) {
                newRoutingTable.addEntry(resourceName, extView.getResourceGroupName(),
                    extView.getInstanceGroupTag(), partitionName, currentState, instanceConfig);
              } else {
                newRoutingTable.addEntry(resourceName, partitionName, currentState, instanceConfig);
              }
            } else {
              logger.error("Invalid instance name." + instanceName
                  + " .Not found in /cluster/configs/. instanceName: ");
            }
          }
        }
      }
    }
    _routingTableRef.set(newRoutingTable);
  }

  class RoutingTable {
    // mapping a resourceName to the ResourceInfo
    private final Map<String, ResourceInfo> resourceInfoMap;

    // mapping a resource group name to a resourceGroupInfo
    private final Map<String, ResourceGroupInfo> resourceGroupInfoMap;

    public RoutingTable() {
      resourceInfoMap = new HashMap<String, ResourceInfo>();
      resourceGroupInfoMap = new HashMap<String, ResourceGroupInfo>();
    }

    public void addEntry(String resourceName, String partitionName, String state,
        InstanceConfig config) {
      if (!resourceInfoMap.containsKey(resourceName)) {
        resourceInfoMap.put(resourceName, new ResourceInfo());
      }
      ResourceInfo resourceInfo = resourceInfoMap.get(resourceName);
      resourceInfo.addEntry(partitionName, state, config);
    }

    /**
     * add an entry with a resource with resourceGrouping enabled.
     */
    public void addEntry(String resourceName, String resourceGroupName, String resourceTag,
        String partitionName, String state, InstanceConfig config) {
      addEntry(resourceName, partitionName, state, config);

      if (!resourceGroupInfoMap.containsKey(resourceGroupName)) {
        resourceGroupInfoMap.put(resourceGroupName, new ResourceGroupInfo());
      }

      ResourceGroupInfo resourceGroupInfo = resourceGroupInfoMap.get(resourceGroupName);
      resourceGroupInfo.addEntry(resourceTag, partitionName, state, config);
    }

    ResourceInfo get(String resourceName) {
      return resourceInfoMap.get(resourceName);
    }

    ResourceGroupInfo getResourceGroup(String resourceGroupName) {
      return resourceGroupInfoMap.get(resourceGroupName);
    }
  }

  private static Comparator<InstanceConfig> INSTANCE_CONFIG_COMPARATOR =
      new Comparator<InstanceConfig>() {
        @Override
        public int compare(InstanceConfig o1, InstanceConfig o2) {
          if (o1 == o2) {
            return 0;
          }
          if (o1 == null) {
            return -1;
          }
          if (o2 == null) {
            return 1;
          }

          int compareTo = o1.getHostName().compareTo(o2.getHostName());
          if (compareTo == 0) {
            return o1.getPort().compareTo(o2.getPort());
          } else {
            return compareTo;
          }

        }
      };

  /**
   * Class to store instances, partitions and their states for each resource.
   */
  class ResourceInfo {
    // store PartitionInfo for each partition
    Map<String, PartitionInfo> partitionInfoMap;
    // stores the Set of Instances in a given state
    Map<String, Set<InstanceConfig>> stateInfoMap;

    public ResourceInfo() {
      partitionInfoMap = new HashMap<String, RoutingTableProvider.PartitionInfo>();
      stateInfoMap = new HashMap<String, Set<InstanceConfig>>();
    }

    public void addEntry(String stateUnitKey, String state, InstanceConfig config) {
      // add
      if (!stateInfoMap.containsKey(state)) {
        stateInfoMap.put(state, new TreeSet<InstanceConfig>(INSTANCE_CONFIG_COMPARATOR));
      }
      Set<InstanceConfig> set = stateInfoMap.get(state);
      set.add(config);

      if (!partitionInfoMap.containsKey(stateUnitKey)) {
        partitionInfoMap.put(stateUnitKey, new PartitionInfo());
      }
      PartitionInfo stateUnitKeyInfo = partitionInfoMap.get(stateUnitKey);
      stateUnitKeyInfo.addEntry(state, config);
    }

    public Set<InstanceConfig> getInstances(String state) {
      Set<InstanceConfig> instanceSet = stateInfoMap.get(state);
      return instanceSet;
    }

    PartitionInfo get(String stateUnitKey) {
      return partitionInfoMap.get(stateUnitKey);
    }
  }

  /**
   * Class to store instances, partitions and their states for each resource group.
   */
  class ResourceGroupInfo {
    // aggregated partitions and instances info for all resources in the resource group.
    ResourceInfo aggregatedResourceInfo;

    // <ResourceTag, ResourceInfo> maps resource tag to the resource with the tag
    // in this resource group.
    // Each ResourceInfo saves only partitions and instances for that resource.
    Map<String, ResourceInfo> tagToResourceMap;

    public ResourceGroupInfo() {
      aggregatedResourceInfo = new ResourceInfo();
      tagToResourceMap = new HashMap<String, ResourceInfo>();
    }

    public void addEntry(String resourceTag, String stateUnitKey, String state, InstanceConfig config) {
      // add the new entry to the aggregated resource info
      aggregatedResourceInfo.addEntry(stateUnitKey, state, config);

      // add the entry to the resourceInfo with given tag
      if (!tagToResourceMap.containsKey(resourceTag)) {
        tagToResourceMap.put(resourceTag, new ResourceInfo());
      }
      ResourceInfo resourceInfo = tagToResourceMap.get(resourceTag);
      resourceInfo.addEntry(stateUnitKey, state, config);
    }

    public Set<InstanceConfig> getInstances(String state) {
      return aggregatedResourceInfo.getInstances(state);
    }

    public Set<InstanceConfig> getInstances(String state, String resourceTag) {
      ResourceInfo resourceInfo = tagToResourceMap.get(resourceTag);
      if (resourceInfo != null) {
        return resourceInfo.getInstances(state);
      }

      return null;
    }

    PartitionInfo get(String stateUnitKey) {
      return aggregatedResourceInfo.get(stateUnitKey);
    }

    PartitionInfo get(String stateUnitKey, String resourceTag) {
      ResourceInfo resourceInfo = tagToResourceMap.get(resourceTag);
      if (resourceInfo == null) {
        return null;
      }

      return resourceInfo.get(stateUnitKey);
    }
  }

  class PartitionInfo {
    Map<String, List<InstanceConfig>> stateInfoMap;

    public PartitionInfo() {
      stateInfoMap = new HashMap<String, List<InstanceConfig>>();
    }

    public void addEntry(String state, InstanceConfig config) {
      if (!stateInfoMap.containsKey(state)) {
        stateInfoMap.put(state, new ArrayList<InstanceConfig>());
      }
      List<InstanceConfig> list = stateInfoMap.get(state);
      list.add(config);
    }

    List<InstanceConfig> get(String state) {
      return stateInfoMap.get(state);
    }

    boolean containsState(String state) {
      return stateInfoMap.containsKey(state);
    }
  }
}
