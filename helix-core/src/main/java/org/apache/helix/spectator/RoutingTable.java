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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to consume ExternalViews of a cluster and provide {resource, partition, state} to
 * {instances} map function.
 */
class RoutingTable {
  private static final Logger logger = LoggerFactory.getLogger(RoutingTable.class);

  // mapping a resourceName to the ResourceInfo
  private final Map<String, ResourceInfo> _resourceInfoMap;
  // mapping a resource group name to a resourceGroupInfo
  private final Map<String, ResourceGroupInfo> _resourceGroupInfoMap;
  private final Collection<LiveInstance> _liveInstances;
  private final Collection<InstanceConfig> _instanceConfigs;

  public RoutingTable() {
    this(Collections.<ExternalView>emptyList(), Collections.<InstanceConfig>emptyList(),
        Collections.<LiveInstance>emptyList());
  }

  public RoutingTable(Collection<ExternalView> externalViews, Collection<InstanceConfig> instanceConfigs,
      Collection<LiveInstance> liveInstances) {
    _resourceInfoMap = new HashMap<>();
    _resourceGroupInfoMap = new HashMap<>();
    _liveInstances = new HashSet<>(liveInstances);
    _instanceConfigs = new HashSet<>(instanceConfigs);
    refresh(externalViews);
  }

  public RoutingTable(Map<String, Map<String, Map<String, CurrentState>>> currentStateMap,
      Collection<InstanceConfig> instanceConfigs, Collection<LiveInstance> liveInstances) {
    _resourceInfoMap = new HashMap<>();
    _resourceGroupInfoMap = new HashMap<>();
    _liveInstances = liveInstances;
    _instanceConfigs = instanceConfigs;
    refresh(currentStateMap);
  }

  private void refresh(Collection<ExternalView> externalViewList) {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    for (InstanceConfig config : _instanceConfigs) {
      instanceConfigMap.put(config.getId(), config);
    }
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
                addEntry(resourceName, extView.getResourceGroupName(),
                    extView.getInstanceGroupTag(), partitionName, currentState, instanceConfig);
              } else {
                addEntry(resourceName, partitionName, currentState, instanceConfig);
              }
            } else {
              logger.error("Invalid instance name. " + instanceName
                  + " .Not found in /cluster/configs/. instanceName: ");
            }
          }
        }
      }
    }
  }

  private void refresh(Map<String, Map<String, Map<String, CurrentState>>> currentStateMap) {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    for (InstanceConfig config : _instanceConfigs) {
      instanceConfigMap.put(config.getId(), config);
    }

    for (LiveInstance liveInstance : _liveInstances) {
      String instanceName = liveInstance.getInstanceName();
      String sessionId = liveInstance.getSessionId();
      InstanceConfig instanceConfig = instanceConfigMap.get(instanceName);
      if (instanceConfig == null) {
        logger.error("Invalid instance name. " + instanceName
            + " .Not found in /cluster/configs/. instanceName: ");
      }

      Map<String, CurrentState> currentStates = Collections.emptyMap();
      if (currentStateMap.containsKey(instanceName) && currentStateMap.get(instanceName)
          .containsKey(sessionId)) {
        currentStates = currentStateMap.get(instanceName).get(sessionId);
      }

      for (CurrentState currentState : currentStates.values()) {
        String resourceName = currentState.getResourceName();
        Map<String, String> stateMap = currentState.getPartitionStateMap();

        for (String partitionName : stateMap.keySet()) {
          String state = stateMap.get(partitionName);
          addEntry(resourceName, partitionName, state, instanceConfig);
        }
      }
    }
  }

  private void addEntry(String resourceName, String partitionName, String state,
      InstanceConfig config) {
    if (!_resourceInfoMap.containsKey(resourceName)) {
      _resourceInfoMap.put(resourceName, new ResourceInfo());
    }
    ResourceInfo resourceInfo = _resourceInfoMap.get(resourceName);
    resourceInfo.addEntry(partitionName, state, config);
  }

  /**
   * add an entry with a resource with resourceGrouping enabled.
   */
  private void addEntry(String resourceName, String resourceGroupName, String resourceTag,
      String partitionName, String state, InstanceConfig config) {
    addEntry(resourceName, partitionName, state, config);

    if (!_resourceGroupInfoMap.containsKey(resourceGroupName)) {
      _resourceGroupInfoMap.put(resourceGroupName, new ResourceGroupInfo());
    }

    ResourceGroupInfo resourceGroupInfo = _resourceGroupInfoMap.get(resourceGroupName);
    resourceGroupInfo.addEntry(resourceTag, partitionName, state, config);
  }

  ResourceInfo get(String resourceName) {
    return _resourceInfoMap.get(resourceName);
  }

  ResourceGroupInfo getResourceGroup(String resourceGroupName) {
    return _resourceGroupInfoMap.get(resourceGroupName);
  }

  /**
   * returns all instances for {resource} that are in a specific {state}.
   * @param resourceName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResource(String resourceName, String state) {
    Set<InstanceConfig> instanceSet = null;
    ResourceInfo resourceInfo = get(resourceName);
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
    ResourceGroupInfo resourceGroupInfo = getResourceGroup(resourceGroupName);
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
    ResourceGroupInfo resourceGroupInfo = getResourceGroup(resourceGroupName);
    if (resourceGroupInfo != null) {
      instanceSet = new HashSet<>();
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

  /**
   * returns the instances for {resource,partition} pair that are in a specific
   * {state}
   * @param resourceName
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResource(String resourceName, String partitionName,
      String state) {
    List<InstanceConfig> instanceList = null;
    ResourceInfo resourceInfo = get(resourceName);
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
    ResourceGroupInfo resourceGroupInfo = getResourceGroup(resourceGroupName);
    if (resourceGroupInfo != null) {
      PartitionInfo keyInfo = resourceGroupInfo.get(partitionName);
      if (keyInfo != null) {
        instanceList = keyInfo.get(state);
      }
    }
    if (instanceList == null) {
      instanceList = Collections.emptyList();
    }
    return Collections.unmodifiableList(instanceList);
  }

  /**
   * Return all liveInstances in the cluster now.
   * @return
   */
  protected Collection<LiveInstance> getLiveInstances() {
    return Collections.unmodifiableCollection(_liveInstances);
  }

  /**
   * Return all instance's config in this cluster.
   * @return
   */
  protected Collection<InstanceConfig> getInstanceConfigs() {
    return Collections.unmodifiableCollection(_instanceConfigs);
  }

  /**
   * Return names of all resources (shown in ExternalView) in this cluster.
   */
  protected Collection<String> getResources() {
    return Collections.unmodifiableCollection(_resourceInfoMap.keySet());
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
    ResourceGroupInfo resourceGroupInfo = getResourceGroup(resourceGroupName);
    List<InstanceConfig> instanceList = null;
    if (resourceGroupInfo != null) {
      instanceList = new ArrayList<>();
      for (String tag : resourceTags) {
        RoutingTable.PartitionInfo keyInfo = resourceGroupInfo.get(partitionName, tag);
        if (keyInfo != null && keyInfo.containsState(state)) {
          instanceList.addAll(keyInfo.get(state));
        }
      }
    }
    if (instanceList == null) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(instanceList);
  }

  /**
   * Class to store instances, partitions and their states for each resource.
   */
  class ResourceInfo {
    // store PartitionInfo for each partition
    Map<String, PartitionInfo> partitionInfoMap;
    // stores the Set of Instances in a given state
    Map<String, Set<InstanceConfig>> stateInfoMap;

    public ResourceInfo() {
      partitionInfoMap = new HashMap<>();
      stateInfoMap = new HashMap<>();
    }

    public void addEntry(String stateUnitKey, String state, InstanceConfig config) {
      if (!stateInfoMap.containsKey(state)) {
        stateInfoMap.put(state, new TreeSet<>(INSTANCE_CONFIG_COMPARATOR));
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
      return stateInfoMap.get(state);
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
      tagToResourceMap = new HashMap<>();
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
      stateInfoMap = new HashMap<>();
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

  private static Comparator<InstanceConfig> INSTANCE_CONFIG_COMPARATOR =
      new Comparator<InstanceConfig>() {
        @Override
        public int compare(InstanceConfig config1, InstanceConfig config2) {
          if (config1 == config2) {
            return 0;
          }
          if (config1 == null) {
            return -1;
          }
          if (config2 == null) {
            return 1;
          }
          // IDs for InstanceConfigs are a concatenation of instance name, host, and port.
          return config1.getId().compareTo(config2.getId());
        }
      };
}
