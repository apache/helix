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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

public class RoutingTableProvider implements ExternalViewChangeListener,
    InstanceConfigChangeListener {
  private static final Logger logger = Logger.getLogger(RoutingTableProvider.class);
  private final AtomicReference<RoutingTable> _routingTableRef;

  public RoutingTableProvider() {
    _routingTableRef = new AtomicReference<RoutingTableProvider.RoutingTable>(new RoutingTable());

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
  public List<InstanceConfig> getInstances(String resourceName, String partitionName, String state) {
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
   * returns all instances for {resource} that are in a specific {state}
   * @param resource
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstances(String resource, String state) {
    Set<InstanceConfig> instanceSet = null;
    RoutingTable routingTable = _routingTableRef.get();
    ResourceInfo resourceInfo = routingTable.get(resource);
    if (resourceInfo != null) {
      instanceSet = resourceInfo.getInstances(state);
    }
    if (instanceSet == null) {
      instanceSet = Collections.emptySet();
    }
    return instanceSet;
  }

  /**
   * Get the configuration of an instance from its name
   * @param instanceName the instance ID
   * @return InstanceConfig if present, null otherwise
   */
  public InstanceConfig getInstanceConfig(String instanceName) {
    return _routingTableRef.get().getConfig(instanceName);
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
  public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
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

    RoutingTable newRoutingTable = new RoutingTable();
    List<InstanceConfig> configList = accessor.getChildValues(keyBuilder.instanceConfigs());
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<String, InstanceConfig>();
    for (InstanceConfig config : configList) {
      instanceConfigMap.put(config.getId(), config);
      newRoutingTable.addConfig(config);
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
              newRoutingTable.addEntry(resourceName, partitionName, currentState, instanceConfig);
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
    private final HashMap<String, ResourceInfo> resourceInfoMap;
    private final Map<String, InstanceConfig> instanceConfigMap;

    public RoutingTable() {
      resourceInfoMap = new HashMap<String, RoutingTableProvider.ResourceInfo>();
      instanceConfigMap = new HashMap<String, InstanceConfig>();
    }

    public void addEntry(String resourceName, String partitionName, String state,
        InstanceConfig config) {
      if (!resourceInfoMap.containsKey(resourceName)) {
        resourceInfoMap.put(resourceName, new ResourceInfo());
      }
      ResourceInfo resourceInfo = resourceInfoMap.get(resourceName);
      resourceInfo.addEntry(partitionName, state, config);

    }

    public void addConfig(InstanceConfig config) {
      instanceConfigMap.put(config.getInstanceName(), config);
    }

    ResourceInfo get(String resourceName) {
      return resourceInfoMap.get(resourceName);
    }

    InstanceConfig getConfig(String instanceName) {
      return instanceConfigMap.get(instanceName);
    }
  }

  class ResourceInfo {
    // store PartitionInfo for each partition
    HashMap<String, PartitionInfo> partitionInfoMap;
    // stores the Set of Instances in a given state
    HashMap<String, Set<InstanceConfig>> stateInfoMap;

    public ResourceInfo() {
      partitionInfoMap = new HashMap<String, RoutingTableProvider.PartitionInfo>();
      stateInfoMap = new HashMap<String, Set<InstanceConfig>>();
    }

    public void addEntry(String stateUnitKey, String state, InstanceConfig config) {
      // add
      if (!stateInfoMap.containsKey(state)) {
        Comparator<InstanceConfig> comparator = new Comparator<InstanceConfig>() {

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
        stateInfoMap.put(state, new TreeSet<InstanceConfig>(comparator));
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

  class PartitionInfo {
    HashMap<String, List<InstanceConfig>> stateInfoMap;

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
  }
}
