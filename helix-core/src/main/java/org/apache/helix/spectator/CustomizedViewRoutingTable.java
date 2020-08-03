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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.helix.PropertyType;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomizedViewRoutingTable extends RoutingTable {
  private static final Logger logger = LoggerFactory.getLogger(CustomizedViewRoutingTable.class);

  private final Collection<CustomizedView> _customizedViews;
  /*
   * The customizedStateType field is the type the controller is aggregating.
   * For example if RoutingTableProvider initialized using the code below:
   * Map<PropertyType, List<String>> sourceDataTypes = new HashMap<>();
   * sourceDataTypes.put(PropertyType.CUSTOMIZEDVIEW, Arrays.asList("typeA", "typeB"));
   * RoutingTableProvider routingTableProvider =
   * new RoutingTableProvider(_spectator, sourceDataTypes);
   * Each one of the TypeA and TypeB is a customizedStateType.
   */
  private final String _customizedStateType;

  public CustomizedViewRoutingTable(PropertyType propertyType, String customizedStateType) {
    this(Collections.<CustomizedView> emptyList(), propertyType, customizedStateType);
  }

  protected CustomizedViewRoutingTable(Collection<CustomizedView> customizedViews,
      PropertyType propertytype, String customizedStateType) {
    this(customizedViews, Collections.<InstanceConfig> emptyList(),
        Collections.<LiveInstance> emptyList(), propertytype, customizedStateType);
  }

  protected CustomizedViewRoutingTable(Collection<CustomizedView> customizedViews,
      Collection<InstanceConfig> instanceConfigs, Collection<LiveInstance> liveInstances,
      PropertyType propertytype, String customizedStateType) {
    super(Collections.<ExternalView> emptyList(), instanceConfigs, liveInstances,
        PropertyType.CUSTOMIZEDVIEW);
    _customizedStateType = customizedStateType;
    _customizedViews = new HashSet<>(customizedViews);
    refresh(_customizedViews);
  }

  private void refresh(Collection<CustomizedView> customizedViewList) {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    if (customizedViewList != null && !customizedViewList.isEmpty()) {
      for (InstanceConfig config : _instanceConfigs) {
        instanceConfigMap.put(config.getId(), config);
      }
      for (CustomizedView customizeView : customizedViewList) {
        String resourceName = customizeView.getId();
        for (String partitionName : customizeView.getPartitionSet()) {
          Map<String, String> stateMap = customizeView.getStateMap(partitionName);
          for (String instanceName : stateMap.keySet()) {
            String customizedState = stateMap.get(instanceName);
            if (instanceConfigMap.containsKey(instanceName)) {
              InstanceConfig instanceConfig = instanceConfigMap.get(instanceName);
              addEntry(resourceName, partitionName, customizedState, instanceConfig);
            } else {
              logger.warn(
                  "Participant {} is not found with proper configuration information. It might already be removed from the cluster. "
                      + "Skip recording partition assignment entry: Partition {}, Participant {}, State {}.",
                  instanceName, partitionName, instanceName, stateMap.get(instanceName));
            }
          }
        }
      }
    }
  }

  /**
   * Returns CustomizedView.
   * @return a collection of CustomizedView
   */
  protected Collection<CustomizedView> geCustomizedViews() {
    return Collections.unmodifiableCollection(_customizedViews);
  }

  /**
   * Returns CustomizedStateType
   * @return the CustomizedStateType of this RoutingTable (Used for CustomizedView)
   */
  protected String getStateType() {
    return _customizedStateType;
  }
}
