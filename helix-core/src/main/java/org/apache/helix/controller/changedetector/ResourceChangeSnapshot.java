package org.apache.helix.controller.changedetector;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;

/**
 * ResourceChangeSnapshot is a POJO that contains the following Helix metadata:
 * 1. InstanceConfig
 * 2. IdealState
 * 3. ResourceConfig
 * 4. LiveInstance
 * 5. Changed property types
 * It serves as a snapshot of the main controller cache to enable the difference (change)
 * calculation between two rounds of the pipeline run.
 */
class ResourceChangeSnapshot {

  private Set<HelixConstants.ChangeType> _changedTypes;
  private Map<String, InstanceConfig> _instanceConfigMap;
  private Map<String, IdealState> _idealStateMap;
  private Map<String, ResourceConfig> _resourceConfigMap;
  private Map<String, LiveInstance> _liveInstances;
  private ClusterConfig _clusterConfig;

  /**
   * Default constructor that constructs an empty snapshot.
   */
  ResourceChangeSnapshot() {
    _changedTypes = new HashSet<>();
    _instanceConfigMap = new HashMap<>();
    _idealStateMap = new HashMap<>();
    _resourceConfigMap = new HashMap<>();
    _liveInstances = new HashMap<>();
    _clusterConfig = null;
  }

  /**
   * Constructor using controller cache (ResourceControllerDataProvider).
   *
   * @param dataProvider
   */
  ResourceChangeSnapshot(ResourceControllerDataProvider dataProvider) {
    _changedTypes = new HashSet<>(dataProvider.getRefreshedChangeTypes());
    _instanceConfigMap = new HashMap<>(dataProvider.getInstanceConfigMap());
    _idealStateMap = new HashMap<>(dataProvider.getIdealStates());
    for (String resourceName : _idealStateMap.keySet()) {
      IdealState orgIdealState = _idealStateMap.get(resourceName);
      if (orgIdealState.getRebalanceMode().equals(IdealState.RebalanceMode.FULL_AUTO)) {
        IdealState trimmedIdealState = new IdealState(orgIdealState.getRecord());
        // For FullAuto resources, map fields and list fields in the IdealStates is not user's input.
        // So there is no need to detect any change in these 2 scopes.
        trimmedIdealState.getRecord().setListFields(Collections.emptyMap());
        trimmedIdealState.getRecord().setMapFields(Collections.emptyMap());
        _idealStateMap.put(resourceName, trimmedIdealState);
      }
    }
    _resourceConfigMap = new HashMap<>(dataProvider.getResourceConfigMap());
    _liveInstances = new HashMap<>(dataProvider.getLiveInstances());
    _clusterConfig = dataProvider.getClusterConfig();
  }

  /**
   * Copy constructor for ResourceChangeCache.
   * @param cache
   */
  ResourceChangeSnapshot(ResourceChangeSnapshot cache) {
    _changedTypes = new HashSet<>(cache._changedTypes);
    _instanceConfigMap = new HashMap<>(cache._instanceConfigMap);
    _idealStateMap = new HashMap<>(cache._idealStateMap);
    _resourceConfigMap = new HashMap<>(cache._resourceConfigMap);
    _liveInstances = new HashMap<>(cache._liveInstances);
    _clusterConfig = cache._clusterConfig;
  }

  Set<HelixConstants.ChangeType> getChangedTypes() {
    return _changedTypes;
  }

  Map<String, InstanceConfig> getInstanceConfigMap() {
    return _instanceConfigMap;
  }

  Map<String, IdealState> getIdealStateMap() {
    return _idealStateMap;
  }

  Map<String, ResourceConfig> getResourceConfigMap() {
    return _resourceConfigMap;
  }

  Map<String, LiveInstance> getLiveInstances() {
    return _liveInstances;
  }

  ClusterConfig getClusterConfig() {
    return _clusterConfig;
  }
}
