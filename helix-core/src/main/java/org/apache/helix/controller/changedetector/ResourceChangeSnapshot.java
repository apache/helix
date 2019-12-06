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
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.ZNRecord;
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
   * @param ignoreControllerGeneratedFields if true, the snapshot won't record any changes that is
   *                                        being modified by the controller.
   */
  ResourceChangeSnapshot(ResourceControllerDataProvider dataProvider,
      boolean ignoreControllerGeneratedFields) {
    _changedTypes = new HashSet<>(dataProvider.getRefreshedChangeTypes());
    _instanceConfigMap = new HashMap<>(dataProvider.getInstanceConfigMap());
    _idealStateMap = new HashMap<>(dataProvider.getIdealStates());
    if (ignoreControllerGeneratedFields && (
        dataProvider.getClusterConfig().isPersistBestPossibleAssignment() || dataProvider
            .getClusterConfig().isPersistIntermediateAssignment())) {
      for (String resourceName : _idealStateMap.keySet()) {
        _idealStateMap.put(resourceName, trimIdealState(_idealStateMap.get(resourceName)));
      }
    }
    _resourceConfigMap = new HashMap<>(dataProvider.getResourceConfigMap());
    _liveInstances = new HashMap<>(dataProvider.getLiveInstances());
    _clusterConfig = dataProvider.getClusterConfig();
  }

  /**
   * Copy constructor for ResourceChangeCache.
   * @param snapshot
   */
  ResourceChangeSnapshot(ResourceChangeSnapshot snapshot) {
    _changedTypes = new HashSet<>(snapshot._changedTypes);
    _instanceConfigMap = new HashMap<>(snapshot._instanceConfigMap);
    _idealStateMap = new HashMap<>(snapshot._idealStateMap);
    _resourceConfigMap = new HashMap<>(snapshot._resourceConfigMap);
    _liveInstances = new HashMap<>(snapshot._liveInstances);
    _clusterConfig = snapshot._clusterConfig;
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

  // Trim the IdealState to exclude any controller modified information.
  private IdealState trimIdealState(IdealState originalIdealState) {
    // Clone the IdealState to avoid modifying the objects in the Cluster Data Cache, which might
    // be used by the other stages in the pipeline.
    IdealState trimmedIdealState = new IdealState(originalIdealState.getRecord());
    ZNRecord trimmedIdealStateRecord = trimmedIdealState.getRecord();
    switch (originalIdealState.getRebalanceMode()) {
      // WARNING: the IdealState copy constructor is not really deep copy. So we should not modify
      // the values directly or the cached values will be changed.
      case FULL_AUTO:
        // For FULL_AUTO resources, both map fields and list fields are not considered as data input
        // for the controller. The controller will write to these two types of fields for persisting
        // the assignment mapping.
        trimmedIdealStateRecord.setListFields(trimmedIdealStateRecord.getListFields().keySet().stream().collect(
            Collectors.toMap(partition -> partition, partition -> Collections.emptyList())));
        // Continue to clean up map fields in the SEMI_AUTO case.
      case SEMI_AUTO:
        // For SEMI_AUTO resources, map fields are not considered as data input for the controller.
        // The controller will write to the map fields for persisting the assignment mapping.
        trimmedIdealStateRecord.setMapFields(trimmedIdealStateRecord.getMapFields().keySet().stream().collect(
            Collectors.toMap(partition -> partition, partition -> Collections.emptyMap())));
        break;
      default:
        break;
    }
    return trimmedIdealState;
  }
}
