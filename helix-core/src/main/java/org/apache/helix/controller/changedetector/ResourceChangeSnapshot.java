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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.controller.changedetector.trimmer.ClusterConfigTrimmer;
import org.apache.helix.controller.changedetector.trimmer.IdealStateTrimmer;
import org.apache.helix.controller.changedetector.trimmer.InstanceConfigTrimmer;
import org.apache.helix.controller.changedetector.trimmer.ResourceConfigTrimmer;
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
  private Map<String, InstanceConfig> _allInstanceConfigMap;
  private Map<String, InstanceConfig> _assignableInstanceConfigMap;
  private Map<String, IdealState> _idealStateMap;
  private Map<String, ResourceConfig> _resourceConfigMap;
  private Map<String, LiveInstance> _allLiveInstances;
  private Map<String, LiveInstance> _assignableLiveInstances;
  private ClusterConfig _clusterConfig;

  /**
   * Default constructor that constructs an empty snapshot.
   */
  ResourceChangeSnapshot() {
    _changedTypes = new HashSet<>();
    _allInstanceConfigMap = new HashMap<>();
    _assignableInstanceConfigMap = new HashMap<>();
    _idealStateMap = new HashMap<>();
    _resourceConfigMap = new HashMap<>();
    _allLiveInstances = new HashMap<>();
    _assignableLiveInstances = new HashMap<>();
    _clusterConfig = null;
  }

  /**
   * Constructor using controller cache (ResourceControllerDataProvider).
   *
   * @param dataProvider
   * @param ignoreNonTopologyChange if true, the snapshot won't record any trivial changes that
   *                                 do not impact the fundamental structure of the cluster.
   *                                 For example, instance disabled or not, rebalance throttling
   *                                 configurations, resource disabled or not, etc.
   */
  ResourceChangeSnapshot(ResourceControllerDataProvider dataProvider,
      boolean ignoreNonTopologyChange) {
    _changedTypes = new HashSet<>(dataProvider.getRefreshedChangeTypes());
    _allInstanceConfigMap = ignoreNonTopologyChange ?
        dataProvider.getInstanceConfigMap().entrySet().parallelStream().collect(Collectors
            .toMap(e -> e.getKey(),
                e -> InstanceConfigTrimmer.getInstance().trimProperty(e.getValue()))) :
        new HashMap<>(dataProvider.getInstanceConfigMap());
    _assignableInstanceConfigMap = ignoreNonTopologyChange ?
        dataProvider.getAssignableInstanceConfigMap().entrySet().parallelStream().collect(Collectors
            .toMap(e -> e.getKey(),
                e -> InstanceConfigTrimmer.getInstance().trimProperty(e.getValue()))) :
        new HashMap<>(dataProvider.getAssignableInstanceConfigMap());
    _idealStateMap = ignoreNonTopologyChange ?
        dataProvider.getIdealStates().entrySet().parallelStream().collect(Collectors
            .toMap(e -> e.getKey(),
                e -> IdealStateTrimmer.getInstance().trimProperty(e.getValue()))) :
        new HashMap<>(dataProvider.getIdealStates());
    _resourceConfigMap = ignoreNonTopologyChange ?
        dataProvider.getResourceConfigMap().entrySet().parallelStream().collect(Collectors
            .toMap(e -> e.getKey(),
                e -> ResourceConfigTrimmer.getInstance().trimProperty(e.getValue()))) :
        new HashMap<>(dataProvider.getResourceConfigMap());
    _clusterConfig = ignoreNonTopologyChange ?
        ClusterConfigTrimmer.getInstance().trimProperty(dataProvider.getClusterConfig()) :
        dataProvider.getClusterConfig();
    _allLiveInstances = new HashMap<>(dataProvider.getLiveInstances());
    _assignableLiveInstances = new HashMap<>(dataProvider.getAssignableLiveInstances());
  }

  /**
   * Copy constructor for ResourceChangeCache.
   * @param snapshot
   */
  ResourceChangeSnapshot(ResourceChangeSnapshot snapshot) {
    _changedTypes = new HashSet<>(snapshot._changedTypes);
    _allInstanceConfigMap = new HashMap<>(snapshot._allInstanceConfigMap);
    _assignableInstanceConfigMap = new HashMap<>(snapshot._assignableInstanceConfigMap);
    _idealStateMap = new HashMap<>(snapshot._idealStateMap);
    _resourceConfigMap = new HashMap<>(snapshot._resourceConfigMap);
    _allLiveInstances = new HashMap<>(snapshot._allLiveInstances);
    _assignableLiveInstances = new HashMap<>(snapshot._assignableLiveInstances);
    _clusterConfig = snapshot._clusterConfig;
  }

  Set<HelixConstants.ChangeType> getChangedTypes() {
    return _changedTypes;
  }

  Map<String, InstanceConfig> getInstanceConfigMap() {
    return _allInstanceConfigMap;
  }

  Map<String, InstanceConfig> getAssignableInstanceConfigMap() {
    return _assignableInstanceConfigMap;
  }

  Map<String, IdealState> getIdealStateMap() {
    return _idealStateMap;
  }

  Map<String, ResourceConfig> getResourceConfigMap() {
    return _resourceConfigMap;
  }

  Map<String, LiveInstance> getLiveInstances() {
    return _allLiveInstances;
  }

  Map<String, LiveInstance> getAssignableLiveInstances() {
    return _assignableLiveInstances;
  }

  ClusterConfig getClusterConfig() {
    return _clusterConfig;
  }
}
