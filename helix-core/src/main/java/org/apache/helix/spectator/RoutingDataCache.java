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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyType;
import org.apache.helix.common.caches.BasicClusterDataCache;
import org.apache.helix.common.caches.CurrentStateCache;
import org.apache.helix.common.caches.CurrentStateSnapshot;
import org.apache.helix.common.caches.CustomizedViewCache;
import org.apache.helix.common.caches.TargetExternalViewCache;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cache the cluster data that are needed by RoutingTableProvider.
 */
class RoutingDataCache extends BasicClusterDataCache {
  private static Logger LOG = LoggerFactory.getLogger(RoutingDataCache.class.getName());

  private final Map<PropertyType, List<String>> _sourceDataTypeMap;

  private CurrentStateCache _currentStateCache;
  // TODO: CustomizedCache needs to be migrated to propertyCache. Once we migrate all cache to
  // propertyCache, this hardcoded list of fields won't be necessary.
  private Map<String, CustomizedViewCache> _customizedViewCaches;
  private TargetExternalViewCache _targetExternalViewCache;
  private Map<String, LiveInstance> _routableLiveInstanceMap;
  private Map<String, InstanceConfig> _routableInstanceConfigMap;

  public RoutingDataCache(String clusterName, PropertyType sourceDataType) {
    this (clusterName, ImmutableMap.of(sourceDataType, Collections.emptyList()));
  }

  /**
   * Initialize empty RoutingDataCache with clusterName, _propertyTypes.
   * @param clusterName
   * @param sourceDataTypeMap
   */
  public RoutingDataCache(String clusterName, Map<PropertyType, List<String>> sourceDataTypeMap) {
    super(clusterName);
    _sourceDataTypeMap = sourceDataTypeMap;
    _currentStateCache = new CurrentStateCache(clusterName);
    _customizedViewCaches = new HashMap<>();
    sourceDataTypeMap.getOrDefault(PropertyType.CUSTOMIZEDVIEW, Collections.emptyList())
        .forEach(customizedStateType -> _customizedViewCaches.put(customizedStateType,
            new CustomizedViewCache(clusterName, customizedStateType)));
    _targetExternalViewCache = new TargetExternalViewCache(clusterName);
    _routableInstanceConfigMap = new HashMap<>();
    _routableLiveInstanceMap = new HashMap<>();
    requireFullRefresh();
  }

  /**
   * This refreshes the cluster data by re-fetching the data from zookeeper in an efficient way
   *
   * @param accessor
   *
   * @return
   */
  @Override
  public synchronized void refresh(HelixDataAccessor accessor) {
    LOG.info("START: RoutingDataCache.refresh() for cluster " + _clusterName);
    long startTime = System.currentTimeMillis();

    // Store whether a refresh for routable instances is necessary, as the super.refresh() call will
    // set the _propertyDataChangedMap values for the instance config and live instance change types to false.
    boolean refreshRoutableInstanceConfigs =
        _propertyDataChangedMap.getOrDefault(HelixConstants.ChangeType.INSTANCE_CONFIG, false);
    // If there is an InstanceConfig change, update the routable instance configs and live instances.
    // Must also do live instances because whether and instance is routable is based off of the instance config.
    boolean refreshRoutableLiveInstances =
        _propertyDataChangedMap.getOrDefault(HelixConstants.ChangeType.LIVE_INSTANCE, false)
            || refreshRoutableInstanceConfigs;

    super.refresh(accessor);

    if (refreshRoutableInstanceConfigs) {
      updateRoutableInstanceConfigMap(_instanceConfigPropertyCache.getPropertyMap());
    }
    if (refreshRoutableLiveInstances) {
      updateRoutableLiveInstanceMap(getRoutableInstanceConfigMap(),
          _liveInstancePropertyCache.getPropertyMap());
    }

    for (PropertyType propertyType : _sourceDataTypeMap.keySet()) {
      long start = System.currentTimeMillis();
      switch (propertyType) {
      case TARGETEXTERNALVIEW:
        if (_propertyDataChangedMap.get(HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW)) {
          _propertyDataChangedMap.put(HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW, false);
          _targetExternalViewCache.refresh(accessor);
          LOG.info("Reload " + _targetExternalViewCache.getExternalViewMap().keySet().size()
              + " TargetExternalViews. Takes " + (System.currentTimeMillis() - start) + " ms");
        }
        break;
      case CURRENTSTATES:
        if (_propertyDataChangedMap.get(HelixConstants.ChangeType.CURRENT_STATE)) {
          _propertyDataChangedMap.put(HelixConstants.ChangeType.CURRENT_STATE, false);
          /**
           * Workaround of https://github.com/apache/helix/issues/919.
           * Why it is workaround?
           * 1. Before a larger scale refactoring, to minimize the impact on cache logic, this change
           * introduces extra read to update the liveInstance list before processing current states.
           * 2. This change does not handle the corresponding callback handlers, which should also be
           * registered when a new liveInstance node is found.
           * TODO: Refactor cache processing logic and also refine the callback handler registration
           * TODO: logic.
           **/
          _liveInstancePropertyCache.refresh(accessor);
          updateRoutableLiveInstanceMap(getRoutableInstanceConfigMap(),
              _liveInstancePropertyCache.getPropertyMap());
          Map<String, LiveInstance> liveInstanceMap = getRoutableLiveInstances();
          _currentStateCache.refresh(accessor, liveInstanceMap);
          LOG.info("Reload CurrentStates. Takes " + (System.currentTimeMillis() - start) + " ms");
        }
        break;
      case CUSTOMIZEDVIEW: {
        if (_propertyDataChangedMap.get(HelixConstants.ChangeType.CUSTOMIZED_VIEW)) {
          for (String customizedStateType : _sourceDataTypeMap.get(PropertyType.CUSTOMIZEDVIEW)) {
            _customizedViewCaches.get(customizedStateType).refresh(accessor);
          }
          LOG.info("Reload CustomizedView for types "
              + _sourceDataTypeMap.get(PropertyType.CUSTOMIZEDVIEW) + " Takes "
              + (System.currentTimeMillis() - start) + " ms");
        }
        _propertyDataChangedMap.put(HelixConstants.ChangeType.CUSTOMIZED_VIEW, false);
      }
        break;
      default:
        break;
      }
    }
    long endTime = System.currentTimeMillis();
    LOG.info("END: RoutingDataCache.refresh() for cluster " + _clusterName + ", took " + (endTime
        - startTime) + " ms");

    LOG.debug("CurrentStates: {}", _currentStateCache);
    LOG.debug("TargetExternalViews: {}", _targetExternalViewCache.getExternalViewMap());
    if (LOG.isDebugEnabled()) {
      for (String customizedStateType : _sourceDataTypeMap
          .getOrDefault(PropertyType.CUSTOMIZEDVIEW, Collections.emptyList())) {
        LOG.debug("CustomizedViews customizedStateType: {} {}", customizedStateType,
            _customizedViewCaches.get(customizedStateType).getCustomizedViewMap());
      }
    }
  }

  private void updateRoutableInstanceConfigMap(Map<String, InstanceConfig> instanceConfigMap) {
    _routableInstanceConfigMap = instanceConfigMap.entrySet().stream().filter(
            (instanceConfigEntry) -> !InstanceConstants.UNROUTABLE_INSTANCE_OPERATIONS.contains(
                instanceConfigEntry.getValue().getInstanceOperation().getOperation()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private void updateRoutableLiveInstanceMap(Map<String, InstanceConfig> instanceConfigMap,
      Map<String, LiveInstance> liveInstanceMap) {
    _routableLiveInstanceMap = liveInstanceMap.entrySet().stream().filter(
            (liveInstanceEntry) -> instanceConfigMap.containsKey(liveInstanceEntry.getKey())
                && !InstanceConstants.UNROUTABLE_INSTANCE_OPERATIONS.contains(
                instanceConfigMap.get(liveInstanceEntry.getKey()).getInstanceOperation()
                    .getOperation()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Returns the LiveInstances for each of the routable instances that are currently up and
   * running.
   *
   * @return a map of LiveInstances
   */
  public Map<String, LiveInstance> getRoutableLiveInstances() {
    return Collections.unmodifiableMap(_routableLiveInstanceMap);
  }

  /**
   * Returns the instance config map for all the routable instances that are in the cluster.
   *
   * @return a map of InstanceConfigs
   */
  public Map<String, InstanceConfig> getRoutableInstanceConfigMap() {
    return Collections.unmodifiableMap(_routableInstanceConfigMap);
  }

  /**
   * Retrieves the TargetExternalView for all resources
   *
   * @return
   */
  public Map<String, ExternalView> getTargetExternalViews() {
    return _targetExternalViewCache.getExternalViewMap();
  }

  /**
   * Retrieves the CustomizedView for all resources
   * @return
   */
  public Map<String, CustomizedView> getCustomizedView(String customizedStateType) {
    if (_customizedViewCaches.containsKey(customizedStateType)) {
      return _customizedViewCaches.get(customizedStateType).getCustomizedViewMap();
    }
    throw new HelixException(String.format(
        "customizedStateType %s does not exist in customizedViewCaches.", customizedStateType));
  }

  /**
   * Get map of current states in cluster. {InstanceName -> {SessionId -> {ResourceName ->
   * CurrentState}}}
   *
   * @return
   */
  public Map<String, Map<String, Map<String, CurrentState>>> getCurrentStatesMap() {
    return _currentStateCache.getParticipantStatesMap();
  }

  public CurrentStateSnapshot getCurrentStateSnapshot() {
    return _currentStateCache.getSnapshot();
  }
}

