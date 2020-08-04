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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyType;
import org.apache.helix.common.caches.BasicClusterDataCache;
import org.apache.helix.common.caches.CurrentStateCache;
import org.apache.helix.common.caches.CurrentStateSnapshot;
import org.apache.helix.common.caches.CustomizedViewCache;
import org.apache.helix.common.caches.TargetExternalViewCache;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
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

    super.refresh(accessor);
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
          Map<String, LiveInstance> liveInstanceMap = getLiveInstances();
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

    if (LOG.isDebugEnabled()) {
      LOG.debug("CurrentStates: " + _currentStateCache);
      LOG.debug("TargetExternalViews: " + _targetExternalViewCache.getExternalViewMap());
      for (String customizedStateType : _sourceDataTypeMap.getOrDefault(PropertyType.CUSTOMIZEDVIEW,
          Collections.emptyList())) {
        LOG.debug("CustomizedViews customizedStateType: " + customizedStateType + " "
            + _customizedViewCaches.get(customizedStateType).getCustomizedViewMap());
      }
    }
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

