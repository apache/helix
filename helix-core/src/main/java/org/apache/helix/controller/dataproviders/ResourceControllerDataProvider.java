package org.apache.helix.controller.dataproviders;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.caches.AbstractDataCache;
import org.apache.helix.common.caches.CustomizedStateCache;
import org.apache.helix.common.caches.CustomizedViewCache;
import org.apache.helix.common.caches.PropertyCache;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.stages.MissingTopStateRecord;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data provider for resource controller.
 *
 * This class will be moved to helix-resource-controller module in the future
 */
public class ResourceControllerDataProvider extends BaseControllerDataProvider {
  private static final Logger logger =
      LoggerFactory.getLogger(ResourceControllerDataProvider.class);
  private static final String PIPELINE_NAME = Pipeline.Type.DEFAULT.name();

  // Resource control specific property caches
  private final PropertyCache<ExternalView> _externalViewCache;
  private final PropertyCache<ExternalView> _targetExternalViewCache;
  private final CustomizedStateCache _customizedStateCache;
  // a map from customized state type to customized view cache
  private final Map<String, CustomizedViewCache> _customizedViewCacheMap;

  // maintain a cache of bestPossible assignment across pipeline runs
  // TODO: this is only for customRebalancer, remove it and merge it with _idealMappingCache.
  private Map<String, ResourceAssignment> _resourceAssignmentCache;

  // maintain a cache of idealmapping (preference list) for full-auto resource across pipeline runs
  private Map<String, ZNRecord> _idealMappingCache;

  // records for top state handoff
  private Map<String, Map<String, MissingTopStateRecord>> _missingTopStateMap;
  private Map<String, Map<String, String>> _lastTopStateLocationMap;

  // Maintain a set of all ChangeTypes for change detection
  private Set<HelixConstants.ChangeType> _refreshedChangeTypes;
  private Set<String> _aggregationEnabledTypes = new HashSet<>();

  // CrushEd strategy needs to have a stable partition list input. So this cached list persist the
  // previous seen partition lists. If the members in a list are not modified, the old list will be
  // used in the algorithm to ensure it calculates the same result.
  // Refer to https://github.com/apache/helix/issues/940.
  // TODO: Sorting the partition list in CrushEd strategy instead of using the cache to reduce
  // TODO: dependency. Note that this will change the cluster partition assignment and potentially
  // TODO: cause shuffling. So it is not backward compatible.
  private final Map<String, List<String>> _stablePartitionListCache = new HashMap<>();

  public ResourceControllerDataProvider() {
    this(AbstractDataCache.UNKNOWN_CLUSTER);
  }

  public ResourceControllerDataProvider(String clusterName) {
    super(clusterName, PIPELINE_NAME);
    _externalViewCache = new PropertyCache<>(this, "ExternalView", new PropertyCache.PropertyCacheKeyFuncs<ExternalView>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().externalViews();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().externalView(objName);
      }

      @Override
      public String getObjName(ExternalView obj) {
        return obj.getResourceName();
      }
    }, true);
    _targetExternalViewCache = new PropertyCache<>(this, "TargetExternalView", new PropertyCache.PropertyCacheKeyFuncs<ExternalView>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().targetExternalViews();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().targetExternalView(objName);
      }

      @Override
      public String getObjName(ExternalView obj) {
        return obj.getResourceName();
      }
    }, true);
    _resourceAssignmentCache = new HashMap<>();
    _idealMappingCache = new HashMap<>();
    _missingTopStateMap = new HashMap<>();
    _lastTopStateLocationMap = new HashMap<>();
    _refreshedChangeTypes = ConcurrentHashMap.newKeySet();
    _customizedStateCache = new CustomizedStateCache(this, _aggregationEnabledTypes);
    _customizedViewCacheMap = new HashMap<>();
  }

  public synchronized void refresh(HelixDataAccessor accessor) {
    long startTime = System.currentTimeMillis();

    // Refresh base
    Set<HelixConstants.ChangeType> changedTypes = super.doRefresh(accessor);
    _refreshedChangeTypes.addAll(changedTypes);

    // Invalidate cached information if any of the important data has been refreshed
    if (changedTypes.contains(HelixConstants.ChangeType.IDEAL_STATE)
        || changedTypes.contains(HelixConstants.ChangeType.LIVE_INSTANCE)
        || changedTypes.contains(HelixConstants.ChangeType.INSTANCE_CONFIG)
        || changedTypes.contains(HelixConstants.ChangeType.RESOURCE_CONFIG)
        || changedTypes.contains((HelixConstants.ChangeType.CLUSTER_CONFIG))) {
      clearCachedResourceAssignments();
    }

    // Refresh resource controller specific property caches
    refreshCustomizedStateConfig(accessor);
    _customizedStateCache.setAggregationEnabledTypes(_aggregationEnabledTypes);
    _customizedStateCache.refresh(accessor, getLiveInstanceCache().getPropertyMap());
    refreshExternalViews(accessor);
    refreshTargetExternalViews(accessor);
    refreshCustomizedViewMap(accessor);

    // This is part of the backward compatible workaround to fix
    // https://github.com/apache/helix/issues/940.
    // TODO: remove the workaround once we are able to apply the simple fix without majorly
    // TODO: impacting user's clusters.
    refreshStablePartitionList(getIdealStates());

    LogUtil.logInfo(logger, getClusterEventId(), String.format(
        "END: ResourceControllerDataProvider.refresh() for cluster %s, started at %d took %d for %s pipeline",
        getClusterName(), startTime, System.currentTimeMillis() - startTime, getPipelineName()));
    dumpDebugInfo();
  }

  protected void dumpDebugInfo() {
    super.dumpDebugInfo();

    if (logger.isTraceEnabled()) {
      logger.trace("Cache content: " + toString());
    }
  }

  private void refreshCustomizedStateConfig(final HelixDataAccessor accessor) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.CUSTOMIZED_STATE_CONFIG)
        .getAndSet(false)) {
      CustomizedStateConfig customizedStateConfig =
          accessor.getProperty(accessor.keyBuilder().customizedStateConfig());
      if (customizedStateConfig != null) {
        _aggregationEnabledTypes =
            new HashSet<>(customizedStateConfig.getAggregationEnabledTypes());
      } else {
        _aggregationEnabledTypes.clear();
      }
      LogUtil.logInfo(logger, getClusterEventId(), String
          .format("Reloaded CustomizedStateConfig for cluster %s, %s pipeline.",
              getClusterName(), getPipelineName()));
    } else {
      LogUtil.logInfo(logger, getClusterEventId(), String
          .format("No customized state config change for %s cluster, %s pipeline",
              getClusterName(), getPipelineName()));
    }
  }

  private void refreshExternalViews(final HelixDataAccessor accessor) {
    // As we are not listening on external view change, external view will be
    // refreshed once during the cache's first refresh() call, or when full refresh is required
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.EXTERNAL_VIEW).getAndSet(false)) {
      synchronized (_externalViewCache) {
        _externalViewCache.refresh(accessor);
      }
    }
  }

  private void refreshTargetExternalViews(final HelixDataAccessor accessor) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW).getAndSet(false)) {
      if (getClusterConfig() != null && getClusterConfig().isTargetExternalViewEnabled()) {
        // Only refresh with data accessor for the first time
        _targetExternalViewCache.refresh(accessor);
      }
    }
  }

  public void refreshCustomizedViewMap(final HelixDataAccessor accessor) {
    // As we are not listening on customized view change, customized view will be
    // refreshed once during the cache's first refresh() call, or when full refresh is required
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.CUSTOMIZED_VIEW).getAndSet(false)) {
      for (String stateType : _aggregationEnabledTypes) {
        if (!_customizedViewCacheMap.containsKey(stateType)) {
          CustomizedViewCache newCustomizedViewCache =
              new CustomizedViewCache(getClusterName(), stateType);
          _customizedViewCacheMap.put(stateType, newCustomizedViewCache);
        }
        _customizedViewCacheMap.get(stateType).refresh(accessor);
      }
      Set<String> previousCachedStateTypes = new HashSet<>(_customizedViewCacheMap.keySet());
      previousCachedStateTypes.removeAll(_aggregationEnabledTypes);
      logger.info("Remove customizedView for state: " + previousCachedStateTypes);
      removeCustomizedViewTypes(previousCachedStateTypes);
    }
  }

  /**
   * Provides the customized state of the node for a given state type (resource -> customizedState)
   * @param instanceName
   * @param customizedStateType
   * @return
   */
  public Map<String, CustomizedState> getCustomizedState(String instanceName,
      String customizedStateType) {
    return _customizedStateCache.getParticipantState(instanceName, customizedStateType);
  }

  public Set<String> getAggregationEnabledCustomizedStateTypes() {
    return _aggregationEnabledTypes;
  }

  protected void setAggregationEnabledCustomizedStateTypes(Set<String> aggregationEnabledTypes) {
    _aggregationEnabledTypes = aggregationEnabledTypes;
  }

  public ExternalView getTargetExternalView(String resourceName) {
    return _targetExternalViewCache.getPropertyByName(resourceName);
  }

  public void updateTargetExternalView(String resourceName, ExternalView targetExternalView) {
    _targetExternalViewCache.setProperty(targetExternalView);
  }

  /**
   * Get local cached external view map
   * @return
   */
  public Map<String, ExternalView> getExternalViews() {
    synchronized (_externalViewCache) {
      return _externalViewCache.getPropertyMap();
    }
  }

  /**
   * Update the cached external view map
   * @param externalViews
   */
  public void updateExternalViews(List<ExternalView> externalViews) {
    synchronized (_externalViewCache) {
      for (ExternalView ev : externalViews) {
        _externalViewCache.setProperty(ev);
      }
    }
  }

  /**
   * Update the cached customized view map
   * @param customizedViews
   */
  public void updateCustomizedViews(String customizedStateType,
      List<CustomizedView> customizedViews) {
    if (!_customizedViewCacheMap.containsKey(customizedStateType)) {
      CustomizedViewCache customizedViewCache =
          new CustomizedViewCache(getClusterName(), customizedStateType);
      _customizedViewCacheMap.put(customizedStateType, customizedViewCache);
    }
    for (CustomizedView cv : customizedViews) {
      _customizedViewCacheMap.get(customizedStateType).getCustomizedViewCache().setProperty(cv);
    }
  }

  /**
   * Get local cached customized view map
   * @return
   */
  public Map<String, CustomizedViewCache> getCustomizedViewCacheMap() {
    return _customizedViewCacheMap;
  }

  /**
   * Remove dead external views from map
   * @param resourceNames
   */

  public void removeExternalViews(List<String> resourceNames) {
    synchronized (_externalViewCache) {
      for (String resourceName : resourceNames) {
        _externalViewCache.deletePropertyByName(resourceName);
      }
    }
  }

  /**
   * Remove dead customized views for certain state types from map
   * @param stateTypeNames
   */

  public void removeCustomizedViewTypes(Set<String> stateTypeNames) {
    for (String stateType : stateTypeNames) {
      _customizedViewCacheMap.remove(stateType);
    }
  }

  /**
   * Remove dead customized views for a certain state type from customized view cache
   * @param stateType a specific customized state type
   * @param resourceNames the names of resources whose customized view is stale
   */
  public void removeCustomizedViews(String stateType, List<String> resourceNames) {
    if (!_customizedViewCacheMap.containsKey(stateType)) {
      logger.warn(String.format("The customized state type : %s is not in the cache", stateType));
      return;
    }
    for (String resourceName : resourceNames) {
      _customizedViewCacheMap.get(stateType).getCustomizedViewCache()
          .deletePropertyByName(resourceName);
    }
  }

  public Map<String, Map<String, MissingTopStateRecord>> getMissingTopStateMap() {
    return _missingTopStateMap;
  }

  public Map<String, Map<String, String>> getLastTopStateLocationMap() {
    return _lastTopStateLocationMap;
  }

  /**
   * Get cached resourceAssignment (bestPossible mapping) for a resource
   * @param resource
   * @return
   */
  public ResourceAssignment getCachedResourceAssignment(String resource) {
    return _resourceAssignmentCache.get(resource);
  }

  /**
   * Get cached resourceAssignments
   * @return
   */
  public Map<String, ResourceAssignment> getCachedResourceAssignments() {
    return Collections.unmodifiableMap(_resourceAssignmentCache);
  }

  /**
   * Cache resourceAssignment (bestPossible mapping) for a resource
   * @param resource
   * @return
   */
  public void setCachedResourceAssignment(String resource, ResourceAssignment resourceAssignment) {
    _resourceAssignmentCache.put(resource, resourceAssignment);
  }

  /**
   * Get cached resourceAssignment (ideal mapping) for a resource
   * @param resource
   * @return
   */
  public ZNRecord getCachedIdealMapping(String resource) {
    return _idealMappingCache.get(resource);
  }

  /**
   * Invalidate the cached resourceAssignment (ideal mapping) for a resource
   * @param resource
   */
  public void invalidateCachedIdealStateMapping(String resource) {
    _idealMappingCache.remove(resource);
  }

  /**
   * Get cached idealmapping
   * @return
   */
  public Map<String, ZNRecord> getCachedIdealMapping() {
    return Collections.unmodifiableMap(_idealMappingCache);
  }

  /**
   * Cache resourceAssignment (ideal mapping) for a resource
   * @param resource
   * @return
   */
  public void setCachedIdealMapping(String resource, ZNRecord mapping) {
    _idealMappingCache.put(resource, mapping);
  }

  /**
   * Return the set of all PropertyTypes that changed prior to this round of rebalance. The caller
   * should clear this set by calling {@link #clearRefreshedChangeTypes()}.
   * @return
   */
  public Set<HelixConstants.ChangeType> getRefreshedChangeTypes() {
    return _refreshedChangeTypes;
  }

  /**
   * Clears the set of all PropertyTypes that changed. The caller will have consumed all change
   * types by calling {@link #getRefreshedChangeTypes()}.
   */
  public void clearRefreshedChangeTypes() {
    _refreshedChangeTypes.clear();
  }

  public void clearCachedResourceAssignments() {
    _resourceAssignmentCache.clear();
    _idealMappingCache.clear();
  }

  public void clearMonitoringRecords() {
    _missingTopStateMap.clear();
    _lastTopStateLocationMap.clear();
  }

  /**
   * This is for a backward compatible workaround to fix https://github.com/apache/helix/issues/940.
   *
   * @param resourceName
   * @return the cached stable partition list of the specified resource. If no such cached item,
   * return null.
   */
  public List<String> getStablePartitionList(String resourceName) {
    return _stablePartitionListCache.get(resourceName);
  }

  /**
   * Refresh the stable partition list cache items and remove the non-exist resources' cache.
   * This is for a backward compatible workaround to fix https://github.com/apache/helix/issues/940.
   *
   * @param idealStateMap
   */
  final void refreshStablePartitionList(Map<String, IdealState> idealStateMap) {
    _stablePartitionListCache.keySet().retainAll(idealStateMap.keySet());
    for (String resourceName : idealStateMap.keySet()) {
      Set<String> newPartitionSet = idealStateMap.get(resourceName).getPartitionSet();
      List<String> cachedPartitionList = getStablePartitionList(resourceName);
      if (cachedPartitionList == null || cachedPartitionList.size() != newPartitionSet.size()
          || !newPartitionSet.containsAll(cachedPartitionList)) {
        _stablePartitionListCache.put(resourceName, new ArrayList<>(newPartitionSet));
      }
    }
  }
}
