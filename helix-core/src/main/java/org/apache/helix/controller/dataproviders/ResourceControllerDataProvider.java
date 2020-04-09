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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.caches.AbstractDataCache;
import org.apache.helix.common.caches.PropertyCache;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.stages.MissingTopStateRecord;
import org.apache.helix.model.ExternalView;
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

  // CrushEd strategy needs to have a stable partition list input. So this cached list persist the
  // previous seen partition lists. If the members in a list are not modified, the old list will be
  // used in the algorithm to ensure it calculates the same result.
  // Refer to https://github.com/apache/helix/issues/940.
  // TODO: Sorting the partition list in CrushEd strategy instead of using the cache to reduce
  // TODO: dependency. Note that this will change the cluster partition assignment and potentially
  // TODO: cause shuffling. So it is non backward compatible.
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
    refreshExternalViews(accessor);
    refreshTargetExternalViews(accessor);
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

  private void refreshExternalViews(final HelixDataAccessor accessor) {
    // As we are not listening on external view change, external view will be
    // refreshed once during the cache's first refresh() call, or when full refresh is required
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.EXTERNAL_VIEW).getAndSet(false)) {
      _externalViewCache.refresh(accessor);
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
    return _externalViewCache.getPropertyMap();
  }

  /**
   * Update the cached external view map
   * @param externalViews
   */
  public void updateExternalViews(List<ExternalView> externalViews) {
    for (ExternalView ev : externalViews) {
      _externalViewCache.setProperty(ev);
    }
  }

  /**
   * Remove dead external views from map
   * @param resourceNames
   */

  public void removeExternalViews(List<String> resourceNames) {
    for (String resourceName : resourceNames) {
      _externalViewCache.deletePropertyByName(resourceName);
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
   * If the list items are the same, return the stable partition list cache. Otherwise, refresh
   * the cache with new list and return the new list.
   * This is for a backward compatible workaround to fix https://github.com/apache/helix/issues/940.
   *
   * @param resourceName
   * @param newPartitionSet
   */
  public List<String> getOrSetStablePartitionList(String resourceName,
      Set<String> newPartitionSet) {
    List<String> cachedPartitions = _stablePartitionListCache.get(resourceName);
    if (cachedPartitions == null || cachedPartitions.size() != newPartitionSet.size()
        || !cachedPartitions.containsAll(newPartitionSet)) {
      _stablePartitionListCache.put(resourceName, new ArrayList<>(newPartitionSet));
    }
    return _stablePartitionListCache.get(resourceName);
  }

  /**
   * Retain the valid partition list cache items and remove the non-exist resources' cache.
   * This is for a backward compatible workaround to fix https://github.com/apache/helix/issues/940.
   *
   * @param resourceNames a list of the valid resources.
   */
  public void retainStablePartitionListCache(Set<String> resourceNames) {
    _stablePartitionListCache.keySet().retainAll(resourceNames);
  }
}
