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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.caches.AbstractDataCache;
import org.apache.helix.common.caches.PropertyCache;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.stages.MissingTopStateRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.ResourceAssignment;
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
  private static final String PIPELINE_NAME = "ResourcePipeline";

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
  }

  public synchronized void refresh(HelixDataAccessor accessor) {
    long startTime = System.currentTimeMillis();
    // Invalidate cached information
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.IDEAL_STATE)
        || _propertyDataChangedMap.get(HelixConstants.ChangeType.LIVE_INSTANCE)
        || _propertyDataChangedMap.get(HelixConstants.ChangeType.INSTANCE_CONFIG)
        || _propertyDataChangedMap.get(HelixConstants.ChangeType.RESOURCE_CONFIG)) {
      clearCachedResourceAssignments();
    }

    // Refresh base
    super.refresh(accessor);

    // Refresh resource controller specific property caches
    refreshExternalViews(accessor);
    refreshTargetExternalViews(accessor);
    LogUtil.logInfo(logger, getClusterEventId(), String.format(
        "END: WorkflowControllerDataProvider.refresh() for cluster %s took %s for %s pipeline",
        getClusterName(), System.currentTimeMillis() - startTime, getPipelineName()));
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
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.EXTERNAL_VIEW)) {
      _externalViewCache.refresh(accessor);
      _propertyDataChangedMap.put(HelixConstants.ChangeType.EXTERNAL_VIEW, false);
    }
  }

  private void refreshTargetExternalViews(final HelixDataAccessor accessor) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW)) {
      if (getClusterConfig() != null && getClusterConfig().isTargetExternalViewEnabled()) {
        _targetExternalViewCache.refresh(accessor);

        // Only set the change type back once we get refreshed with data accessor for the
        // first time
        _propertyDataChangedMap.put(HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW, false);
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
   * Invalid the cached resourceAssignment (ideal mapping) for a resource
   * @param resource
   */
  public void invalidCachedIdealStateMapping(String resource) {
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

  public void clearCachedResourceAssignments() {
    _resourceAssignmentCache.clear();
    _idealMappingCache.clear();
  }

  public void clearMonitoringRecords() {
    _missingTopStateMap.clear();
    _lastTopStateLocationMap.clear();
  }
}
