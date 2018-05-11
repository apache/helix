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

import java.util.Map;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyType;
import org.apache.helix.common.caches.BasicClusterDataCache;
import org.apache.helix.common.caches.CurrentStateCache;
import org.apache.helix.common.caches.TargetExternalViewCache;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache the cluster data that are needed by RoutingTableProvider.
 */
class RoutingDataCache extends BasicClusterDataCache {
  private static Logger LOG = LoggerFactory.getLogger(RoutingDataCache.class.getName());

  private final PropertyType _sourceDataType;
  private CurrentStateCache _currentStateCache;
  private TargetExternalViewCache _targetExternalViewCache;

  public RoutingDataCache(String clusterName, PropertyType sourceDataType) {
    super(clusterName);
    _sourceDataType = sourceDataType;
    _currentStateCache = new CurrentStateCache(clusterName);
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

    if (_sourceDataType.equals(PropertyType.TARGETEXTERNALVIEW) && _propertyDataChangedMap
        .get(HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW)) {
      long start = System.currentTimeMillis();
      _propertyDataChangedMap
          .put(HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW, Boolean.valueOf(false));
      _targetExternalViewCache.refresh(accessor);
      LOG.info("Reload " + _targetExternalViewCache.getExternalViewMap().keySet().size()
          + " TargetExternalViews. Takes " + (System.currentTimeMillis() - start) + " ms");
    }

    if (_sourceDataType.equals(PropertyType.CURRENTSTATES) && _propertyDataChangedMap
        .get(HelixConstants.ChangeType.CURRENT_STATE)) {
      long start = System.currentTimeMillis();
      Map<String, LiveInstance> liveInstanceMap = getLiveInstances();
      _currentStateCache.refresh(accessor, liveInstanceMap);
      LOG.info("Reload CurrentStates. Takes " + (System.currentTimeMillis() - start) + " ms");
    }

    long endTime = System.currentTimeMillis();
    LOG.info("END: RoutingDataCache.refresh() for cluster " + _clusterName + ", took " + (endTime
        - startTime) + " ms");

    if (LOG.isDebugEnabled()) {
      LOG.debug("CurrentStates: " + _currentStateCache);
      LOG.debug("TargetExternalViews: " + _targetExternalViewCache.getExternalViewMap());
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
   * Get map of current states in cluster. {InstanceName -> {SessionId -> {ResourceName ->
   * CurrentState}}}
   *
   * @return
   */
  public Map<String, Map<String, Map<String, CurrentState>>> getCurrentStatesMap() {
    return _currentStateCache.getCurrentStatesMap();
  }
}

