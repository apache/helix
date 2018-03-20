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

import java.util.Collections;
import java.util.Map;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.common.caches.BasicClusterDataCache;
import org.apache.helix.common.caches.CurrentStateCache;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache the cluster data that are needed by RoutingTableProvider.
 */
public class RoutingDataCache extends BasicClusterDataCache {
  private static Logger LOG = LoggerFactory.getLogger(RoutingDataCache.class.getName());

  private final PropertyType _sourceDataType;
  private CurrentStateCache _currentStateCache;
  private Map<String, ExternalView> _targetExternalViewMap;

  public RoutingDataCache(String clusterName, PropertyType sourceDataType) {
    super(clusterName);
    _sourceDataType = sourceDataType;
    _currentStateCache = new CurrentStateCache(clusterName);
    _targetExternalViewMap = Collections.emptyMap();
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
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      _propertyDataChangedMap
          .put(HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW, Boolean.valueOf(false));
      _targetExternalViewMap = accessor.getChildValuesMap(keyBuilder.targetExternalViews());
      LOG.info("Reload TargetExternalViews: " + _targetExternalViewMap.keySet() + ". Takes " + (
          System.currentTimeMillis() - start) + " ms");
    }

    if (_sourceDataType.equals(PropertyType.CURRENTSTATES) && _propertyDataChangedMap
        .get(HelixConstants.ChangeType.CURRENT_STATE)) {
      long start = System.currentTimeMillis();
      Map<String, LiveInstance> liveInstanceMap = getLiveInstances();
      _currentStateCache.refresh(accessor, liveInstanceMap);
      LOG.info("Reload CurrentStates: " + _targetExternalViewMap.keySet() + ". Takes " + (
          System.currentTimeMillis() - start) + " ms");
    }

    long endTime = System.currentTimeMillis();
    LOG.info("END: RoutingDataCache.refresh() for cluster " + _clusterName + ", took " + (endTime
        - startTime) + " ms");

    if (LOG.isDebugEnabled()) {
      LOG.debug("CurrentStates: " + _currentStateCache);
      LOG.debug("TargetExternalViews: " + _targetExternalViewMap);
    }
  }

  /**
   * Retrieves the TargetExternalView for all resources
   *
   * @return
   */
  public Map<String, ExternalView> getTargetExternalViews() {
    return Collections.unmodifiableMap(_targetExternalViewMap);
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

