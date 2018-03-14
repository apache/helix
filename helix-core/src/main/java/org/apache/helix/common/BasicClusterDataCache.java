package org.apache.helix.common;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache the cluster data
 */
public class BasicClusterDataCache {
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

  private Map<String, LiveInstance> _liveInstanceMap;
  private Map<String, InstanceConfig> _instanceConfigMap;
  private Map<String, ExternalView> _externalViewMap;
  protected String _clusterName;

  protected Map<HelixConstants.ChangeType, Boolean> _propertyDataChangedMap;

  public BasicClusterDataCache(String clusterName) {
    _propertyDataChangedMap = new ConcurrentHashMap<>();
    _liveInstanceMap = new HashMap<>();
    _instanceConfigMap = new HashMap<>();
    _externalViewMap = new HashMap<>();
    _clusterName = clusterName;
  }

  /**
   * This refreshes the cluster data by re-fetching the data from zookeeper in an efficient way
   *
   * @param accessor
   *
   * @return
   */
  public synchronized void refresh(HelixDataAccessor accessor) {
    LOG.info("START: ClusterDataCache.refresh() for cluster " + _clusterName);
    long startTime = System.currentTimeMillis();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.EXTERNAL_VIEW)) {
      long start = System.currentTimeMillis();
      _propertyDataChangedMap.put(HelixConstants.ChangeType.EXTERNAL_VIEW, Boolean.valueOf(false));
      _externalViewMap = accessor.getChildValuesMap(keyBuilder.externalViews());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reload ExternalViews: " + _externalViewMap.keySet() + ". Takes " + (
            System.currentTimeMillis() - start) + " ms");
      }
    }

    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.LIVE_INSTANCE)) {
      _propertyDataChangedMap.put(HelixConstants.ChangeType.LIVE_INSTANCE, Boolean.valueOf(false));
      _liveInstanceMap = accessor.getChildValuesMap(keyBuilder.liveInstances());
      LOG.debug("Reload LiveInstances: " + _liveInstanceMap.keySet());
    }

    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.INSTANCE_CONFIG)) {
      _propertyDataChangedMap
          .put(HelixConstants.ChangeType.INSTANCE_CONFIG, Boolean.valueOf(false));
      _instanceConfigMap = accessor.getChildValuesMap(keyBuilder.instanceConfigs());
      LOG.debug("Reload InstanceConfig: " + _instanceConfigMap.keySet());
    }

    long endTime = System.currentTimeMillis();
    LOG.info(
        "END: RoutingDataCache.refresh() for cluster " + _clusterName + ", took " + (endTime
            - startTime) + " ms");

    if (LOG.isDebugEnabled()) {
      LOG.debug("LiveInstances: " + _liveInstanceMap.keySet());
      for (LiveInstance instance : _liveInstanceMap.values()) {
        LOG.debug("live instance: " + instance.getInstanceName() + " " + instance.getSessionId());
      }
      LOG.debug("ExternalViews: " + _externalViewMap.keySet());
      LOG.debug("InstanceConfigs: " + _instanceConfigMap.keySet());
    }
  }

  /**
   * Retrieves the ExternalView for all resources
   *
   * @return
   */
  public Map<String, ExternalView> getExternalViews() {
    return Collections.unmodifiableMap(_externalViewMap);
  }

  /**
   * Returns the LiveInstances for each of the instances that are curretnly up and running
   *
   * @return
   */
  public Map<String, LiveInstance> getLiveInstances() {
    return Collections.unmodifiableMap(_liveInstanceMap);
  }

  /**
   * Returns the instance config map
   *
   * @return
   */
  public Map<String, InstanceConfig> getInstanceConfigMap() {
    return Collections.unmodifiableMap(_instanceConfigMap);
  }

  /**
   * Notify the cache that some part of the cluster data has been changed.
   */
  public synchronized void notifyDataChange(HelixConstants.ChangeType changeType) {
    _propertyDataChangedMap.put(changeType, Boolean.valueOf(true));
  }

  /**
   * Indicate that a full read should be done on the next refresh
   */
  public synchronized void requireFullRefresh() {
    for(HelixConstants.ChangeType type : HelixConstants.ChangeType.values()) {
      _propertyDataChangedMap.put(type, Boolean.valueOf(true));
    }
  }

  /**
   * toString method to print the data cache state
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("liveInstaceMap:" + _liveInstanceMap).append("\n");
    sb.append("externalViewMap:" + _externalViewMap).append("\n");
    sb.append("instanceConfigMap:" + _instanceConfigMap).append("\n");

    return sb.toString();
  }
}

