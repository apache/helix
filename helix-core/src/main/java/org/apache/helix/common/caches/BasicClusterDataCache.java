package org.apache.helix.common.caches;

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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache the basic cluster data, including LiveInstances, InstanceConfigs and ExternalViews.
 */
public class BasicClusterDataCache implements ControlContextProvider {
  private static Logger LOG = LoggerFactory.getLogger(BasicClusterDataCache.class.getName());

  private static final String INSTANCE_CONFIG = "InstanceConfig";
  private static final String LIVE_INSTANCE = "LiveInstance";

  private String _clusterEventId;

  protected PropertyCache<LiveInstance> _liveInstancePropertyCache;
  protected PropertyCache<InstanceConfig> _instanceConfigPropertyCache;
  protected ExternalViewCache _externalViewCache;

  protected String _clusterName;

  protected Map<HelixConstants.ChangeType, Boolean> _propertyDataChangedMap;

  public BasicClusterDataCache(String clusterName) {
    _propertyDataChangedMap = new ConcurrentHashMap<>();
    _externalViewCache = new ExternalViewCache(clusterName);
    _clusterName = clusterName;
    _clusterEventId = AbstractDataCache.UNKNOWN_EVENT_ID;

    _liveInstancePropertyCache = new PropertyCache<>(this, LIVE_INSTANCE,
        new PropertyCache.PropertyCacheKeyFuncs<LiveInstance>() {
          @Override
          public PropertyKey getRootKey(HelixDataAccessor accessor) {
            return accessor.keyBuilder().liveInstances();
          }

          @Override
          public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
            return accessor.keyBuilder().liveInstance(objName);
          }

          @Override
          public String getObjName(LiveInstance obj) {
            return obj.getInstanceName();
          }
        }, true);

    _instanceConfigPropertyCache = new PropertyCache<>(this, INSTANCE_CONFIG,
        new PropertyCache.PropertyCacheKeyFuncs<InstanceConfig>() {
          @Override
          public PropertyKey getRootKey(HelixDataAccessor accessor) {
            return accessor.keyBuilder().instanceConfigs();
          }

          @Override
          public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
            return accessor.keyBuilder().instanceConfig(objName);
          }

          @Override
          public String getObjName(InstanceConfig obj) {
            return obj.getInstanceName();
          }
        }, true);
  }

  /**
   * This refreshes the cluster data by re-fetching the data from zookeeper in an efficient way.
   * If we want to support multi-threading in the future, this method needs to be synchronized.
   *
   * @param accessor
   *
   * @return
   */
  public void refresh(HelixDataAccessor accessor) {
    LOG.info("START: BasicClusterDataCache.refresh() for cluster " + _clusterName);
    long startTime = System.currentTimeMillis();

    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.EXTERNAL_VIEW)) {
      _propertyDataChangedMap.put(HelixConstants.ChangeType.EXTERNAL_VIEW, false);
      _externalViewCache.refresh(accessor);
    }

    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.LIVE_INSTANCE)) {
      long start = System.currentTimeMillis();
      _propertyDataChangedMap.put(HelixConstants.ChangeType.LIVE_INSTANCE, false);
      _propertyDataChangedMap.put(HelixConstants.ChangeType.CURRENT_STATE, true);
      _liveInstancePropertyCache.refresh(accessor);
      LOG.info("Reload LiveInstances: " + _liveInstancePropertyCache.getPropertyMap().keySet()
          + ". Takes " + (System.currentTimeMillis() - start) + " ms");
    }

    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.INSTANCE_CONFIG)) {
      long start = System.currentTimeMillis();
      _propertyDataChangedMap.put(HelixConstants.ChangeType.INSTANCE_CONFIG, false);
      _instanceConfigPropertyCache.refresh(accessor);
      LOG.info("Reload InstanceConfig: " + _instanceConfigPropertyCache.getPropertyMap().keySet()
          + ". Takes " + (System.currentTimeMillis() - start) + " ms");
    }

    long endTime = System.currentTimeMillis();
    LOG.info(
        "END: BasicClusterDataCache.refresh() for cluster " + _clusterName + ", took " + (endTime
            - startTime) + " ms");

    if (LOG.isDebugEnabled()) {
      LOG.debug("LiveInstances: {}", _liveInstancePropertyCache.getPropertyMap());
      LOG.debug("ExternalViews: {}", _externalViewCache.getExternalViewMap().keySet());
      LOG.debug("InstanceConfigs: {}", _instanceConfigPropertyCache.getPropertyMap());
    }
  }

  /**
   * Retrieves the ExternalView for all resources
   *
   * @return
   */
  public Map<String, ExternalView> getExternalViews() {
    return _externalViewCache.getExternalViewMap();
  }

  /**
   * Returns the LiveInstances for each of the instances that are curretnly up and running
   *
   * @return
   */
  public Map<String, LiveInstance> getLiveInstances() {
    return _liveInstancePropertyCache.getPropertyMap();
  }

  /**
   * Returns the instance config map
   *
   * @return
   */
  public Map<String, InstanceConfig> getInstanceConfigMap() {
    return _instanceConfigPropertyCache.getPropertyMap();
  }

  /**
   * Notify the cache that some part of the cluster data has been changed.
   *
   * @param changeType
   * @param pathChanged
   */
  public void notifyDataChange(HelixConstants.ChangeType changeType, String pathChanged) {
    notifyDataChange(changeType);
  }

  /**
   * Notify the cache that some part of the cluster data has been changed.
   *
   * @param changeType
   */
  public void notifyDataChange(HelixConstants.ChangeType changeType) {
    _propertyDataChangedMap.put(changeType, true);
  }

  /**
   * Clear the corresponding cache based on change type
   * @param changeType
   */
  public synchronized void clearCache(HelixConstants.ChangeType changeType) {
    switch (changeType) {
    case LIVE_INSTANCE:
    case INSTANCE_CONFIG:
      LOG.warn("clearCache is deprecated for changeType: {}.", changeType);
      break;
    case EXTERNAL_VIEW:
      _externalViewCache.clear();
      break;
    default:
      break;
    }
  }

  /**
   * Indicate that a full read should be done on the next refresh
   */
  public void requireFullRefresh() {
    for(HelixConstants.ChangeType type : HelixConstants.ChangeType.values()) {
      _propertyDataChangedMap.put(type, Boolean.TRUE);
    }
  }

  /**
   * toString method to print the data cache state
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("liveInstancePropertyCache: ").append(_liveInstancePropertyCache.getPropertyMap())
        .append("\n");
    sb.append("externalViewCache: ").append(_externalViewCache.getExternalViewMap()).append("\n");
    sb.append("instanceConfigPropertyCache: ").append(_instanceConfigPropertyCache.getPropertyMap())
        .append("\n");

    return sb.toString();
  }

  @Override
  public String getClusterName() {
    return _clusterName;
  }

  @Override
  public String getClusterEventId() {
    return _clusterEventId;
  }

  @Override
  public void setClusterEventId(String clusterEventId) {
    _clusterEventId = clusterEventId;
  }

  @Override
  public String getPipelineName() {
    return AbstractDataCache.UNKNOWN_PIPELINE;
  }
}

