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

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomizedStateCache extends AbstractDataCache<CustomizedState> {
  private static final Logger LOG = LoggerFactory.getLogger(CurrentStateCache.class.getName());

  // instance -> customizedStateType -> resource -> CustomizedState
  private Map<String, Map<String, Map<String, CustomizedState>>> _customizedStateMap;
  private Map<PropertyKey, CustomizedState> _customizedStateCache = Maps.newHashMap();

  public CustomizedStateCache(String clusterName) {
    this(createDefaultControlContextProvider(clusterName));
  }

  public CustomizedStateCache(ControlContextProvider contextProvider) {
    super(contextProvider);
    _customizedStateMap = Collections.emptyMap();
  }

  /**
   * This refreshes the CustomizedStates data by re-fetching the data from zookeeper in an efficient
   * way
   * @param accessor
   * @param liveInstanceMap map of all liveInstances in cluster
   * @return
   */
  public boolean refresh(HelixDataAccessor accessor, Map<String, LiveInstance> liveInstanceMap,
      Set<String> aggregationEnabledTypes) {
    long startTime = System.currentTimeMillis();

    refreshCustomizedStatesCache(accessor, liveInstanceMap, aggregationEnabledTypes);

    Map<String, Map<String, Map<String, CustomizedState>>> allCustomizedStateMap = new HashMap<>();
    for (PropertyKey key : _customizedStateCache.keySet()) {
      CustomizedState customizedState = _customizedStateCache.get(key);
      String[] params = key.getParams();
      if (customizedState != null && params.length >= 4) {
        String instanceName = params[1];
        String customizedStateType = params[2];
        String resourceName = params[3];
        Map<String, Map<String, CustomizedState>> instanceCustomizedStateMap =
            allCustomizedStateMap.get(instanceName);
        if (instanceCustomizedStateMap == null) {
          instanceCustomizedStateMap = Maps.newHashMap();
          allCustomizedStateMap.put(instanceName, instanceCustomizedStateMap);
        }
        Map<String, CustomizedState> customizedStateNameCustomizedStateMap =
            instanceCustomizedStateMap.get(customizedStateType);
        if (customizedStateNameCustomizedStateMap == null) {
          customizedStateNameCustomizedStateMap = Maps.newHashMap();
          instanceCustomizedStateMap.put(customizedStateType,
              customizedStateNameCustomizedStateMap);
        }
        customizedStateNameCustomizedStateMap.put(resourceName, customizedState);
      }
    }

    for (String instance : allCustomizedStateMap.keySet()) {
      allCustomizedStateMap.put(instance,
          Collections.unmodifiableMap(allCustomizedStateMap.get(instance)));
    }
    _customizedStateMap = Collections.unmodifiableMap(allCustomizedStateMap);

    long endTime = System.currentTimeMillis();
    LogUtil.logInfo(LOG, genEventInfo(),
        "END: CustomizedStateCache.refresh() for cluster "
            + _controlContextProvider.getClusterName() + ", started at : " + startTime + ", took "
            + (endTime - startTime) + " ms");
    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, genEventInfo(),
          String.format("Customized State refreshed : %s", _customizedStateMap.toString()));
    }
    return true;
  }

  // reload customized states that has been changed from zk to local cache.
  private void refreshCustomizedStatesCache(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap, Set<String> aggregationEnabledTypes) {
    long start = System.currentTimeMillis();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Set<PropertyKey> customizedStateKeys = new HashSet<>();
    List<String> resourceNames = new ArrayList<>();
    for (String instanceName : liveInstanceMap.keySet()) {
      for (String customizedStateName : aggregationEnabledTypes) {
        resourceNames =
            accessor.getChildNames(keyBuilder.customizedStates(instanceName, customizedStateName));
        for (String resourceName : resourceNames) {
          customizedStateKeys
              .add(keyBuilder.customizedState(instanceName, customizedStateName, resourceName));
        }
      }
      // All new entries from zk not cached locally yet should be read from ZK.
      Set<PropertyKey> reloadKeys = new HashSet<>(customizedStateKeys);
      reloadKeys.removeAll(_customizedStateCache.keySet());

      Set<PropertyKey> cachedKeys = new HashSet<>(_customizedStateCache.keySet());
      cachedKeys.retainAll(customizedStateKeys);

      Set<PropertyKey> reloadedKeys = new HashSet<>();
      Map<PropertyKey, CustomizedState> newStateCache =
          Collections.unmodifiableMap(refreshProperties(accessor, reloadKeys,
              new ArrayList<>(cachedKeys), _customizedStateCache, reloadedKeys));

      _customizedStateCache = newStateCache;

      if (LOG.isDebugEnabled()) {
        LogUtil.logDebug(LOG, genEventInfo(),
            "# of CustomizedStates reload: " + reloadKeys.size() + ", skipped:"
                + (customizedStateKeys.size() - reloadKeys.size()) + ". took "
                + (System.currentTimeMillis() - start)
                + " ms to reload new customized states for cluster: "
                + _controlContextProvider.getClusterName());
      }
    }
  }

  /**
   * Return CustomizedStates map for all instances.
   * @return
   */
  public Map<String, Map<String, Map<String, CustomizedState>>> getCustomizedStatesMap() {
    return Collections.unmodifiableMap(_customizedStateMap);
  }

  /**
   * Return all CustomizedStates on the given instance.
   * @param instance
   * @return
   */
  public Map<String, Map<String, CustomizedState>> getCustomizedStates(String instance) {
    if (!_customizedStateMap.containsKey(instance)) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(_customizedStateMap.get(instance));
  }

  /**
   * Provides the customized state of the node for a given customized state type
   * @param instance
   * @param customizeStateType
   * @return
   */
  public Map<String, CustomizedState> getCustomizedState(String instance,
      String customizeStateType) {
    if (!_customizedStateMap.containsKey(instance)
        || !_customizedStateMap.get(instance).containsKey(customizeStateType)) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(_customizedStateMap.get(instance).get(customizeStateType));
  }
}