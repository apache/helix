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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache to hold all CurrentStates of a cluster.
 */
public class CurrentStateCache extends AbstractDataCache<CurrentState> {
  private static final Logger LOG = LoggerFactory.getLogger(CurrentStateCache.class.getName());

  private Map<String, Map<String, Map<String, CurrentState>>> _currentStateMap;
  private Map<PropertyKey, CurrentState> _currentStateCache = Maps.newHashMap();
  // If the cache is already refreshed with current state data.
  private boolean _initialized = false;
  private CurrentStateSnapshot _snapshot;

  public CurrentStateCache(String clusterName) {
    this(createDefaultControlContextProvider(clusterName));
  }

  public CurrentStateCache(ControlContextProvider contextProvider) {
    super(contextProvider);
    _currentStateMap = Collections.emptyMap();
    _snapshot = new CurrentStateSnapshot(_currentStateCache);
  }

  /**
   * This refreshes the CurrentStates data by re-fetching the data from zookeeper in an efficient
   * way
   *
   * @param accessor
   * @param liveInstanceMap map of all liveInstances in cluster
   *
   * @return
   */
  public boolean refresh(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap) {
    long startTime = System.currentTimeMillis();

    refreshCurrentStatesCache(accessor, liveInstanceMap);
    Map<String, Map<String, Map<String, CurrentState>>> allCurStateMap = new HashMap<>();
    for (PropertyKey key : _currentStateCache.keySet()) {
      CurrentState currentState = _currentStateCache.get(key);
      String[] params = key.getParams();
      if (currentState != null && params.length >= 4) {
        String instanceName = params[1];
        String sessionId = params[2];
        String stateName = params[3];
        Map<String, Map<String, CurrentState>> instanceCurStateMap =
            allCurStateMap.get(instanceName);
        if (instanceCurStateMap == null) {
          instanceCurStateMap = Maps.newHashMap();
          allCurStateMap.put(instanceName, instanceCurStateMap);
        }
        Map<String, CurrentState> sessionCurStateMap = instanceCurStateMap.get(sessionId);
        if (sessionCurStateMap == null) {
          sessionCurStateMap = Maps.newHashMap();
          instanceCurStateMap.put(sessionId, sessionCurStateMap);
        }
        sessionCurStateMap.put(stateName, currentState);
      }
    }

    for (String instance : allCurStateMap.keySet()) {
      allCurStateMap.put(instance, Collections.unmodifiableMap(allCurStateMap.get(instance)));
    }
    _currentStateMap = Collections.unmodifiableMap(allCurStateMap);

    long endTime = System.currentTimeMillis();
    LogUtil.logInfo(LOG, genEventInfo(),
        "END: CurrentStateCache.refresh() for cluster " + _controlContextProvider.getClusterName()
            + ", started at : " + startTime + ", took " + (endTime - startTime) + " ms");
    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, genEventInfo(),
          String.format("Current State refreshed : %s", _currentStateMap.toString()));
    }
    return true;
  }

  // reload current states that has been changed from zk to local cache.
  private void refreshCurrentStatesCache(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap) {

    long start = System.currentTimeMillis();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    Set<PropertyKey> currentStateKeys = new HashSet<>();
    for (String instanceName : liveInstanceMap.keySet()) {
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      String sessionId = liveInstance.getEphemeralOwner();
      List<String> currentStateNames =
          accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId));
      for (String currentStateName : currentStateNames) {
        currentStateKeys.add(keyBuilder.currentState(instanceName, sessionId, currentStateName));
      }
    }
    // All new entries from zk not cached locally yet should be read from ZK.
    Set<PropertyKey> reloadKeys = new HashSet<>(currentStateKeys);
    reloadKeys.removeAll(_currentStateCache.keySet());

    Set<PropertyKey> cachedKeys = new HashSet<>(_currentStateCache.keySet());
    cachedKeys.retainAll(currentStateKeys);

    Set<PropertyKey> reloadedKeys = new HashSet<>();
    Map<PropertyKey, CurrentState> newStateCache = Collections.unmodifiableMap(
        refreshProperties(accessor, reloadKeys, new ArrayList<>(cachedKeys),
            _currentStateCache, reloadedKeys));

    // if the cache was not initialized, the previous state should not be included in the snapshot
    if (_initialized) {
      _snapshot = new CurrentStateSnapshot(newStateCache, _currentStateCache, reloadedKeys);
    } else {
      _snapshot = new CurrentStateSnapshot(newStateCache);
      _initialized = true;
    }

    _currentStateCache = newStateCache;

    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, genEventInfo(),
          "# of CurrentStates reload: " + reloadKeys.size() + ", skipped:" + (
              currentStateKeys.size() - reloadKeys.size()) + ". took " + (System.currentTimeMillis()
              - start) + " ms to reload new current states for cluster: " + _controlContextProvider
              .getClusterName());
    }
  }

  /**
   * Return CurrentStates map for all instances.
   *
   * @return
   */
  public Map<String, Map<String, Map<String, CurrentState>>> getCurrentStatesMap() {
    return Collections.unmodifiableMap(_currentStateMap);
  }

  /**
   * Return all CurrentState on the given instance.
   *
   * @param instance
   *
   * @return
   */
  public Map<String, Map<String, CurrentState>> getCurrentStates(String instance) {
    if (!_currentStateMap.containsKey(instance)) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(_currentStateMap.get(instance));
  }

  /**
   * Provides the current state of the node for a given session id, the sessionid can be got from
   * LiveInstance
   *
   * @param instance
   * @param clientSessionId
   *
   * @return
   */
  public Map<String, CurrentState> getCurrentState(String instance, String clientSessionId) {
    if (!_currentStateMap.containsKey(instance) || !_currentStateMap.get(instance)
        .containsKey(clientSessionId)) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(_currentStateMap.get(instance).get(clientSessionId));
  }

  @Override
  public CurrentStateSnapshot getSnapshot() {
    return _snapshot;
  }
}
