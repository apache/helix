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
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class ParticipantStateCache<T> extends AbstractDataCache {
  private static Logger LOG = LoggerFactory.getLogger(ParticipantStateCache.class.getName());
  protected Map<String, Map<String, Map<String, T>>> _participantStateMap;

  protected Map<PropertyKey, T> _participantStateCache = Maps.newHashMap();
  public ParticipantStateCache(ControlContextProvider controlContextProvider) {
    super(controlContextProvider);
    _participantStateMap = Collections.emptyMap();
  }

  public ParticipantStateCache(String clusterName) {
    this(createDefaultControlContextProvider(clusterName));
  }


  /**
   * This refreshes the participant state cache data by re-fetching the data from zookeeper in an efficient way
   *
   * @param accessor
   * @param liveInstanceMap map of all liveInstances in cluster
   *
   * @return
   */
  public boolean refresh(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap, Boolean snapshotEnabled, Set<String> restrictedKeys) {
    long startTime = System.currentTimeMillis();

    refreshParticipantStatesCacheFromZk(accessor, liveInstanceMap, snapshotEnabled, restrictedKeys);
    Map<String, Map<String, Map<String, T>>> allParticipantStateMap = new HashMap<>();
    for (PropertyKey key : _participantStateCache.keySet()) {
      T participantState = _participantStateCache.get(key);
      String[] params = key.getParams();
      if (participantState != null && params.length >= 4) {
        String keyLevel1 = params[1];
        String keyLevel2 = params[2];
        String keyLevel3 = params[3];
        Map<String, Map<String, T>> stateMapLevel1 =
            allParticipantStateMap.get(keyLevel1);
        if (stateMapLevel1 == null) {
          stateMapLevel1 = Maps.newHashMap();
          allParticipantStateMap.put(keyLevel1, stateMapLevel1);
        }
        Map<String, T> stateMapLevel2 = stateMapLevel1.get(keyLevel2);
        if (stateMapLevel2 == null) {
          stateMapLevel2 = Maps.newHashMap();
          stateMapLevel1.put(keyLevel2, stateMapLevel2);
        }
        stateMapLevel2.put(keyLevel3, participantState);
      }
    }

    for (String instance : allParticipantStateMap.keySet()) {
      allParticipantStateMap.put(instance, Collections.unmodifiableMap(allParticipantStateMap.get(instance)));
    }
    _participantStateMap = Collections.unmodifiableMap(allParticipantStateMap);

    long endTime = System.currentTimeMillis();
    LogUtil.logInfo(LOG, genEventInfo(),
        "END: participantStateCache.refresh() for cluster " + _controlContextProvider.getClusterName()
            + ", started at : " + startTime + ", took " + (endTime - startTime) + " ms");
    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, genEventInfo(),
          String.format("Participant State refreshed : %s", _participantStateMap.toString()));
    }
    return true;
  }

  // reload participant states that has been changed from zk to local cache.

  private void refreshParticipantStatesCacheFromZk(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap, Boolean snapshotEnabled,
      Set<String> restrictedKeys) {

    long start = System.currentTimeMillis();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    Set<PropertyKey> participantStateKeys = new HashSet<>();
    for (String instanceName : liveInstanceMap.keySet()) {
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);

      if (restrictedKeys.isEmpty()) {
        String sessionId = liveInstance.getEphemeralOwner();
        List<String> currentStateNames =
            accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId));

        for (String currentStateName : currentStateNames) {
          participantStateKeys
              .add(keyBuilder.currentState(instanceName, sessionId, currentStateName));
        }
      } else {
        List<String> resourceNames = new ArrayList<>();
        for (String customizedStateType : restrictedKeys) {
          resourceNames = accessor
              .getChildNames(keyBuilder.customizedStates(instanceName, customizedStateType));
          for (String resourceName : resourceNames) {
            participantStateKeys
                .add(keyBuilder.customizedState(instanceName, customizedStateType, resourceName));
          }
        }
      }
    }
    // All new entries from zk not cached locally yet should be read from ZK.
    Set<PropertyKey> reloadKeys = new HashSet<>(participantStateKeys);
    reloadKeys.removeAll(_participantStateCache.keySet());

    Set<PropertyKey> cachedKeys = new HashSet<>(_participantStateCache.keySet());
    cachedKeys.retainAll(participantStateKeys);

    Set<PropertyKey> reloadedKeys = new HashSet<>();
    Map<PropertyKey, T> newStateCache = Collections.unmodifiableMap(refreshProperties(accessor,
        reloadKeys, new ArrayList<>(cachedKeys), _participantStateCache, reloadedKeys));

    if (snapshotEnabled) {
      refreshSnapshot(newStateCache, _participantStateCache, reloadedKeys);
    }

    _participantStateCache = newStateCache;

    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, genEventInfo(), "# of participant state reload: " + reloadKeys.size()
          + ", skipped:" + (participantStateKeys.size() - reloadKeys.size()) + ". took "
          + (System.currentTimeMillis() - start) + " ms to reload new participant states for cluster: "
          + _controlContextProvider.getClusterName() + "and state: " + this.getClass().getName());
    }
  }

  protected void refreshSnapshot(Map<PropertyKey, T> newStateCache,
      Map<PropertyKey, T> participantStateCache, Set<PropertyKey> reloadedKeys) {
  }

  /**
   * Return the whole participant state map.
   * @return
   */
  public Map<String, Map<String, Map<String, T>>> getParticipantStatesMap() {
    return Collections.unmodifiableMap(_participantStateMap);
  }

  /**
   * Return all participant states for a certain level1 key.
   * @param keyLevel1
   * @return
   */
  public Map<String, Map<String, T>> getParticipantStates(String keyLevel1) {
    if (!_participantStateMap.containsKey(keyLevel1)) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(_participantStateMap.get(keyLevel1));
  }

  /**
   * Provides the participant state map for a certain level1 key and level2 key
   * @param keyLevel1
   * @param keyLevel2
   * @return
   */
  public Map<String, T> getParticipantState(String keyLevel1,
      String keyLevel2) {
    if (!_participantStateMap.containsKey(keyLevel1)
        || !_participantStateMap.get(keyLevel1).containsKey(keyLevel2)) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(_participantStateMap.get(keyLevel1).get(keyLevel2));
  }
}
