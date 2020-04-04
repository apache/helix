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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represent a cache that holds a certain participant side state of for the whole cluster.
 */
public abstract class ParticipantStateCache<T> extends AbstractDataCache {
  private static Logger LOG = LoggerFactory.getLogger(ParticipantStateCache.class);
  protected Map<String, Map<String, Map<String, T>>> _participantStateMap;

  protected Map<PropertyKey, T> _participantStateCache = Maps.newHashMap();

  public ParticipantStateCache(ControlContextProvider controlContextProvider) {
    super(controlContextProvider);
    _participantStateMap = new HashMap<>();
  }

  /**
   * This refreshes the participant state cache data by re-fetching the data from zookeeper in an
   * efficient way
   * @param accessor
   * @param liveInstanceMap map of all liveInstances in cluster
   * @return
   */
  public boolean refresh(HelixDataAccessor accessor, Map<String, LiveInstance> liveInstanceMap) {
    long startTime = System.currentTimeMillis();

    refreshParticipantStatesCacheFromZk(accessor, liveInstanceMap);
    Map<String, Map<String, Map<String, T>>> allParticipantStateMap = new HashMap<>();
    // There should be 4 levels of keys. The first one is the cluster name, the second one is the
    // instance name, the third one is a customized key (could be session Id or customized state
    // type), the fourth one is the resourceName
    for (PropertyKey key : _participantStateCache.keySet()) {
      T participantState = _participantStateCache.get(key);
      String[] params = key.getParams();
      if (participantState != null && params.length >= 4) {
        String instanceName = params[1];
        String customizedName = params[2];
        String resourceName = params[3];
        Map<String, Map<String, T>> instanceMap = allParticipantStateMap.get(instanceName);
        if (instanceMap == null) {
          instanceMap = Maps.newHashMap();
          allParticipantStateMap.put(instanceName, instanceMap);
        }
        Map<String, T> customizedMap = instanceMap.get(customizedName);
        if (customizedMap == null) {
          customizedMap = Maps.newHashMap();
          instanceMap.put(customizedName, customizedMap);
        }
        customizedMap.put(resourceName, participantState);
      } else {
        LogUtil.logError(LOG, genEventInfo(),
            "Invalid key found in the participant state cache" + key);
      }
    }

    _participantStateMap = Collections.unmodifiableMap(allParticipantStateMap);

    long endTime = System.currentTimeMillis();
    LogUtil.logInfo(LOG, genEventInfo(),
        "END: participantStateCache.refresh() for cluster " + _controlContextProvider
            .getClusterName() + ", started at : " + startTime + ", took " + (endTime - startTime)
            + " ms");
    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, genEventInfo(),
          String.format("Participant State refreshed : %s", _participantStateMap.toString()));
    }
    return true;
  }

  // reload participant states that has been changed from zk to local cache.
  private void refreshParticipantStatesCacheFromZk(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap) {

    long start = System.currentTimeMillis();
    Set<PropertyKey> participantStateKeys =
        PopulateParticipantKeys(accessor, liveInstanceMap);

    // All new entries from zk not cached locally yet should be read from ZK.
    Set<PropertyKey> reloadKeys = new HashSet<>(participantStateKeys);
    reloadKeys.removeAll(_participantStateCache.keySet());

    Set<PropertyKey> cachedKeys = new HashSet<>(_participantStateCache.keySet());
    cachedKeys.retainAll(participantStateKeys);

    Set<PropertyKey> reloadedKeys = new HashSet<>();
    Map<PropertyKey, T> newStateCache = Collections.unmodifiableMap(
        refreshProperties(accessor, reloadKeys, new ArrayList<>(cachedKeys), _participantStateCache,
            reloadedKeys));

    refreshSnapshot(newStateCache, _participantStateCache, reloadedKeys);

    _participantStateCache = newStateCache;

    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, genEventInfo(),
          "# of participant state reload: " + reloadKeys.size() + ", skipped:" + (
              participantStateKeys.size() - reloadKeys.size()) + ". took " + (
              System.currentTimeMillis() - start)
              + " ms to reload new participant states for cluster: " + _controlContextProvider
              .getClusterName() + "and state: " + this.getClass().getName());
    }
  }

  protected abstract Set<PropertyKey> PopulateParticipantKeys(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap);

  /**
   * Refresh the snapshot of the cache. This method is optional for child class to extend. If the
   * child class does not need to refresh snapshot, it just does nothing.
   * @return
   */
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
   * Return all participant states for a certain instance.
   * @param instanceName
   * @return
   */
  public Map<String, Map<String, T>> getParticipantStates(String instanceName) {
    if (!_participantStateMap.containsKey(instanceName)) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(_participantStateMap.get(instanceName));
  }

  /**
   * Provides the participant state map for a certain instance and a customized key
   * @param instanceName
   * @param customizedKey
   * @return
   */
  public Map<String, T> getParticipantState(String instanceName, String customizedKey) {
    if (!_participantStateMap.containsKey(instanceName) || !_participantStateMap.get(instanceName)
        .containsKey(customizedKey)) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(_participantStateMap.get(instanceName).get(customizedKey));
  }
}
