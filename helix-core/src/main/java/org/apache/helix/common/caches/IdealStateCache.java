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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache to hold all IdealStates of a cluster.
 * Deprecated - use {@link PropertyCache<IdealState>} instead
 */
@Deprecated
public class IdealStateCache extends AbstractDataCache<IdealState> {
  private static final Logger LOG = LoggerFactory.getLogger(IdealStateCache.class.getName());

  private Map<String, IdealState> _idealStateMap;
  private Map<String, IdealState> _idealStateCache;

  private String _clusterName;

  public IdealStateCache(String clusterName) {
    super(createDefaultControlContextProvider(clusterName));
    _clusterName = clusterName;
    _idealStateMap = Collections.emptyMap();
    _idealStateCache = Collections.emptyMap();
  }


  /**
   * This refreshes the IdealState data by re-fetching the data from zookeeper in an efficient
   * way
   *
   * @param accessor
   *
   * @return
   */
  public void refresh(HelixDataAccessor accessor) {
    long startTime = System.currentTimeMillis();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Set<PropertyKey> currentIdealStateKeys = new HashSet<>();
    for (String idealState : accessor.getChildNames(keyBuilder.idealStates())) {
      currentIdealStateKeys.add(keyBuilder.idealStates(idealState));
    }

    Set<PropertyKey> cachedKeys = new HashSet<>();
    Map<PropertyKey, IdealState> cachedIdealStateMap = Maps.newHashMap();
    for (String idealState : _idealStateCache.keySet()) {
      cachedKeys.add(keyBuilder.idealStates(idealState));
      cachedIdealStateMap
          .put(keyBuilder.idealStates(idealState), _idealStateCache.get(idealState));
    }
    cachedKeys.retainAll(currentIdealStateKeys);

    Set<PropertyKey> reloadKeys = new HashSet<>(currentIdealStateKeys);
    reloadKeys.removeAll(cachedKeys);

    Map<PropertyKey, IdealState> updatedMap =
        refreshProperties(accessor, reloadKeys, new ArrayList<>(cachedKeys),
            cachedIdealStateMap, new HashSet<>());
    Map<String, IdealState> newIdealStateMap = Maps.newHashMap();
    for (IdealState idealState : updatedMap.values()) {
      newIdealStateMap.put(idealState.getResourceName(), idealState);
    }

    _idealStateCache = new HashMap<>(newIdealStateMap);
    _idealStateMap = new HashMap<>(newIdealStateMap);

    long endTime = System.currentTimeMillis();
    LogUtil.logInfo(LOG, _controlContextProvider.getClusterEventId(),
        "Refresh " + _idealStateMap.size() + " idealStates for cluster " + _clusterName + ", took "
            + (endTime - startTime) + " ms");
  }

  /**
   * Return IdealState map for all resources.
   *
   * @return
   */
  public Map<String, IdealState> getIdealStateMap() {
    return Collections.unmodifiableMap(_idealStateMap);
  }

  public void clear() {
    _idealStateMap.clear();
    _idealStateCache.clear();
  }

  /**
   * Set the IdealStates.
   * CAUTION: After refresh() call, the previously set idealstates will be updated.
   *
   * @param idealStates
   */
  public void setIdealStates(List<IdealState> idealStates) {
    Map<String, IdealState> idealStateMap = new HashMap();
    for (IdealState idealState : idealStates) {
      idealStateMap.put(idealState.getId(), idealState);
    }
    _idealStateMap = idealStateMap;
  }
}
