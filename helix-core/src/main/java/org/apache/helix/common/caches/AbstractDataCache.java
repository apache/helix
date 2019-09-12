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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.controller.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataCache<T extends HelixProperty> {
  private static Logger LOG = LoggerFactory.getLogger(AbstractDataCache.class.getName());
  public static final String UNKNOWN_CLUSTER = "UNKNOWN_CLUSTER";
  public static final String UNKNOWN_EVENT_ID = "NO_ID";
  public static final String UNKNOWN_PIPELINE = "UNKNOWN_PIPELINE";

  protected ControlContextProvider _controlContextProvider;
  private AtomicBoolean _existsChange;

  public AbstractDataCache(ControlContextProvider controlContextProvider) {
    _controlContextProvider = controlContextProvider;
    _existsChange = new AtomicBoolean();
  }

  /**
   * Selectively fetch Helix Properties from ZK by comparing the version of local cached one with the one on ZK.
   * If version on ZK is newer, fetch it from zk and update local cache.
   * @param accessor the HelixDataAccessor
   * @param reloadKeysIn keys needs to be reload
   * @param cachedKeys keys already exists in the cache
   * @param cachedPropertyMap cached map of propertykey -> property object
   * @param reloadedKeys keys actually reloaded; may include more keys than reloadKeysIn
   * @return updated properties map
   */
  protected Map<PropertyKey, T> refreshProperties(
      HelixDataAccessor accessor, Set<PropertyKey> reloadKeysIn, List<PropertyKey> cachedKeys,
      Map<PropertyKey, T> cachedPropertyMap, Set<PropertyKey> reloadedKeys) {
    // All new entries from zk not cached locally yet should be read from ZK.
    List<PropertyKey> reloadKeys = new ArrayList<>(reloadKeysIn);
    Map<PropertyKey, T> refreshedPropertyMap = Maps.newHashMap();
    List<HelixProperty.Stat> stats = accessor.getPropertyStats(cachedKeys);
    for (int i = 0; i < cachedKeys.size(); i++) {
      PropertyKey key = cachedKeys.get(i);
      HelixProperty.Stat stat = stats.get(i);
      if (stat != null) {
        T property = cachedPropertyMap.get(key);

        if (property != null && property.getBucketSize() == 0 && property.getStat().equals(stat)) {
          refreshedPropertyMap.put(key, property);
        } else {
          // need update from zk
          reloadKeys.add(key);
        }
      } else {
        LOG.warn("stat is null for key: " + key);
        reloadKeys.add(key);
      }
    }

    reloadedKeys.clear();
    reloadedKeys.addAll(reloadKeys);
    // There exists a change if reloadedKeys is not empty
    _existsChange.set(!reloadedKeys.isEmpty());

    List<T> reloadedProperty = accessor.getProperty(reloadKeys, true);
    Iterator<PropertyKey> csKeyIter = reloadKeys.iterator();
    for (T property : reloadedProperty) {
      PropertyKey key = csKeyIter.next();
      if (property != null) {
        refreshedPropertyMap.put(key, property);
      } else {
        LOG.warn("znode is null for key: " + key);
      }
    }

    LogUtil.logInfo(LOG, genEventInfo(), String.format("%s properties refreshed from ZK.", reloadKeys.size()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("refreshed keys: " + reloadKeys);
    }

    return refreshedPropertyMap;
  }

  protected String genEventInfo() {
    return String.format("%s::%s::%s", _controlContextProvider.getClusterName(),
        _controlContextProvider.getPipelineName(), _controlContextProvider.getClusterEventId());
  }

  public AbstractDataSnapshot getSnapshot() {
    throw new HelixException(String.format("DataCache %s does not support generating snapshot.",
        getClass().getSimpleName()));
  }

  /**
   * Returns whether there has been any changes in the last refreshProperties() call.
   * @return
   */
  public boolean getExistsChange() {
    return _existsChange.get();
  }

  // for backward compatibility, used in scenarios where we only initialize child
  // classes with cluster name
  protected static ControlContextProvider createDefaultControlContextProvider(
      final String clusterName) {
    return new ControlContextProvider() {
      private String _clusterName = clusterName;
      private String _eventId = UNKNOWN_EVENT_ID;

      @Override
      public String getClusterName() {
        return _clusterName;
      }

      @Override
      public String getClusterEventId() {
        return _eventId;
      }

      @Override
      public void setClusterEventId(String eventId) {
        _eventId = eventId;
      }

      @Override
      public String getPipelineName() {
        return UNKNOWN_PIPELINE;
      }
    };
  }
}
