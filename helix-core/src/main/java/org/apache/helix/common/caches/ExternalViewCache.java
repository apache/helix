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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache to hold all ExternalViews of a cluster.
 */
public class ExternalViewCache extends AbstractDataCache {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalViewCache.class.getName());

  protected Map<String, ExternalView> _externalViewMap;
  protected Map<String, ExternalView> _externalViewCache;

  protected String _clusterName;

  private PropertyType _type;

  public ExternalViewCache(String clusterName) {
    this(clusterName, PropertyType.EXTERNALVIEW);
  }

  protected ExternalViewCache(String clusterName, PropertyType type) {
    _clusterName = clusterName;
    _externalViewMap = Collections.emptyMap();
    _externalViewCache = Collections.emptyMap();
    _type = type;
  }


  /**
   * This refreshes the ExternalView data by re-fetching the data from zookeeper in an efficient
   * way
   *
   * @param accessor
   *
   * @return
   */
  public void refresh(HelixDataAccessor accessor) {
    long startTime = System.currentTimeMillis();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Set<PropertyKey> currentPropertyKeys = new HashSet<>();

    List<String> resources = accessor.getChildNames(externalViewsKey(keyBuilder));
    for (String resource : resources) {
      currentPropertyKeys.add(externalViewKey(keyBuilder, resource));
    }

    Set<PropertyKey> cachedKeys = new HashSet<>();
    Map<PropertyKey, ExternalView> cachedExternalViewMap = Maps.newHashMap();
    for (String resource : _externalViewCache.keySet()) {
      PropertyKey key = externalViewKey(keyBuilder, resource);
      cachedKeys.add(key);
      cachedExternalViewMap.put(key, _externalViewCache.get(resource));
    }
    cachedKeys.retainAll(currentPropertyKeys);

    Set<PropertyKey> reloadKeys = new HashSet<>(currentPropertyKeys);
    reloadKeys.removeAll(cachedKeys);

    Map<PropertyKey, ExternalView> updatedMap =
        refreshProperties(accessor, new LinkedList<>(reloadKeys), new ArrayList<>(cachedKeys),
            cachedExternalViewMap);
    Map<String, ExternalView> newExternalViewMap = Maps.newHashMap();
    for (ExternalView externalView : updatedMap.values()) {
      newExternalViewMap.put(externalView.getResourceName(), externalView);
    }

    _externalViewCache = new HashMap<>(newExternalViewMap);
    _externalViewMap = new HashMap<>(newExternalViewMap);

    long endTime = System.currentTimeMillis();
    LOG.info("Refresh " + _externalViewMap.size() + " ExternalViews for cluster " + _clusterName
        + ", took " + (endTime - startTime) + " ms");
  }

  private PropertyKey externalViewsKey(PropertyKey.Builder keyBuilder) {
    PropertyKey evPropertyKey;
    if (_type.equals(PropertyType.EXTERNALVIEW)) {
      evPropertyKey = keyBuilder.externalViews();
    } else if (_type.equals(PropertyType.TARGETEXTERNALVIEW)) {
      evPropertyKey = keyBuilder.targetExternalViews();
    } else {
      throw new HelixException(
          "Failed to refresh ExternalViewCache, Wrong property type " + _type + "!");
    }

    return evPropertyKey;
  }

  private PropertyKey externalViewKey(PropertyKey.Builder keyBuilder, String resource) {
    PropertyKey evPropertyKey;
    if (_type.equals(PropertyType.EXTERNALVIEW)) {
      evPropertyKey = keyBuilder.externalView(resource);
    } else if (_type.equals(PropertyType.TARGETEXTERNALVIEW)) {
      evPropertyKey = keyBuilder.targetExternalView(resource);
    } else {
      throw new HelixException(
          "Failed to refresh ExternalViewCache, Wrong property type " + _type + "!");
    }

    return evPropertyKey;
  }

  /**
   * Return ExternalView map for all resources.
   *
   * @return
   */
  public Map<String, ExternalView> getExternalViewMap() {
    return Collections.unmodifiableMap(_externalViewMap);
  }

  public void clear() {
    _externalViewCache.clear();
    _externalViewMap.clear();
  }
}
