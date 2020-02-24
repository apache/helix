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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.model.CustomizedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Cache to hold all CustomizedView of a specific type.
 */
public class CustomizedViewCache extends AbstractDataCache<CustomizedView> {
  private static final Logger LOG = LoggerFactory.getLogger(CustomizedViewCache.class.getName());

  protected Map<String, CustomizedView> _customizedViewMap;
  protected Map<String, CustomizedView> _customizedViewCache;
  protected String _clusterName;
  private PropertyType _propertyType;
  private String _type;

  public CustomizedViewCache(String clusterName, String type) {
    this(clusterName, PropertyType.CUSTOMIZEDVIEW, type);
  }

  protected CustomizedViewCache(String clusterName, PropertyType propertyType, String type) {
    super(createDefaultControlContextProvider(clusterName));
    _clusterName = clusterName;
    _customizedViewMap = Collections.emptyMap();
    _customizedViewCache = Collections.emptyMap();
    _propertyType = propertyType;
    _type = type;
  }


  /**
   * This refreshes the CustomizedView data by re-fetching the data from zookeeper in an efficient
   * way
   * @param accessor
   * @return
   */
  public void refresh(HelixDataAccessor accessor) {
    long startTime = System.currentTimeMillis();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Set<PropertyKey> currentPropertyKeys = new HashSet<>();

    List<String> resources = accessor.getChildNames(customizedViewsKey(keyBuilder));

    for (String resource : resources) {
      currentPropertyKeys.add(customizedViewKey(keyBuilder, resource));
    }

    Set<PropertyKey> cachedKeys = new HashSet<>();
    Map<PropertyKey, CustomizedView> cachedCustomizedViewMap = Maps.newHashMap();
    for (String resource : _customizedViewCache.keySet()) {
      PropertyKey key = customizedViewKey(keyBuilder, resource);
      cachedKeys.add(key);
      cachedCustomizedViewMap.put(key, _customizedViewCache.get(resource));
    }
    cachedKeys.retainAll(currentPropertyKeys);

    Set<PropertyKey> reloadKeys = new HashSet<>(currentPropertyKeys);
    reloadKeys.removeAll(cachedKeys);

    Map<PropertyKey, CustomizedView> updatedMap =
        refreshProperties(accessor, reloadKeys, new ArrayList<>(cachedKeys),
            cachedCustomizedViewMap, new HashSet<>());

    Map<String, CustomizedView> newCustomizedViewMap = Maps.newHashMap();
    for (CustomizedView customizedView : updatedMap.values()) {
      newCustomizedViewMap.put(customizedView.getResourceName(), customizedView);
    }

    _customizedViewCache = new HashMap<>(newCustomizedViewMap);
    _customizedViewMap = new HashMap<>(newCustomizedViewMap);

    long endTime = System.currentTimeMillis();
    LOG.info("Refresh " + _customizedViewMap.size() + " CustomizedViews of type " + _type
        + " for cluster " + _clusterName + ", took " + (endTime - startTime) + " ms");
  }

  private PropertyKey customizedViewsKey(PropertyKey.Builder keyBuilder) {
    PropertyKey customizedViewPropertyKey;
    if (_propertyType.equals(PropertyType.CUSTOMIZEDVIEW)){
      customizedViewPropertyKey = keyBuilder.customizedView(_type);
    } else {
      throw new HelixException(
          "Failed to refresh CustomizedViewCache, Wrong property type " + _propertyType + "!");
    }
    return customizedViewPropertyKey;
  }

  private PropertyKey customizedViewKey(PropertyKey.Builder keyBuilder, String resource) {
    PropertyKey customizedViewPropertyKey;
    if (_propertyType.equals(PropertyType.CUSTOMIZEDVIEW)) {
      customizedViewPropertyKey = keyBuilder.customizedView(_type, resource);
    } else {
      throw new HelixException(
          "Failed to refresh CustomizedViewCache, Wrong property type " + _propertyType + "!");
    }
    return customizedViewPropertyKey;
  }

  /**
   * Return CustomizedView map for all resources.
   * @return
   */
  public Map<String, CustomizedView> getCustomizedViewMap() {
    return Collections.unmodifiableMap(_customizedViewMap);
  }

  /**
   * Remove dead customized views from map
   * @param resourceNames
   */

  public void removeCustomizedView(List<String> resourceNames) {
    for (String resourceName : resourceNames) {
      _customizedViewCache.remove(resourceName);
    }
  }


  public void clear() {
    _customizedViewCache.clear();
    _customizedViewMap.clear();
  }
}