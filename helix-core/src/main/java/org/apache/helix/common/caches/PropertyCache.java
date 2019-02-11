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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.controller.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

/**
 * A general cache for HelixProperty that supports LIST, GET, SET, DELETE methods of Helix property.
 * All operation is in memory and is not persisted into the data store, but it provides a method to
 * refresh cache content from data store using the given data accessor.
 *
 * Currently this class only supports helix properties that stores under same root, i.e.
 * IdealState, InstanceConfig, ExternalView, LiveInstance, ResourceConfig, etc.
 *
 * WARNING: This class is not thread safe - caller should refresh and utilize the cache in a
 * thread-safe manner
 * @param <T>
 */
public class PropertyCache<T extends HelixProperty> extends AbstractDataCache<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyCache.class);

  /**
   * Interface to abstract retrieval of a type of HelixProperty and its instances
   * @param <O> type of HelixProperty
   */
  public interface PropertyCacheKeyFuncs<O extends HelixProperty> {
    /**
     * Get PropertyKey for the root of this type of object, used for LIST all objects
     * @return property key to object root
     */
    PropertyKey getRootKey(HelixDataAccessor accessor);

    /**
     * Get PropertyKey for a single object of this type, used for GET single instance of the type
     * @param objName object name
     * @return property key to the object instance
     */
    PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName);

    /**
     * Get the string to identify the object when we actually use them. It's not necessarily the
     * "id" field of HelixProperty, but could have more semantic meanings of that object type
     * @param obj object instance
     * @return object identifier
     */
    String getObjName(O obj);
  }

  // Used for serving user operations
  private Map<String, T> _objMap;

  // Used for caching data from object store - this makes it possible to have async
  // data refresh from object store
  private Map<String, T> _objCache;

  private final String _propertyDescription;
  private final boolean _useSelectiveUpdate;
  private final PropertyCacheKeyFuncs<T> _keyFuncs;

  public PropertyCache(ControlContextProvider contextProvider, String propertyDescription,
      PropertyCacheKeyFuncs<T> keyFuncs, boolean useSelectiveUpdate) {
    super(contextProvider);
    _propertyDescription = propertyDescription;
    _keyFuncs = keyFuncs;
    _objMap = new HashMap<>();
    _objCache = new HashMap<>();
    _useSelectiveUpdate = useSelectiveUpdate;
  }

  static class SelectivePropertyRefreshInputs<K extends HelixProperty> {
    private final List<PropertyKey> reloadKeys;
    private final List<PropertyKey> cachedKeys;
    private final Map<PropertyKey, K> cachedPropertyMap;

    SelectivePropertyRefreshInputs(List<PropertyKey> keysToReload,
        List<PropertyKey> currentlyCachedKeys, Map<PropertyKey, K> currentCache) {
      reloadKeys = keysToReload;
      cachedKeys = currentlyCachedKeys;
      cachedPropertyMap = currentCache;
    }

    List<PropertyKey> getCachedKeys() {
      return cachedKeys;
    }

    List<PropertyKey> getReloadKeys() {
      return reloadKeys;
    }

    Map<PropertyKey, K> getCachedPropertyMap() {
      return cachedPropertyMap;
    }
  }

  @VisibleForTesting
  SelectivePropertyRefreshInputs<T> genSelectiveUpdateInput(HelixDataAccessor accessor,
      Map<String, T> oldCache, PropertyCache.PropertyCacheKeyFuncs<T> propertyKeyFuncs) {
    // Generate keys for all current live instances
    Set<PropertyKey> latestKeys = Sets.newHashSet();
    for (String liveInstanceName : accessor.getChildNames(propertyKeyFuncs.getRootKey(accessor))) {
      latestKeys.add(propertyKeyFuncs.getObjPropertyKey(accessor, liveInstanceName));
    }

    Set<PropertyKey> oldCachedKeys = Sets.newHashSet();
    Map<PropertyKey, T> cachedObjs = new HashMap<>();
    for (String objName : oldCache.keySet()) {
      PropertyKey objKey = propertyKeyFuncs.getObjPropertyKey(accessor, objName);
      oldCachedKeys.add(objKey);
      cachedObjs.put(objKey, oldCache.get(objName));
    }
    Set<PropertyKey> cachedKeys = Sets.intersection(oldCachedKeys, latestKeys);
    Set<PropertyKey> reloadKeys = Sets.difference(latestKeys, cachedKeys);

    return new SelectivePropertyRefreshInputs<>(new ArrayList<>(reloadKeys),
        new ArrayList<>(cachedKeys), cachedObjs);
  }

  /**
   * Refresh the cache with the given data accessor
   * @param accessor helix data accessor provided by caller
   */
  public void refresh(final HelixDataAccessor accessor) {
    long start = System.currentTimeMillis();
    if (_useSelectiveUpdate) {
      doRefreshWithSelectiveUpdate(accessor);
    } else {
      doSimpleCacheRefresh(accessor);
    }
    LogUtil.logInfo(LOG, genEventInfo(),
        String.format("Refreshed %s property %s took %s ms. Selective: %s", _objMap.size(),
            _propertyDescription, System.currentTimeMillis() - start, _useSelectiveUpdate));
  }

  private void doSimpleCacheRefresh(final HelixDataAccessor accessor) {
    _objCache = accessor.getChildValuesMap(_keyFuncs.getRootKey(accessor), true);
    _objMap = new HashMap<>(_objCache);
  }

  private void doRefreshWithSelectiveUpdate(final HelixDataAccessor accessor) {
    SelectivePropertyRefreshInputs<T> input =
        genSelectiveUpdateInput(accessor, _objCache, _keyFuncs);
    Map<PropertyKey, T> updatedData = refreshProperties(accessor, input.getReloadKeys(),
        input.getCachedKeys(), input.getCachedPropertyMap());
    _objCache = propertyKeyMapToStringMap(updatedData, _keyFuncs);

    // need to separate keys so we can potentially update cache map asynchronously while
    // keeping snapshot unchanged
    _objMap = new HashMap<>(_objCache);
  }

  private Map<String, T> propertyKeyMapToStringMap(Map<PropertyKey, T> propertyKeyMap,
      PropertyCache.PropertyCacheKeyFuncs<T> objNameFunc) {
    Map<String, T> stringMap = new HashMap<>();
    for (T obj : propertyKeyMap.values()) {
      stringMap.put(objNameFunc.getObjName(obj), obj);
    }
    return stringMap;
  }

  public Map<String, T> getPropertyMap() {
    return Collections.unmodifiableMap(_objMap);
  }

  public T getPropertyByName(String name) {
    if (name == null) {
      return null;
    }
    return _objMap.get(name);
  }

  public void setPropertyMap(Map<String, T> objMap) {
    // make a copy in case objMap is modified by the caller later on
    // not updating the cache as cache is for data from data store
    _objMap = new HashMap<>(objMap);
  }

  public void setProperty(T obj) {
    _objMap.put(_keyFuncs.getObjName(obj), obj);
  }

  public void deletePropertyByName(String name) {
    _objMap.remove(name);
  }
}
