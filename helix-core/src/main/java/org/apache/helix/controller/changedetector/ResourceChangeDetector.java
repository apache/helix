package org.apache.helix.controller.changedetector;

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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;

/**
 * ResourceChangeDetector implements ChangeDetector. It caches resource-related metadata from
 * Helix's main resource pipeline cache (DataProvider) and the computation results of change
 * detection.
 * WARNING: the methods of this class are not thread-safe.
 */
public class ResourceChangeDetector implements ChangeDetector {

  private ResourceChangeCache _oldCache; // cache snapshot for previous pipeline run
  private ResourceChangeCache _newCache; // cache snapshot for this pipeline run

  private Map<String, ? extends HelixProperty> _oldPropertyMap;
  private Map<String, ? extends HelixProperty> _newPropertyMap;

  // The following caches the computation results
  private Map<HelixConstants.ChangeType, Collection<String>> _changedItems = new HashMap<>();
  private Map<HelixConstants.ChangeType, Collection<String>> _addedItems = new HashMap<>();
  private Map<HelixConstants.ChangeType, Collection<String>> _removedItems = new HashMap<>();

  /**
   * Compare the underlying HelixProperty objects and produce a collection of names of changed
   * properties.
   * @return
   */
  private Collection<String> getChangedItems() {
    Collection<String> changedItems = new HashSet<>();
    _oldPropertyMap.forEach((name, property) -> {
      if (_newPropertyMap.containsKey(name) && !property.equals(_newPropertyMap.get(name))) {
        changedItems.add(name);
      }
    });
    return changedItems;
  }

  /**
   * Return a collection of names that are newly added.
   * @return
   */
  private Collection<String> getAddedItems() {
    Collection<String> addedItems = new HashSet<>(_newPropertyMap.keySet());
    addedItems.removeAll(_oldPropertyMap.keySet());
    return addedItems;
  }

  /**
   * Return a collection of names that were removed.
   * @return
   */
  private Collection<String> getRemovedItems() {
    Collection<String> removedItems = new HashSet<>(_oldPropertyMap.keySet());
    removedItems.removeAll(_newPropertyMap.keySet());
    return removedItems;
  }

  /**
   * Based on the change type given, call the right getters for two property maps and save the
   * references as oldPropertyMap and newPropertyMap.
   * @param changeType
   */
  private void determinePropertyMapByType(HelixConstants.ChangeType changeType) {
    switch (changeType) {
    case INSTANCE_CONFIG:
      _oldPropertyMap = _oldCache.getInstanceConfigMap();
      _newPropertyMap = _newCache.getInstanceConfigMap();
      break;
    case IDEAL_STATE:
      _oldPropertyMap = _oldCache.getIdealStateMap();
      _newPropertyMap = _newCache.getIdealStateMap();
      break;
    case RESOURCE_CONFIG:
      _oldPropertyMap = _oldCache.getResourceConfigMap();
      _newPropertyMap = _newCache.getResourceConfigMap();
      break;
    case LIVE_INSTANCE:
      _oldPropertyMap = _oldCache.getLiveInstances();
      _newPropertyMap = _newCache.getLiveInstances();
      break;
    default:
      throw new HelixException(String
          .format("ResourceChangeDetector does not support the given change type: %s", changeType));
    }
  }

  /**
   * Makes the current newCache the oldCache and reads in the up-to-date cache for change
   * computation. To be called in the controller pipeline.
   * @param dataProvider newly refreshed DataProvider (cache)
   */
  public synchronized void updateCache(ResourceControllerDataProvider dataProvider) {
    // Do nothing if nothing has changed
    if (dataProvider.getRefreshedChangeTypes().isEmpty()) {
      return;
    }

    // If there are changes, update internal states
    _oldCache = new ResourceChangeCache(_newCache);
    _newCache = new ResourceChangeCache(dataProvider);

    // Invalidate cached computation
    _changedItems.clear();
    _addedItems.clear();
    _removedItems.clear();
  }

  @Override
  public Collection<HelixConstants.ChangeType> getChangeTypes() {
    return Collections.unmodifiableSet(_newCache.getChangedTypes());
  }

  @Override
  public Collection<String> getChangesByType(HelixConstants.ChangeType changeType) {
    return _changedItems.computeIfAbsent(changeType, changedItems -> {
      determinePropertyMapByType(changeType);
      return getChangedItems();
    });
  }

  @Override
  public Collection<String> getAdditionsByType(HelixConstants.ChangeType changeType) {
    return _addedItems.computeIfAbsent(changeType, changedItems -> {
      determinePropertyMapByType(changeType);
      return getAddedItems();
    });
  }

  @Override
  public Collection<String> getRemovalsByType(HelixConstants.ChangeType changeType) {
    return _removedItems.computeIfAbsent(changeType, changedItems -> {
      determinePropertyMapByType(changeType);
      return getRemovedItems();
    });
  }
}
