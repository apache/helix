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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixProperty;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResourceChangeDetector implements ChangeDetector. It caches resource-related metadata from
 * Helix's main resource pipeline cache (DataProvider) and the computation results of change
 * detection.
 * WARNING: the methods of this class are not thread-safe.
 */
public class ResourceChangeDetector implements ChangeDetector {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceChangeDetector.class.getName());

  private final boolean _ignoreNonTopologyChange;
  private ResourceChangeSnapshot _oldSnapshot; // snapshot for previous pipeline run
  private ResourceChangeSnapshot _newSnapshot; // snapshot for this pipeline run

  // The following caches the computation results
  private Map<HelixConstants.ChangeType, Collection<String>> _changedItems = new HashMap<>();
  private Map<HelixConstants.ChangeType, Collection<String>> _addedItems = new HashMap<>();
  private Map<HelixConstants.ChangeType, Collection<String>> _removedItems = new HashMap<>();

  public ResourceChangeDetector(boolean ignoreNonTopologyChange) {
    _newSnapshot = new ResourceChangeSnapshot();
    _ignoreNonTopologyChange = ignoreNonTopologyChange;
  }

  public ResourceChangeDetector() {
    this(false);
  }

  /**
   * Compare the underlying HelixProperty objects and produce a collection of names of changed
   * properties.
   * @return
   */
  private Collection<String> getChangedItems(Map<String, ? extends HelixProperty> oldPropertyMap,
      Map<String, ? extends HelixProperty> newPropertyMap) {
    Collection<String> changedItems = new HashSet<>();
    oldPropertyMap.forEach((name, property) -> {
      if (newPropertyMap.containsKey(name)
          && !property.getRecord().equals(newPropertyMap.get(name).getRecord())) {
        changedItems.add(name);
      }
    });
    return changedItems;
  }

  /**
   * Return a collection of names that are newly added.
   * @return
   */
  private Collection<String> getAddedItems(Map<String, ? extends HelixProperty> oldPropertyMap,
      Map<String, ? extends HelixProperty> newPropertyMap) {
    return Sets.difference(newPropertyMap.keySet(), oldPropertyMap.keySet());
  }

  /**
   * Return a collection of names that were removed.
   * @return
   */
  private Collection<String> getRemovedItems(Map<String, ? extends HelixProperty> oldPropertyMap,
      Map<String, ? extends HelixProperty> newPropertyMap) {
    return Sets.difference(oldPropertyMap.keySet(), newPropertyMap.keySet());
  }

  private void clearCachedComputation() {
    _changedItems.clear();
    _addedItems.clear();
    _removedItems.clear();
  }

  /**
   * Based on the change type given and propertyMap type, call the right getters for propertyMap.
   * @param changeType
   * @param snapshot
   * @return
   */
  private Map<String, ? extends HelixProperty> determinePropertyMapByType(
      HelixConstants.ChangeType changeType, ResourceChangeSnapshot snapshot) {
    switch (changeType) {
    case INSTANCE_CONFIG:
      return snapshot.getAssignableInstanceConfigMap();
    case IDEAL_STATE:
      return snapshot.getIdealStateMap();
    case RESOURCE_CONFIG:
      return snapshot.getResourceConfigMap();
    case LIVE_INSTANCE:
      return snapshot.getAssignableLiveInstances();
    case CLUSTER_CONFIG:
      ClusterConfig config = snapshot.getClusterConfig();
      if (config == null) {
        return Collections.emptyMap();
      } else {
        return Collections.singletonMap(config.getClusterName(), config);
      }
    default:
      LOG.warn(
          "ResourceChangeDetector cannot determine propertyMap for the given ChangeType: {}. Returning an empty map.",
          changeType);
      return Collections.emptyMap();
    }
  }

  /**
   * Makes the current newSnapshot the oldSnapshot and reads in the up-to-date snapshot for change
   * computation. To be called in the controller pipeline.
   * @param dataProvider newly refreshed DataProvider (cache)
   */
  public synchronized void updateSnapshots(ResourceControllerDataProvider dataProvider) {
    // If there are changes, update internal states
    _oldSnapshot = new ResourceChangeSnapshot(_newSnapshot);
    _newSnapshot = new ResourceChangeSnapshot(dataProvider, _ignoreNonTopologyChange);
    dataProvider.clearRefreshedChangeTypes();

    // Invalidate cached computation
    clearCachedComputation();
  }

  public synchronized void resetSnapshots() {
    _newSnapshot = new ResourceChangeSnapshot();
    clearCachedComputation();
  }

  @Override
  public synchronized Collection<HelixConstants.ChangeType> getChangeTypes() {
    return Collections.unmodifiableSet(_newSnapshot.getChangedTypes());
  }

  @Override
  public synchronized Collection<String> getChangesByType(HelixConstants.ChangeType changeType) {
    return _changedItems.computeIfAbsent(changeType,
        changedItems -> getChangedItems(determinePropertyMapByType(changeType, _oldSnapshot),
            determinePropertyMapByType(changeType, _newSnapshot)));
  }

  @Override
  public synchronized Collection<String> getAdditionsByType(HelixConstants.ChangeType changeType) {
    return _addedItems.computeIfAbsent(changeType,
        changedItems -> getAddedItems(determinePropertyMapByType(changeType, _oldSnapshot),
            determinePropertyMapByType(changeType, _newSnapshot)));
  }

  @Override
  public synchronized Collection<String> getRemovalsByType(HelixConstants.ChangeType changeType) {
    return _removedItems.computeIfAbsent(changeType,
        changedItems -> getRemovedItems(determinePropertyMapByType(changeType, _oldSnapshot),
            determinePropertyMapByType(changeType, _newSnapshot)));
  }

  /**
   * @return A map contains all the changed items that are categorized by the change types.
   */
  public Map<HelixConstants.ChangeType, Set<String>> getAllChanges() {
    return getChangeTypes().stream()
        .collect(Collectors.toMap(changeType -> changeType, changeType -> {
          Set<String> itemKeys = new HashSet<>();
          itemKeys.addAll(getAdditionsByType(changeType));
          itemKeys.addAll(getChangesByType(changeType));
          itemKeys.addAll(getRemovalsByType(changeType));
          return itemKeys;
        })).entrySet().stream().filter(changeEntry -> !changeEntry.getValue().isEmpty()).collect(
            Collectors
                .toMap(changeEntry -> changeEntry.getKey(), changeEntry -> changeEntry.getValue()));
  }
}
