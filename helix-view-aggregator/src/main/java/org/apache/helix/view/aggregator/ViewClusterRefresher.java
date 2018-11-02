package org.apache.helix.view.aggregator;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.view.dataprovider.SourceClusterDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains logics to refresh view cluster based on information from source cluster data
 * providers.
 * This class assumes SourceClusterDataProviders have its caches refreshed already.
 */
public class ViewClusterRefresher {
  private static final Logger logger = LoggerFactory.getLogger(ViewClusterRefresher.class);
  private String _viewClusterName;
  private HelixDataAccessor _viewClusterDataAccessor;
  private Set<SourceClusterDataProvider> _dataProviderView;

  // These 3 caches stores objects that are pushed to view cluster (write-through) cache,
  // thus we don't need to read from view cluster everytime we refresh it.
  private Map<String, HelixProperty> _viewClusterLiveInstanceCache;
  private Map<String, HelixProperty> _viewClusterInstanceConfigCache;
  private Map<String, HelixProperty> _viewClusterExternalViewCache;

  public ViewClusterRefresher(String viewClusterName, HelixDataAccessor viewClusterDataAccessor) {
    _viewClusterName = viewClusterName;
    _viewClusterDataAccessor = viewClusterDataAccessor;
    _viewClusterLiveInstanceCache = new HashMap<>();
    _viewClusterInstanceConfigCache = new HashMap<>();
    _viewClusterExternalViewCache = new HashMap<>();
  }

  private class ClusterPropertyDiff {
    /**
     * List of names of objects to set (create or modify)
     */
    List<String> _keysToSet;

    /**
     * List of actual objects represented by _keysToSet
     */
    List<HelixProperty> _propertiesToSet;

    /**
     * List of names of objects to delete
     */
    List<String> _keysToDelete;

    public ClusterPropertyDiff() {
      _keysToSet = new ArrayList<>();
      _propertiesToSet = new ArrayList<>();
      _keysToDelete = new ArrayList<>();
    }

    public void addPropertyToSet(String key, HelixProperty obj) {
      // batch setChildren API needs keys and objects to have corresponding order
      _keysToSet.add(key);
      _propertiesToSet.add(obj);
    }

    public void addPropertiesToDelete(Collection<? extends String> keys) {
      _keysToDelete.addAll(keys);
    }

    public List<String> getKeysToSet() {
      return Collections.unmodifiableList(_keysToSet);
    }

    public List<HelixProperty> getPropertiesToSet() {
      return Collections.unmodifiableList(_propertiesToSet);
    }

    public List<String> getKeysToDelete() {
      return Collections.unmodifiableList(_keysToDelete);
    }
  }

  public void updateProviderView(Set<SourceClusterDataProvider> dataProviderView) {
    _dataProviderView = dataProviderView;
  }

  /**
   * Create / update / delete property of given type in view cluster, based on data change from
   * source clusters.
   *
   * @param propertyType type of property to refresh in view cluster
   * @return true if successfully refreshed all instances of the given property else false
   * @throws IllegalArgumentException throws exception when give type is not supported
   */
  public boolean refreshPropertiesInViewCluster(PropertyType propertyType)
      throws IllegalArgumentException {
    boolean ok = false;
    Set<String> listedNamesInView;
    Set<String> listedNamesInSource = new HashSet<>();
    Map<String, HelixProperty> sourceProperties = new HashMap<>();
    Map<String, HelixProperty> viewClusterPropertyCache =
        getViewClusterPropertyCache(propertyType);
    if (viewClusterPropertyCache == null) {
      throw new IllegalArgumentException(
          "Cannot find view cluster property cache. Property: " + propertyType.name());
    }

    try {
      listedNamesInView =
          new HashSet<>(_viewClusterDataAccessor.getChildNames(getPropertyKey(propertyType, null)));
      // Prepare data
      for (SourceClusterDataProvider provider : _dataProviderView) {
        if (!provider.getPropertiesToAggregate().contains(propertyType)) {
          logger.info(String
              .format("SourceCluster %s does not need to aggregate %s, skip.", provider.getName(),
                  propertyType.name()));
          continue;
        }
        switch (propertyType) {
        case INSTANCES:
          listedNamesInSource.addAll(provider.getInstanceConfigNames());
          sourceProperties.putAll(provider.getInstanceConfigMap());
          break;
        case LIVEINSTANCES:
          listedNamesInSource.addAll(provider.getLiveInstanceNames());
          sourceProperties.putAll(provider.getLiveInstances());
          break;
        case EXTERNALVIEW:
          listedNamesInSource.addAll(provider.getExternalViewNames());
          for (Map.Entry<String, ExternalView> entry : provider.getExternalViews().entrySet()) {
            String resourceName = entry.getKey();
            if (!sourceProperties.containsKey(resourceName)) {
              sourceProperties.put(resourceName, new ExternalView(resourceName));
            }
            mergeExternalViews((ExternalView) sourceProperties.get(resourceName), entry.getValue());
          }
          break;
        default:
          // Will NOT come here as for unsupported property type, exception will be thrown out
          // earlier
          break;
        }
      }

      // Perform refresh
      ok = doRefresh(propertyType, listedNamesInView, listedNamesInSource, sourceProperties,
          viewClusterPropertyCache);
    } catch (Exception e) {
      logger.warn(String
          .format("Caught exception during refreshing %s for view cluster %s", propertyType.name(),
              _viewClusterName), e);
    }
    logRefreshResult(propertyType, ok);

    return ok;
  }

  /**
   * Merge external view "toMerge" into external view "source":
   *  - if partition in toMerge does not exist in source, we add it into source
   *  - if partition exist in both external views, we add all map fields from toMerge to source
   *
   * @param source
   * @param toMerge
   */
  private static void mergeExternalViews(ExternalView source, ExternalView toMerge)
      throws IllegalArgumentException {
    if (!source.getId().equals(toMerge.getId())) {
      throw new IllegalArgumentException(String
          .format("Cannot merge ExternalViews with different ID. SourceID: %s; ToMergeID: %s",
              source.getId(), toMerge.getId()));
    }
    for (String partitionName : toMerge.getPartitionSet()) {
      // Deep copying state map to avoid modifying source cache
      if (!source.getPartitionSet().contains(partitionName)) {
        source.setStateMap(partitionName, new TreeMap<String, String>());
      }
      source.getStateMap(partitionName).putAll(toMerge.getStateMap(partitionName));
    }
  }

  /**
   * Based on property names in view cluster, property names in source clusters, and all cached
   * properties in source clusters, generate ClusterPropertyDiff that contains information about
   * what to add / update or delete
   *
   * @param viewPropertyNames names of all properties (i.e. liveInstances) in view cluster
   * @param sourcePropertyNames names of all properties (i.e. liveInstances) in all source clusters
   * @param cachedSourceProperties all cached properties from source clusters
   * @param viewClusterPropertyCache all properties that are previously set successfully to view cluster
   * @param <T> extends HelixProperty
   * @return ClusterPropertyDiff object contains diff information
   */
  private <T extends HelixProperty> ClusterPropertyDiff calculatePropertyDiff(
      Set<String> viewPropertyNames, Set<String> sourcePropertyNames,
      Map<String, T> cachedSourceProperties, Map<String, T> viewClusterPropertyCache) {
    ClusterPropertyDiff diff = new ClusterPropertyDiff();

    // items whose names are in view cluster but not in source should be removed for sure
    Set<String> toDelete = new HashSet<>(viewPropertyNames);
    toDelete.removeAll(sourcePropertyNames);
    diff.addPropertiesToDelete(toDelete);

    for (Map.Entry<String, T> sourceProperty : cachedSourceProperties.entrySet()) {
      String name = sourceProperty.getKey();
      HelixProperty property = sourceProperty.getValue();

      // cache refresh happens earlier than list curNames, so if cache is still in curNames,
      // we confirm that this is a valid live instance. This is necessary because ZK
      // can possibly not return all children content in a refresh, but list child names
      // will reliably return all children names.
      //
      // Else, either this child is already deleted, or we fail to retrieve information
      // from a cache refresh. either way, we will leave it to next ViewClusterRefresh cycle
      // to confirm state
      if (property != null && sourcePropertyNames.contains(name) && (
          !viewClusterPropertyCache.containsKey(name) || !viewClusterPropertyCache.get(name)
              .getRecord().equals(property.getRecord()))) {
        diff.addPropertyToSet(name, property);
      }
    }
    return diff;
  }

  /**
   * Refresh view cluster regarding a particular property based given source of truths.
   * Steps are:
   *  - Calculate diff based on propertyNamesInView, propertyNamesInSource and
   *    cachedSourceProperties
   *  - Generate property keys for properties to set / delete
   *  - Delete properties
   *  - Set properties
   *
   * @param propertyType type of property to refresh
   * @param viewPropertyNames all names of the target properties in view cluster
   * @param sourcePropertyNames all names of the target properties in source clusters
   * @param cachedSourceProperties all up-to-date cached properties in source cluster
   * @param viewClusterPropertyCache view cluster cache
   * @param <T> extends HelixProperty
   * @return true if all required refreshes are successful, else false
   */
  private <T extends HelixProperty> boolean doRefresh(PropertyType propertyType,
      Set<String> viewPropertyNames, Set<String> sourcePropertyNames,
      Map<String, T> cachedSourceProperties, Map<String, T> viewClusterPropertyCache) {
    boolean ok = true;
    // Calculate diff
    ClusterPropertyDiff diff =
        calculatePropertyDiff(viewPropertyNames, sourcePropertyNames, cachedSourceProperties,
            viewClusterPropertyCache);

    // Generate property keys
    List<PropertyKey> keysToSet = new ArrayList<>();
    List<PropertyKey> keysToDelete = new ArrayList<>();
    for (String name : diff.getKeysToSet()) {
      PropertyKey key = getPropertyKey(propertyType, name);
      if (key != null) {
        keysToSet.add(key);
      }
    }

    for (String name : diff.getKeysToDelete()) {
      PropertyKey key = getPropertyKey(propertyType, name);
      if (key != null) {
        keysToDelete.add(key);
      }
    }

    // Delete outdated properties
    if (!deleteProperties(keysToDelete, viewClusterPropertyCache)) {
      ok = false;
    }

    // Add or update changed properties
    if (!addOrUpdateProperties(keysToSet, diff.getPropertiesToSet(), viewClusterPropertyCache)) {
      ok = false;
    }
    return ok;
  }

  /**
   * Based on type and property name, generate property key
   * @param propertyType type of the property
   * @param propertyName name of the property. If null, return key of the parent of
   *                     all properties of given type
   * @return property key
   */
  private PropertyKey getPropertyKey(PropertyType propertyType, String propertyName) {
    switch (propertyType) {
    case INSTANCES:
      return propertyName == null
          ? _viewClusterDataAccessor.keyBuilder().instanceConfigs()
          : _viewClusterDataAccessor.keyBuilder().instanceConfig(propertyName);
    case LIVEINSTANCES:
      return propertyName == null
          ? _viewClusterDataAccessor.keyBuilder().liveInstances()
          : _viewClusterDataAccessor.keyBuilder().liveInstance(propertyName);
    case EXTERNALVIEW:
      return propertyName == null
          ? _viewClusterDataAccessor.keyBuilder().externalViews()
          : _viewClusterDataAccessor.keyBuilder().externalView(propertyName);
    default:
      return null;
    }
  }

  private Map<String, HelixProperty> getViewClusterPropertyCache(PropertyType propertyType) {
    switch (propertyType) {
    case INSTANCES:
      return _viewClusterInstanceConfigCache;
    case LIVEINSTANCES:
      return _viewClusterLiveInstanceCache;
    case EXTERNALVIEW:
      return _viewClusterExternalViewCache;
    default:
      return null;
    }
  }

  /**
   * Create or Update properties in ZK specified by a list of property keys. Update the given cache
   * for the objects that got successfully created or updated in ZK
   * @param keysToAddOrUpdate
   * @param objects
   * @param cache
   * @param <T> HelixProperty
   *
   * @return true if all objects are successfully created or updated, else false
   */
  private <T extends HelixProperty> boolean addOrUpdateProperties(
      List<PropertyKey> keysToAddOrUpdate, List<HelixProperty> objects, Map<String, T> cache) {
    boolean ok = true;
    logger.info(
        String.format("AddOrUpdate %s objects: %s", keysToAddOrUpdate.size(), keysToAddOrUpdate));
    boolean[] addOrUpdateResults = _viewClusterDataAccessor.setChildren(keysToAddOrUpdate, objects);
    for (int i = 0; i < addOrUpdateResults.length; i++) {
      if (!addOrUpdateResults[i]) {
        // Don't add item to cache yet - will retry during next refresh
        logger.warn(String.format("Failed to create or update live instance %s, will retry later",
            keysToAddOrUpdate.get(i).getPath()));
        ok = false;
      } else {
        // Successfully updated ViewClusterZK, proceed to update cache
        @SuppressWarnings("unchecked")
        T property = (T) objects.get(i);
        cache.put(getBaseObjectNameFromPropertyKey(keysToAddOrUpdate.get(i)), property);
      }
    }
    return ok;
  }

  /**
   * Delete properties in ZK specified by a list of property keys. Update the given cache
   * for the objects that got successfully deleted in ZK
   * @param keysToDelete
   * @param cache
   * @param <T> HelixProperty
   * @return true if all objects got successfully deleted else false
   */
  private <T extends HelixProperty> boolean deleteProperties(List<PropertyKey> keysToDelete,
      Map<String, T> cache) {
    boolean ok = true;
    logger.info(String.format("Deleting %s objects: %s", keysToDelete.size(), keysToDelete));
    for (PropertyKey key : keysToDelete) {
      if (!_viewClusterDataAccessor.removeProperty(key)) {
        // Don't remove item from cache yet - will retry during next refresh
        ok = false;
        logger.warn(String.format("Failed to create or update live instance %s, will retry later",
            key.getPath()));
      } else {
        // Successfully updated ViewClusterZK, proceed to update cache
        cache.remove(getBaseObjectNameFromPropertyKey(key));
      }
    }
    return ok;
  }

  /**
   * Parse property key and get the id of the ZNode that property key represents
   * @param key
   * @return
   */
  private static String getBaseObjectNameFromPropertyKey(PropertyKey key) {
    String[] params = key.getParams();
    return params[params.length - 1];
  }

  private void logRefreshResult(PropertyType type, boolean ok) {
    if (!ok) {
      logger.warn(String
          .format("Failed to refresh all %s for view cluster %s, will retry",
              type.name(), _viewClusterName));
    } else {
      logger.info(String.format("Successfully refreshed all %s for view cluster %s",
          type.name(), _viewClusterName));
    }
  }
}
