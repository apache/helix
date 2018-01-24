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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.view.dataprovider.SourceClusterDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains logics to refresh view cluster based on information from source cluster data providers
 */
public class ViewClusterRefresher {
  private static final Logger logger = LoggerFactory.getLogger(ViewClusterRefresher.class);
  private String _viewClusterName;
  private HelixDataAccessor _viewClusterDataAccessor;
  private PropertyKey.Builder _viewClusterKeyBuilder;
  private Map<String, SourceClusterDataProvider> _dataProviderMap;

  // These 3 caches stores objects that are pushed to view cluster (write-through) cache,
  // thus we don't need to read from view cluster everytime we refresh it.
  private Map<String, LiveInstance> _viewClusterLiveInstanceCache;
  private Map<String, InstanceConfig> _viewClusterInstanceConfigCache;
  private Map<String, ExternalView> _viewClusterExternalViewCache;

  public ViewClusterRefresher(String viewClusterName, HelixDataAccessor viewClusterDataAccessor,
      Map<String, SourceClusterDataProvider> dataProviderMap) {
    _viewClusterName = viewClusterName;
    _viewClusterDataAccessor = viewClusterDataAccessor;
    _dataProviderMap = dataProviderMap;
    _viewClusterLiveInstanceCache = new HashMap<>();
    _viewClusterInstanceConfigCache = new HashMap<>();
    _viewClusterExternalViewCache = new HashMap<>();
  }

  /**
   * Create / update current live instances; remove dead live instances.
   * This function assumes that all data providers have been refreshed.
   */
  public void refreshLiveInstancesInViewCluster() {
    // TODO: implement it
  }

  /**
   * Create / update current instance configs; remove dead instance configs
   * This function assumes that all data providers have been refreshed.
   */
  public void refreshInstanceConfigsInViewCluster() {
    // TODO: implement it
  }

  /**
   * Create / update currently existing external views; delete outdated external views.
   * This function assumes that all data providers have been refreshed.
   */
  public void refreshExtrenalViewsInViewCluster() {
    // TODO: implement it
  }

  /**
   * Merge external view "toMerge" into external view "source":
   *  - if partition in toMerge does not exist in source, we add it into source
   *  - if partition exist in both external views, we add all map fields from toMerge to source
   * @param source
   * @param toMerge
   */
  public static void mergeExternalViews(ExternalView source, ExternalView toMerge)
      throws IllegalArgumentException {
    if (!source.getId().equals(toMerge.getId())) {
      throw new IllegalArgumentException(String
          .format("Cannot merge ExternalViews with different ID. SourceID: %s; ToMergeID: %s",
              source.getId(), toMerge.getId()));
    }
    for (String partitionName : toMerge.getPartitionSet()) {
      if (!source.getPartitionSet().contains(partitionName)) {
        source.setStateMap(partitionName, toMerge.getStateMap(partitionName));
      } else {
        Map<String, String> mergedPartitionState = source.getStateMap(partitionName);
        mergedPartitionState.putAll(toMerge.getStateMap(partitionName));
        source.setStateMap(partitionName, mergedPartitionState);
      }
    }
  }
}
