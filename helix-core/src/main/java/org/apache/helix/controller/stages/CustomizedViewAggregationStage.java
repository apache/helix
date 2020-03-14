package org.apache.helix.controller.stages;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.caches.CustomizedViewCache;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomizedViewAggregationStage extends AbstractAsyncBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(CustomizedViewAggregationStage.class);

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.CustomizedStateViewComputeWorker;
  }

  @Override
  public void execute(final ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

    if (manager == null || resourceMap == null || cache == null) {
      throw new StageException(
          "Missing attributes in event:" + event + ". Requires ClusterManager|RESOURCES|DataCache");
    }

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    CustomizedStateOutput customizedStateOutput =
        event.getAttribute(AttributeName.CUSTOMIZED_STATE.name());

    List<CustomizedView> newCustomizedViews = new ArrayList<>();
    Set<String> monitoringResources = new HashSet<>();

    cache.refreshCustomizedViewMap(dataAccessor);
    Map<String, CustomizedViewCache> customizedViewCacheMap = cache.getCustomizedViewCacheMap();

    // remove stale customized view type from ZK and cache
    List<String> customizedViewTypesToRemove = new ArrayList<>();
    for (String stateType : customizedViewCacheMap.keySet()) {
      if (!customizedStateOutput.getAllStateTypes().contains(stateType)) {
        LogUtil.logInfo(LOG, _eventId, "Remove customizedView for stateType: " + stateType);
        dataAccessor.removeProperty(keyBuilder.customizedView(stateType));
        customizedViewTypesToRemove.add(stateType);
      }
    }

    cache.removeCustomizedViewTypes(customizedViewTypesToRemove);

    // update customized view
    for (String stateType : customizedStateOutput.getAllStateTypes()) {
      Map<String, CustomizedView> curCustomizedViews = new HashMap<>();
      CustomizedViewCache customizedViewCache = customizedViewCacheMap.get(stateType);
      if (customizedViewCache != null) {
        curCustomizedViews = customizedViewCache.getCustomizedViewMap();
      } else {
        customizedViewCache = new CustomizedViewCache(event.getClusterName(), stateType);
      }

      for (Resource resource : resourceMap.values()) {
        try {
          computeCustomizedStateView(resource, stateType, customizedStateOutput, curCustomizedViews,
              newCustomizedViews);

          List<PropertyKey> keys = new ArrayList<>();
          for (Iterator<CustomizedView> it = newCustomizedViews.iterator(); it.hasNext(); ) {
            CustomizedView view = it.next();
            String resourceName = view.getResourceName();
            keys.add(keyBuilder.customizedView(stateType, resourceName));
          }
          // add/update customized-views
          if (newCustomizedViews.size() > 0) {
            dataAccessor.setChildren(keys, newCustomizedViews);
            customizedViewCache.refresh(dataAccessor);
          }

          List<String> customizedViewsToRemove = new ArrayList<>();

          // remove stale customized views
          for (String resourceName : curCustomizedViews.keySet()) {
            if (!resourceMap.keySet().contains(resourceName)) {
              LogUtil.logInfo(LOG, _eventId, "Remove customizedView for resource: " + resourceName);
              dataAccessor.removeProperty(keyBuilder.customizedView(stateType, resourceName));
              customizedViewsToRemove.add(resourceName);
            }
          }
          customizedViewCache.removeCustomizedView(customizedViewsToRemove);
        } catch (HelixException ex) {
          LogUtil.logError(LOG, _eventId,
              "Failed to calculate customized view for resource " + resource.getResourceName(), ex);
        }
      }
    }
  }

  private void computeCustomizedStateView(final Resource resource, final String stateType,
      CustomizedStateOutput customizedStateOutput,
      final Map<String, CustomizedView> curCustomizedViews,
      List<CustomizedView> newCustomizedViews) {
    String resourceName = resource.getResourceName();
    CustomizedView view = new CustomizedView(resource.getResourceName());

    for (Partition partition : resource.getPartitions()) {
      Map<String, String> customizedStateMap =
          customizedStateOutput.getPartitionCustomizedStateMap(stateType, resourceName, partition);
      if (customizedStateMap != null && customizedStateMap.size() > 0) {
        for (String instance : customizedStateMap.keySet()) {
          view.setState(partition.getPartitionName(), instance, customizedStateMap.get(instance));
        }
      }
    }

    CustomizedView curCustomizedView = curCustomizedViews.get(resourceName);

    // compare the new customized view with current one, set only on different
    if (curCustomizedView == null || !curCustomizedView.getRecord().equals(view.getRecord())) {
      // Add customized view to the list which will be written to ZK later.
      newCustomizedViews.add(view);
    }
  }
}
