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
import java.util.Collections;
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
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.CustomizedViewMonitor;
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

    Map<String, CustomizedViewCache> customizedViewCacheMap = cache.getCustomizedViewCacheMap();

    // remove stale customized view type from ZK and cache
    Set<String> customizedTypesToRemove = new HashSet<>();
    for (String stateType : customizedViewCacheMap.keySet()) {
      if (!customizedStateOutput.getAllStateTypes().contains(stateType)) {
        LogUtil.logInfo(LOG, _eventId,
            "Remove customizedView for stateType: " + stateType + ", on cluster " + event
                .getClusterName());
        dataAccessor.removeProperty(keyBuilder.customizedView(stateType));
        customizedTypesToRemove.add(stateType);
      }
    }
    cache.removeCustomizedViewTypes(customizedTypesToRemove);

    // update customized view
    for (String stateType : customizedStateOutput.getAllStateTypes()) {
      List<CustomizedView> updatedCustomizedViews = new ArrayList<>();
      Map<String, Map<Partition, Map<String, Long>>> updatedStartTimestamps = new HashMap<>();
      Map<String, CustomizedView> curCustomizedViews = new HashMap<>();
      CustomizedViewCache customizedViewCache = customizedViewCacheMap.get(stateType);
      if (customizedViewCache != null) {
        curCustomizedViews = customizedViewCache.getCustomizedViewMap();
      }

      for (Resource resource : resourceMap.values()) {
        try {
          computeCustomizedStateView(resource, stateType, customizedStateOutput, curCustomizedViews,
              updatedCustomizedViews, updatedStartTimestamps);
        } catch (HelixException ex) {
          LogUtil.logError(LOG, _eventId,
              "Failed to calculate customized view for resource " + resource.getResourceName()
                  + ", on cluster " + event.getClusterName(), ex);
        }
      }
      List<PropertyKey> keys = new ArrayList<>();
      for (Iterator<CustomizedView> it = updatedCustomizedViews.iterator(); it.hasNext(); ) {
        CustomizedView view = it.next();
        String resourceName = view.getResourceName();
        keys.add(keyBuilder.customizedView(stateType, resourceName));
      }
      // add/update customized-views from zk and cache
      if (updatedCustomizedViews.size() > 0) {
        boolean[] success = dataAccessor.setChildren(keys, updatedCustomizedViews);
        reportLatency(event, updatedCustomizedViews, curCustomizedViews, updatedStartTimestamps,
            success);
        cache.updateCustomizedViews(stateType, updatedCustomizedViews);
      }

      // remove stale customized views from zk and cache
      List<String> customizedViewToRemove = new ArrayList<>();
      for (String resourceName : curCustomizedViews.keySet()) {
        if (!resourceMap.containsKey(resourceName)) {
          LogUtil.logInfo(LOG, _eventId,
              "Remove customizedView for resource: " + resourceName + ", on cluster " + event
                  .getClusterName());
          dataAccessor.removeProperty(keyBuilder.customizedView(stateType, resourceName));
          customizedViewToRemove.add(resourceName);
        }
      }
      cache.removeCustomizedViews(stateType, customizedViewToRemove);
    }
  }

  private void computeCustomizedStateView(final Resource resource, final String stateType,
      CustomizedStateOutput customizedStateOutput,
      final Map<String, CustomizedView> curCustomizedViews,
      List<CustomizedView> updatedCustomizedViews,
      Map<String, Map<Partition, Map<String, Long>>> updatedStartTimestamps) {
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
      updatedCustomizedViews.add(view);
      updatedStartTimestamps.put(resourceName,
          customizedStateOutput.getResourceStartTimeMap(stateType, resourceName));
    }
  }

  private void reportLatency(ClusterEvent event, List<CustomizedView> updatedCustomizedViews,
      Map<String, CustomizedView> curCustomizedViews,
      Map<String, Map<Partition, Map<String, Long>>> updatedStartTimestamps,
      boolean[] updateSuccess) {
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    if (clusterStatusMonitor == null) {
      return;
    }
    long curTime = System.currentTimeMillis();
    String clusterName = event.getClusterName();
    CustomizedViewMonitor customizedViewMonitor =
        clusterStatusMonitor.getOrCreateCustomizedViewMonitor(clusterName);

    for (int i = 0; i < updatedCustomizedViews.size(); i++) {
      CustomizedView newCV = updatedCustomizedViews.get(i);
      String resourceName = newCV.getResourceName();

      if (!updateSuccess[i]) {
        LOG.warn("Customized views are not updated successfully for resource {} on cluster {}",
            resourceName, clusterName);
        continue;
      }

      CustomizedView oldCV =
          curCustomizedViews.getOrDefault(resourceName, new CustomizedView(resourceName));

      Map<String, Map<String, String>> newPartitionStateMaps = newCV.getRecord().getMapFields();
      Map<String, Map<String, String>> oldPartitionStateMaps = oldCV.getRecord().getMapFields();
      Map<Partition, Map<String, Long>> partitionStartTimeMaps =
          updatedStartTimestamps.getOrDefault(resourceName, Collections.emptyMap());

      for (Map.Entry<String, Map<String, String>> partitionStateMapEntry : newPartitionStateMaps
          .entrySet()) {
        String partitionName = partitionStateMapEntry.getKey();
        Map<String, String> newStateMap = partitionStateMapEntry.getValue();
        Map<String, String> oldStateMap =
            oldPartitionStateMaps.getOrDefault(partitionName, Collections.emptyMap());
        if (!newStateMap.equals(oldStateMap)) {
          Map<String, Long> partitionStartTimeMap = partitionStartTimeMaps
              .getOrDefault(new Partition(partitionName), Collections.emptyMap());

          for (Map.Entry<String, String> stateMapEntry : newStateMap.entrySet()) {
            String instanceName = stateMapEntry.getKey();
            String newVal = stateMapEntry.getValue();
            // We do not calculate the latency for deleting customized state
            // So new value shouldn't be empty
            if (!newVal.isEmpty() && !newVal.equals(oldStateMap.get(instanceName))) {
              Long timestamp = partitionStartTimeMap.get(instanceName);
              if (timestamp != null && timestamp > 0) {
                customizedViewMonitor.recordUpdateToAggregationLatency(curTime - timestamp);
              } else {
                LOG.info(
                    "Failed to find customized state update time stamp for resource {} partition {}, instance {}, on cluster {} the number should be positive.",
                    resourceName, partitionName, instanceName, clusterName);
              }
            }
          }
        }
      }
    }
  }
}
