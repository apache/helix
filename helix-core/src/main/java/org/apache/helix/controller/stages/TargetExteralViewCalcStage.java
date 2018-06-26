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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class TargetExteralViewCalcStage extends AbstractBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(TargetExteralViewCalcStage.class);

  @Override
  public void process(final ClusterEvent event) throws Exception {
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    DedupEventProcessor<String, Runnable> tevWorker =
        getAsyncWorkerFromClusterEvent(event, AsyncWorkerType.TargetExternalViewCalcWorker);

    ClusterConfig clusterConfig = cache.getClusterConfig();

    if (cache.isTaskCache() || !clusterConfig.isTargetExternalViewEnabled()) {
      return;
    }

    if (tevWorker == null) {
      LOG.info("Generating target external view synchronously for cluster {}, {} pipeline",
          cache.getClusterName(), cache.isTaskCache() ? "TASK" : "RESOURCE");
      doCalculateTargetExternalView(event);
    } else {
      // We have an async worker so update external view asynchronously
      LOG.info("Sending target external view generating task for cluster {}, {} pipeline to worker",
          cache.getClusterName(), cache.isTaskCache() ? "TASK" : "RESOURCE");
      tevWorker.queueEvent(getAsyncTaskDedupType(cache.isTaskCache()), new Runnable() {
        @Override
        public void run() {
          doCalculateTargetExternalView(event);
        }
      });
    }

  }

  private void doCalculateTargetExternalView(final ClusterEvent event) {
    HelixManager helixManager = event.getAttribute(AttributeName.helixmanager.name());
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    HelixDataAccessor accessor = helixManager.getHelixDataAccessor();

    BestPossibleStateOutput bestPossibleAssignments =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    IntermediateStateOutput intermediateAssignments =
        event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());

    long startTimeStamp = System.currentTimeMillis();
    LOG.info("START: computing target external view for cluster {}, {} pipeline",
        cache.getClusterName(), cache.isTaskCache() ? "TASK" : "RESOURCE");

    if (!accessor.getBaseDataAccessor()
        .exists(accessor.keyBuilder().targetExternalViews().getPath(), AccessOption.PERSISTENT)) {
      accessor.getBaseDataAccessor()
          .create(accessor.keyBuilder().targetExternalViews().getPath(), null,
              AccessOption.PERSISTENT);
    }

    List<PropertyKey> keys = new ArrayList<>();
    List<ExternalView> targetExternalViews = new ArrayList<>();

    for (String resourceName : bestPossibleAssignments.resourceSet()) {
      if (cache.getIdealState(resourceName) == null || cache.getIdealState(resourceName).isExternalViewDisabled()) {
        continue;
      }
      Resource resource = resourceMap.get(resourceName);
      if (resource != null) {
        PartitionStateMap partitionStateMap =
            intermediateAssignments.getPartitionStateMap(resourceName);
        Map<String, Map<String, String>> intermediateAssignment = convertToMapFields(
            partitionStateMap.getStateMap());

        Map<String, List<String>> preferenceLists =
            bestPossibleAssignments.getPreferenceLists(resourceName);

        boolean needPersist = false;
        ExternalView targetExternalView = cache.getTargetExternalView(resourceName);
        if (targetExternalView == null) {
          targetExternalView = new ExternalView(resourceName);
          targetExternalView.getRecord()
              .getSimpleFields()
              .putAll(cache.getIdealState(resourceName).getRecord().getSimpleFields());
          needPersist = true;
        }

        if (preferenceLists != null && !targetExternalView.getRecord().getListFields()
            .equals(preferenceLists)) {
          targetExternalView.getRecord().setListFields(preferenceLists);
          needPersist = true;
        }

        if (intermediateAssignment != null && !targetExternalView.getRecord().getMapFields()
            .equals(intermediateAssignment)) {
          targetExternalView.getRecord().setMapFields(intermediateAssignment);
          needPersist = true;
        }

        if (needPersist) {
          keys.add(accessor.keyBuilder().targetExternalView(resourceName));
          targetExternalViews.add(targetExternalView);
          cache.updateTargetExternalView(resourceName, targetExternalView);
        }
      }
    }
    // TODO (HELIX-964): remove TEV when idealstate is removed
    accessor.setChildren(keys, targetExternalViews);

    long endTimeStamp = System.currentTimeMillis();
    LOG.info("END: computing target external view for cluster {}, {} pipeline. Took: {} ms",
        cache.getClusterName(), cache.isTaskCache() ? "TASK" : "RESOURCE",
        endTimeStamp - startTimeStamp);
  }

  private Map<String, Map<String, String>> convertToMapFields(
      Map<Partition, Map<String, String>> partitionMapMap) {
    Map<String, Map<String, String>> mapFields = Maps.newHashMap();
    for (Partition p : partitionMapMap.keySet()) {
      mapFields.put(p.getPartitionName(), new HashMap<>(partitionMapMap.get(p)));
    }
    return mapFields;
  }

}
