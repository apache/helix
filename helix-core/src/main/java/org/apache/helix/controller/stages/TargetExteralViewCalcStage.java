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
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

public class TargetExteralViewCalcStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(TargetExteralViewCalcStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    LOG.info("START TargetExteralViewCalcStage.process()");
    long startTime = System.currentTimeMillis();
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    ClusterConfig clusterConfig = cache.getClusterConfig();

    if (cache.isTaskCache() || !clusterConfig.isTargetExternalViewEnabled()) {
      return;
    }

    HelixManager helixManager = event.getAttribute(AttributeName.helixmanager.name());
    HelixDataAccessor accessor = helixManager.getHelixDataAccessor();

    if (!accessor.getBaseDataAccessor()
        .exists(accessor.keyBuilder().targetExternalViews().getPath(), AccessOption.PERSISTENT)) {
      accessor.getBaseDataAccessor()
          .create(accessor.keyBuilder().targetExternalViews().getPath(), null,
              AccessOption.PERSISTENT);
    }

    IntermediateStateOutput intermediateAssignment =
        event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());

    List<PropertyKey> keys = new ArrayList<>();
    List<HelixProperty> targetExternalViews = new ArrayList<>();

    for (String resourceName : intermediateAssignment.resourceSet()) {
      if (cache.getIdealState(resourceName) == null || cache.getIdealState(resourceName).isExternalViewDisabled()) {
        continue;
      }
      Resource resource = resourceMap.get(resourceName);
      if (resource != null) {
        PartitionStateMap partitionStateMap =
            intermediateAssignment.getPartitionStateMap(resourceName);
        Map<String, Map<String, String>> assignmentToPersist = convertToMapFields(partitionStateMap.getStateMap());

        ExternalView targetExternalView = cache.getTargetExternalView(resourceName);
        if (targetExternalView == null) {
          targetExternalView = new ExternalView(resourceName);
          targetExternalView.getRecord()
              .getSimpleFields()
              .putAll(cache.getIdealState(resourceName).getRecord().getSimpleFields());
        }
        if (assignmentToPersist != null && !targetExternalView.getRecord().getMapFields()
            .equals(assignmentToPersist)) {
          targetExternalView.getRecord().setMapFields(assignmentToPersist);
          keys.add(accessor.keyBuilder().targetExternalView(resourceName));
          targetExternalViews.add(targetExternalView);
          cache.updateTargetExternalView(resourceName, targetExternalView);
        }
      }
    }
    accessor.setChildren(keys, targetExternalViews);

    long endTime = System.currentTimeMillis();
    LOG.info(
        "END TargetExteralViewCalcStage.process() for cluster " + cache.getClusterName() + " took " + (
            endTime - startTime) + " ms");
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
