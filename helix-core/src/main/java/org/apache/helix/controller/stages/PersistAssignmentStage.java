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

import java.util.Map;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.log4j.Logger;

/**
 * Persist the ResourceAssignment of each resource that went through rebalancing
 */
public class PersistAssignmentStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(PersistAssignmentStage.class);

  @Override public void process(ClusterEvent event) throws Exception {
    LOG.info("START PersistAssignmentStage.process()");
    long startTime = System.currentTimeMillis();

    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    ClusterConfig clusterConfig = cache.getClusterConfig();

    if (clusterConfig.isPersistBestPossibleAssignment()) {
      HelixManager helixManager = event.getAttribute("helixmanager");
      HelixDataAccessor accessor = helixManager.getHelixDataAccessor();
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      BestPossibleStateOutput assignments =
          event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
      Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());

      for (String resourceId : assignments.resourceSet()) {
        Resource resource = resourceMap.get(resourceId);
        if (resource != null) {
          boolean changed = false;
          Map<Partition, Map<String, String>> assignment = assignments.getResourceMap(resourceId);
          IdealState idealState = cache.getIdealState(resourceId);
          if (idealState == null) {
            LOG.warn("IdealState not found for resource " + resourceId);
            continue;
          }
          IdealState.RebalanceMode mode = idealState.getRebalanceMode();
          if (!mode.equals(IdealState.RebalanceMode.SEMI_AUTO) && !mode
              .equals(IdealState.RebalanceMode.FULL_AUTO)) {
            // do not persist assignment for resource in neither semi or full auto.
            continue;
          }
          for (Partition partition : resource.getPartitions()) {
            Map<String, String> instanceMap = assignment.get(partition);
            Map<String, String> existInstanceMap =
                idealState.getInstanceStateMap(partition.getPartitionName());
            if (instanceMap == null && existInstanceMap == null) {
              continue;
            }
            if (instanceMap == null || existInstanceMap == null || !instanceMap
                .equals(existInstanceMap)) {
              changed = true;
              break;
            }
          }
          if (changed) {
            for (Partition partition : assignment.keySet()) {
              Map<String, String> instanceMap = assignment.get(partition);
              idealState.setInstanceStateMap(partition.getPartitionName(), instanceMap);
            }
            accessor.setProperty(keyBuilder.idealStates(resourceId), idealState);
          }
        }
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.info("END PersistAssignmentStage.process(), took " + (endTime - startTime) + " ms");
  }
}
