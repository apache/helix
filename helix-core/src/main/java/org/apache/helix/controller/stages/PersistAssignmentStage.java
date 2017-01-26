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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.common.PartitionStateMap;
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

    if (!clusterConfig.isPersistBestPossibleAssignment() && !clusterConfig
        .isPersistIntermediateAssignment()) {
      return;
    }

    BestPossibleStateOutput bestPossibleAssignment =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    HelixManager helixManager = event.getAttribute("helixmanager");
    HelixDataAccessor accessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());

    for (String resourceId : bestPossibleAssignment.resourceSet()) {
      Resource resource = resourceMap.get(resourceId);
      if (resource != null) {
        final IdealState idealState = cache.getIdealState(resourceId);
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

        boolean needPersist = false;
        if (mode.equals(IdealState.RebalanceMode.FULL_AUTO)) {
          // persist preference list in ful-auto mode.
          Map<String, List<String>> newLists =
              bestPossibleAssignment.getPreferenceLists(resourceId);
          if (newLists != null && hasPreferenceListChanged(newLists, idealState)) {
            idealState.setPreferenceLists(newLists);
            needPersist = true;
          }
        }

        PartitionStateMap partitionStateMap =
            bestPossibleAssignment.getPartitionStateMap(resourceId);
        if (clusterConfig.isPersistIntermediateAssignment()) {
          IntermediateStateOutput intermediateAssignment =
              event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
          partitionStateMap = intermediateAssignment.getPartitionStateMap(resourceId);
        }

        Map<Partition, Map<String, String>> assignmentToPersist = partitionStateMap.getStateMap();

        if (assignmentToPersist != null && hasInstanceMapChanged(assignmentToPersist, idealState)) {
          for (Partition partition : assignmentToPersist.keySet()) {
            Map<String, String> instanceMap = assignmentToPersist.get(partition);
            idealState.setInstanceStateMap(partition.getPartitionName(), instanceMap);
          }
          needPersist = true;
        }

        if (needPersist) {
          // Update instead of set to ensure any intermediate changes that the controller does not update are kept.
          accessor.updateProperty(keyBuilder.idealStates(resourceId), new DataUpdater<ZNRecord>() {
            @Override public ZNRecord update(ZNRecord current) {
              if (current != null) {
                // Overwrite MapFields and ListFields items with the same key.
                // Note that default merge will keep old values in the maps or lists unchanged, which is not desired.
                current.getMapFields().putAll(idealState.getRecord().getMapFields());
                current.getListFields().putAll(idealState.getRecord().getListFields());
              }
              return current;
            }
          }, idealState);
        }
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.info("END PersistAssignmentStage.process() took " + (endTime - startTime) + " ms");
  }

  /**
   * has the preference list changed from the one persisted in current IdealState
   */
  private boolean hasPreferenceListChanged(Map<String, List<String>> newLists,
      IdealState idealState) {
    Map<String, List<String>> existLists = idealState.getPreferenceLists();

    Set<String> partitions = new HashSet<String>(newLists.keySet());
    partitions.addAll(existLists.keySet());

    for (String partition : partitions) {
      List<String> assignedInstances = newLists.get(partition);
      List<String> existingInstances = existLists.get(partition);
      if (assignedInstances == null && existingInstances == null) {
        continue;
      }
      if (assignedInstances == null || existingInstances == null || !assignedInstances
          .equals(existingInstances)) {
        return true;
      }
    }

    return false;
  }

  private boolean hasInstanceMapChanged(Map<Partition, Map<String, String>> newAssiments,
      IdealState idealState) {
    Set<Partition> partitions = new HashSet<Partition>(newAssiments.keySet());
    for (String p : idealState.getPartitionSet()) {
      partitions.add(new Partition(p));
    }

    for (Partition partition : partitions) {
      Map<String, String> instanceMap = newAssiments.get(partition);
      Map<String, String> existInstanceMap =
          idealState.getInstanceStateMap(partition.getPartitionName());
      if (instanceMap == null && existInstanceMap == null) {
        continue;
      }
      if (instanceMap == null || existInstanceMap == null || !instanceMap
          .equals(existInstanceMap)) {
        return true;
      }
    }

    return false;
  }
}
