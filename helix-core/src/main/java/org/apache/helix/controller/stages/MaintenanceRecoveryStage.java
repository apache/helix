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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceRecoveryStage extends AbstractAsyncBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(MaintenanceRecoveryStage.class);

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.MaintenanceRecoveryWorker;
  }

  @Override
  public void execute(final ClusterEvent event) throws Exception {
    // Check the cache is there
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    if (cache == null) {
      return;
    }

    // Check for the maintenance signal
    // If it was entered manually or the signal is null (which shouldn't happen), skip this stage
    MaintenanceSignal maintenanceSignal = cache.getMaintenanceSignal();
    if (maintenanceSignal == null || maintenanceSignal
        .getTriggeringEntity() != MaintenanceSignal.TriggeringEntity.CONTROLLER) {
      return;
    }

    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null || !manager.isConnected()) {
      LogUtil.logInfo(LOG, _eventId,
          "MaintenanceRecoveryStage failed due to HelixManager being null or not connected!");
      return;
    }

    // At this point, the cluster entered maintenance mode automatically. Retrieve the
    // auto-triggering reason
    MaintenanceSignal.AutoTriggerReason internalReason = maintenanceSignal.getAutoTriggerReason();
    boolean shouldExitMaintenance;
    String reason;
    switch (internalReason) {
    case MAX_OFFLINE_INSTANCES_EXCEEDED:
      // Check on the number of offline/disabled instances
      int numOfflineInstancesForAutoExit =
          cache.getClusterConfig().getNumOfflineInstancesForAutoExit();
      if (numOfflineInstancesForAutoExit < 0) {
        return; // Config is not set, no auto-exit
      }
      // Get the count of all instances that are either offline or disabled
      int offlineDisabledCount =
          cache.getAllInstances().size() - cache.getEnabledLiveInstances().size();
      shouldExitMaintenance = offlineDisabledCount <= numOfflineInstancesForAutoExit;
      reason = String.format(
          "Auto-exiting maintenance mode for cluster %s; Num. of offline/disabled instances is %d, less than or equal to the exit threshold %d",
          event.getClusterName(), offlineDisabledCount, numOfflineInstancesForAutoExit);
      break;
    case MAX_PARTITION_PER_INSTANCE_EXCEEDED:
      IntermediateStateOutput intermediateStateOutput =
          event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
      if (intermediateStateOutput == null) {
        return;
      }
      shouldExitMaintenance = !violatesMaxPartitionsPerInstance(cache, intermediateStateOutput);
      reason = String.format(
          "Auto-exiting maintenance mode for cluster %s; All instances have fewer or equal number of partitions than maxPartitionsPerInstance threshold.",
          event.getClusterName());
      break;
    default:
      shouldExitMaintenance = false;
      reason = "";
    }
    if (shouldExitMaintenance) {
      // The cluster has recovered sufficiently, so proceed to exit the maintenance mode by removing
      // MaintenanceSignal. AutoTriggerReason won't be recorded
      manager.getClusterManagmentTool().autoEnableMaintenanceMode(manager.getClusterName(), false,
          reason, internalReason);
      LogUtil.logInfo(LOG, _eventId, reason);
    }
  }

  /**
   * Check that the intermediateStateOutput assignment does not violate maxPartitionPerInstance
   * threshold.
   * @param cache
   * @param intermediateStateOutput
   * @return true if violation is found, false otherwise.
   */
  private boolean violatesMaxPartitionsPerInstance(ResourceControllerDataProvider cache,
      IntermediateStateOutput intermediateStateOutput) {
    int maxPartitionPerInstance = cache.getClusterConfig().getMaxPartitionsPerInstance();
    if (maxPartitionPerInstance <= 0) {
      // Config is not set; return
      return false;
    }
    Map<String, PartitionStateMap> resourceStatesMap =
        intermediateStateOutput.getResourceStatesMap();
    Map<String, Integer> instancePartitionCounts = new HashMap<>();

    for (String resource : resourceStatesMap.keySet()) {
      IdealState idealState = cache.getIdealState(resource);
      if (idealState != null
          && idealState.getStateModelDefRef().equals(BuiltInStateModelDefinitions.Task.name())) {
        // Ignore task here. Task has its own throttling logic
        continue;
      }

      PartitionStateMap partitionStateMap = resourceStatesMap.get(resource);
      Map<Partition, Map<String, String>> stateMaps = partitionStateMap.getStateMap();
      for (Partition p : stateMaps.keySet()) {
        Map<String, String> stateMap = stateMaps.get(p);
        for (String instance : stateMap.keySet()) {
          // If this replica is in DROPPED state, do not count it in the partition count since it is
          // to be dropped
          String state = stateMap.get(instance);
          if (state.equals(HelixDefinedState.DROPPED.name())) {
            continue;
          }
          if (!instancePartitionCounts.containsKey(instance)) {
            instancePartitionCounts.put(instance, 0);
          }
          // Number of replicas (from different partitions) held in this instance
          int partitionCount = instancePartitionCounts.get(instance);
          partitionCount++;
          if (partitionCount > maxPartitionPerInstance) {
            // There exists an instance whose intermediate state assignment violates the maximum
            // partitions per instance threshold, return!
            return true;
          }
          instancePartitionCounts.put(instance, partitionCount);
        }
      }
    }
    // No violation found
    return false;
  }
}
