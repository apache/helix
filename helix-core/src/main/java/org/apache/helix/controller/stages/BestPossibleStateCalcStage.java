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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.AutoRebalancer;
import org.apache.helix.controller.rebalancer.CustomRebalancer;
import org.apache.helix.controller.rebalancer.MaintenanceRebalancer;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For partition compute best possible (instance,state) pair based on
 * IdealState,StateModel,LiveInstance
 */
public class BestPossibleStateCalcStage extends AbstractBaseStage {
  private static final Logger logger = LoggerFactory.getLogger(BestPossibleStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    final Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    final ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());

    if (currentStateOutput == null || resourceMap == null || cache == null) {
      throw new StageException(
          "Missing attributes in event:" + event + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }

    final BestPossibleStateOutput bestPossibleStateOutput =
        compute(event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);

    final Map<String, InstanceConfig> instanceConfigMap = cache.getInstanceConfigMap();
    final Map<String, StateModelDefinition> stateModelDefMap = cache.getStateModelDefMap();
    asyncExecute(cache.getAsyncTasksThreadPool(), new Callable<Object>() {
      @Override
      public Object call() {
        try {
          if (clusterStatusMonitor != null) {
            clusterStatusMonitor
                .setPerInstanceResourceStatus(bestPossibleStateOutput, instanceConfigMap,
                    resourceMap, stateModelDefMap);
          }
        } catch (Exception e) {
          LogUtil
              .logError(logger, _eventId, "Could not update cluster status metrics!", e);
        }
        return null;
      }
    });
  }

  private BestPossibleStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    ResourceControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    BestPossibleStateOutput output = new BestPossibleStateOutput();

    HelixManager helixManager = event.getAttribute(AttributeName.helixmanager.name());
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());

    // Check whether the offline/disabled instance count in the cluster reaches the set limit,
    // if yes, pause the rebalancer.
    boolean isValid = validateOfflineInstancesLimit(cache,
        (HelixManager) event.getAttribute(AttributeName.helixmanager.name()));

    final List<String> failureResources = new ArrayList<>();
    Iterator<Resource> itr = resourceMap.values().iterator();
    while (itr.hasNext()) {
      Resource resource = itr.next();
      boolean result = false;
      try {
        result =
            computeResourceBestPossibleState(event, cache, currentStateOutput, resource, output);
      } catch (HelixException ex) {
        LogUtil.logError(logger, _eventId,
            "Exception when calculating best possible states for " + resource.getResourceName(),
            ex);

      }
      if (!result) {
        failureResources.add(resource.getResourceName());
        LogUtil.logWarn(logger, _eventId,
            "Failed to calculate best possible states for " + resource.getResourceName());
      }
    }

    // Check and report if resource rebalance has failure
    updateRebalanceStatus(!isValid || !failureResources.isEmpty(), failureResources, helixManager,
        cache, clusterStatusMonitor,
        "Failed to calculate best possible states for " + failureResources.size() + " resources.");

    return output;
  }

  private void updateRebalanceStatus(final boolean hasFailure, final List<String> failedResources,
      final HelixManager helixManager, final ResourceControllerDataProvider cache,
      final ClusterStatusMonitor clusterStatusMonitor, final String errorMessage) {
    asyncExecute(cache.getAsyncTasksThreadPool(), new Callable<Object>() {
      @Override
      public Object call() {
        try {
          if (hasFailure) {
            /* TODO Enable this update when we resolve ZK server load issue. This will cause extra write to ZK.
            if (_statusUpdateUtil != null) {
              _statusUpdateUtil
                  .logError(StatusUpdateUtil.ErrorType.RebalanceResourceFailure, this.getClass(),
                      errorMessage, helixManager);
            }
            */
            LogUtil.logWarn(logger, _eventId, errorMessage);
          }
          if (clusterStatusMonitor != null) {
            clusterStatusMonitor.setRebalanceFailureGauge(hasFailure);
            clusterStatusMonitor.setResourceRebalanceStates(failedResources,
                ResourceMonitor.RebalanceStatus.BEST_POSSIBLE_STATE_CAL_FAILED);
          }
        } catch (Exception e) {
          LogUtil.logError(logger, _eventId, "Could not update cluster status!", e);
        }
        return null;
      }
    });
  }

  // Check whether the offline/disabled instance count in the cluster reaches the set limit,
  // if yes, pause the rebalancer, and throw exception to terminate rebalance cycle.
  private boolean validateOfflineInstancesLimit(final ResourceControllerDataProvider cache,
      final HelixManager manager) {
    int maxOfflineInstancesAllowed = cache.getClusterConfig().getMaxOfflineInstancesAllowed();
    if (maxOfflineInstancesAllowed >= 0) {
      int offlineCount = cache.getAllInstances().size() - cache.getEnabledLiveInstances().size();
      if (offlineCount > maxOfflineInstancesAllowed) {
        String errMsg = String.format(
            "Offline Instances count %d greater than allowed count %d. Stop rebalance and put the cluster %s into maintenance mode.",
            offlineCount, maxOfflineInstancesAllowed, cache.getClusterName());
        if (manager != null) {
          if (manager.getHelixDataAccessor()
              .getProperty(manager.getHelixDataAccessor().keyBuilder().maintenance()) == null) {
            manager.getClusterManagmentTool().autoEnableMaintenanceMode(manager.getClusterName(),
                true, errMsg, MaintenanceSignal.AutoTriggerReason.MAX_OFFLINE_INSTANCES_EXCEEDED);
            LogUtil.logWarn(logger, _eventId, errMsg);
          }
        } else {
          LogUtil.logError(logger, _eventId, "Failed to put cluster " + cache.getClusterName()
              + " into maintenance mode, HelixManager is not set!");
        }
        return false;
      }
    }
    return true;
  }

  private boolean computeResourceBestPossibleState(ClusterEvent event, ResourceControllerDataProvider cache,
      CurrentStateOutput currentStateOutput, Resource resource, BestPossibleStateOutput output) {
    // for each ideal state
    // read the state model def
    // for each resource
    // get the preference list
    // for each instanceName check if its alive then assign a state

    String resourceName = resource.getResourceName();
    LogUtil.logDebug(logger, _eventId, "Processing resource:" + resourceName);
    // Ideal state may be gone. In that case we need to get the state model name
    // from the current state
    IdealState idealState = cache.getIdealState(resourceName);
    if (idealState == null) {
      // if ideal state is deleted, use an empty one
      LogUtil.logInfo(logger, _eventId, "resource:" + resourceName + " does not exist anymore");
      idealState = new IdealState(resourceName);
      idealState.setStateModelDefRef(resource.getStateModelDefRef());
    }

    // Skip resources are tasks for regular pipeline
    if (idealState.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)) {
      LogUtil.logWarn(logger, _eventId, String
          .format("Resource %s should not be processed by %s pipeline", resourceName,
              cache.getPipelineName()));
      return false;
    }

    Rebalancer<ResourceControllerDataProvider> rebalancer =
        getRebalancer(idealState, resourceName, cache.isMaintenanceModeEnabled());
    MappingCalculator<ResourceControllerDataProvider> mappingCalculator = getMappingCalculator(rebalancer, resourceName);

    if (rebalancer == null || mappingCalculator == null) {
      LogUtil.logError(logger, _eventId,
          "Error computing assignment for resource " + resourceName + ". no rebalancer found. rebalancer: " + rebalancer
              + " mappingCalculator: " + mappingCalculator);
    }

    if (rebalancer != null && mappingCalculator != null) {
      ResourceAssignment partitionStateAssignment = null;
      try {
        HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
        rebalancer.init(manager);
        idealState =
            rebalancer.computeNewIdealState(resourceName, idealState, currentStateOutput, cache);

        output.setPreferenceLists(resourceName, idealState.getPreferenceLists());

        // Use the internal MappingCalculator interface to compute the final assignment
        // The next release will support rebalancers that compute the mapping from start to finish
        partitionStateAssignment = mappingCalculator
            .computeBestPossiblePartitionState(cache, idealState, resource, currentStateOutput);
        for (Partition partition : resource.getPartitions()) {
          Map<String, String> newStateMap = partitionStateAssignment.getReplicaMap(partition);
          output.setState(resourceName, partition, newStateMap);
        }

        // Check if calculation is done successfully
        return checkBestPossibleStateCalculation(idealState);
      } catch (HelixException e) {
        // No eligible instance is found.
        LogUtil.logError(logger, _eventId, e.getMessage());
      } catch (Exception e) {
        LogUtil.logError(logger, _eventId,
            "Error computing assignment for resource " + resourceName + ". Skipping.");
        // TODO : remove this part after debugging NPE
        StringBuilder sb = new StringBuilder();

        sb.append(String
            .format("HelixManager is null : %s\n", event.getAttribute("helixmanager") == null));
        sb.append(String.format("Rebalancer is null : %s\n", rebalancer == null));
        sb.append(String.format("Calculated idealState is null : %s\n", idealState == null));
        sb.append(String.format("MappingCaculator is null : %s\n", mappingCalculator == null));
        sb.append(
            String.format("PartitionAssignment is null : %s\n", partitionStateAssignment == null));
        sb.append(String.format("Output is null : %s\n", output == null));

        LogUtil.logError(logger, _eventId, sb.toString());
      }
    }
    // Exception or rebalancer is not found
    return false;
  }

  private boolean checkBestPossibleStateCalculation(IdealState idealState) {
    // If replicas is 0, indicate the resource is not fully initialized or ready to be rebalanced
    if (idealState.getRebalanceMode() == IdealState.RebalanceMode.FULL_AUTO && !idealState
        .getReplicas().equals("0")) {
      Map<String, List<String>> preferenceLists = idealState.getPreferenceLists();
      if (preferenceLists == null || preferenceLists.isEmpty()) {
        return false;
      }
      int emptyListCount = 0;
      for (List<String> preferenceList : preferenceLists.values()) {
        if (preferenceList.isEmpty()) {
          emptyListCount++;
        }
      }
      // If all lists are empty, rebalance fails completely
      return emptyListCount != preferenceLists.values().size();
    } else {
      // For non FULL_AUTO RebalanceMode, rebalancing is not controlled by Helix
      return true;
    }
  }

  private Rebalancer<ResourceControllerDataProvider> getRebalancer(IdealState idealState, String resourceName,
      boolean isMaintenanceModeEnabled) {
    Rebalancer<ResourceControllerDataProvider> customizedRebalancer = null;
    String rebalancerClassName = idealState.getRebalancerClassName();
    if (rebalancerClassName != null) {
      if (logger.isDebugEnabled()) {
        LogUtil.logDebug(logger, _eventId,
            "resource " + resourceName + " use idealStateRebalancer " + rebalancerClassName);
      }
      try {
        customizedRebalancer = Rebalancer.class
            .cast(HelixUtil.loadClass(getClass(), rebalancerClassName).newInstance());
      } catch (Exception e) {
        LogUtil.logError(logger, _eventId,
            "Exception while invoking custom rebalancer class:" + rebalancerClassName, e);
      }
    }

    Rebalancer<ResourceControllerDataProvider> rebalancer = null;
    switch (idealState.getRebalanceMode()) {
    case FULL_AUTO:
      if (isMaintenanceModeEnabled) {
        rebalancer = new MaintenanceRebalancer();
      } else {
        if (customizedRebalancer != null) {
          rebalancer = customizedRebalancer;
        } else {
          rebalancer = new AutoRebalancer();
        }
      }
      break;
    case SEMI_AUTO:
      rebalancer = new SemiAutoRebalancer<>();
      break;
    case CUSTOMIZED:
      rebalancer = new CustomRebalancer();
      break;
    case USER_DEFINED:
    case TASK:
      rebalancer = customizedRebalancer;
      break;
    default:
      LogUtil.logError(logger, _eventId,
          "Fail to find the rebalancer, invalid rebalance mode " + idealState.getRebalanceMode());
      break;
    }

    return rebalancer;
  }

  private MappingCalculator<ResourceControllerDataProvider> getMappingCalculator(
      Rebalancer<ResourceControllerDataProvider> rebalancer, String resourceName) {
    MappingCalculator<ResourceControllerDataProvider> mappingCalculator = null;

    if (rebalancer != null) {
      try {
        mappingCalculator = MappingCalculator.class.cast(rebalancer);
      } catch (ClassCastException e) {
        LogUtil.logWarn(logger, _eventId,
            "Rebalancer does not have a mapping calculator, defaulting to SEMI_AUTO, resource: "
                + resourceName);
      }
    }
    if (mappingCalculator == null) {
      mappingCalculator = new SemiAutoRebalancer<>();
    }

    return mappingCalculator;
  }
}
