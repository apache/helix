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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
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
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For partition compute best possible (instance,state) pair based on
 * IdealState,StateModel,LiveInstance
 */
public class BestPossibleStateCalcStage extends AbstractBaseStage {
  private static final Logger logger =
      LoggerFactory.getLogger(BestPossibleStateCalcStage.class.getName());

  // Lazy initialize the WAGED rebalancer instance since the BestPossibleStateCalcStage instance was
  // instantiated without the HelixManager information that is required.
  // TODO: Initialize the WAGED rebalancer in the BestPossibleStateCalcStage constructor once it is
  // TODO: updated so as to accept a HelixManager or HelixZkClient information.
  private WagedRebalancer _wagedRebalancer = null;

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE.name());
    final Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    final ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

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
          LogUtil.logError(logger, _eventId, "Could not update cluster status metrics!", e);
        }
        return null;
      }
    });
  }

  // Need to keep a default constructor for backward compatibility
  public BestPossibleStateCalcStage() {
  }

  // Construct the BestPossibleStateCalcStage with a given WAGED rebalancer for the callers other
  // than the controller pipeline. Such as the verifiers and test cases.
  public BestPossibleStateCalcStage(WagedRebalancer wagedRebalancer) {
    _wagedRebalancer = wagedRebalancer;
  }

  private WagedRebalancer getWagedRebalancer(HelixManager helixManager,
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences) {
    // Create WagedRebalancer instance if it hasn't been already initialized
    if (_wagedRebalancer == null) {
      _wagedRebalancer = new WagedRebalancer(helixManager, preferences);
    } else {
      // Since the preference can be updated at runtime, try to update the algorithm preference
      // before returning the rebalancer.
      _wagedRebalancer.updatePreference(preferences);
    }
    return _wagedRebalancer;
  }

  private BestPossibleStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    BestPossibleStateOutput output = new BestPossibleStateOutput();

    HelixManager helixManager = event.getAttribute(AttributeName.helixmanager.name());
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());

    // Check whether the offline/disabled instance count in the cluster reaches the set limit,
    // if yes, pause the rebalancer.
    boolean isValid =
        validateOfflineInstancesLimit(cache, event.getAttribute(AttributeName.helixmanager.name()));

    final List<String> failureResources = new ArrayList<>();

    Map<String, Resource> calculatedResourceMap =
        computeResourceBestPossibleStateWithWagedRebalancer(cache, currentStateOutput, helixManager,
            resourceMap, output, failureResources);

    Map<String, Resource> remainingResourceMap = new HashMap<>(resourceMap);
    remainingResourceMap.keySet().removeAll(calculatedResourceMap.keySet());

    // Fallback to the original single resource rebalancer calculation.
    // This is required because we support mixed cluster that uses both WAGED rebalancer and the
    // older rebalancers.
    Iterator<Resource> itr = remainingResourceMap.values().iterator();
    while (itr.hasNext()) {
      Resource resource = itr.next();
      boolean result = false;
      try {
        result = computeSingleResourceBestPossibleState(event, cache, currentStateOutput, resource,
            output);
      } catch (HelixException ex) {
        LogUtil.logError(logger, _eventId, String
            .format("Exception when calculating best possible states for %s",
                resource.getResourceName()), ex);

      }
      if (!result) {
        failureResources.add(resource.getResourceName());
        LogUtil.logWarn(logger, _eventId, String
            .format("Failed to calculate best possible states for %s", resource.getResourceName()));
      }
    }

    // Check and report if resource rebalance has failure
    updateRebalanceStatus(!isValid || !failureResources.isEmpty(), failureResources, helixManager,
        cache, clusterStatusMonitor, String
            .format("Failed to calculate best possible states for %d resources.",
                failureResources.size()));

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
            manager.getClusterManagmentTool()
                .autoEnableMaintenanceMode(manager.getClusterName(), true, errMsg,
                    MaintenanceSignal.AutoTriggerReason.MAX_OFFLINE_INSTANCES_EXCEEDED);
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

  /**
   * Rebalance with the WAGED rebalancer
   * The rebalancer only calculates the new ideal assignment for all the resources that are
   * configured to use the WAGED rebalancer.
   *
   * @param cache              Cluster data cache.
   * @param currentStateOutput The current state information.
   * @param helixManager
   * @param resourceMap        The complete resource map. The method will filter the map for the compatible resources.
   * @param output             The best possible state output.
   * @param failureResources   The failure records that will be updated if any resource cannot be computed.
   * @return The map of all the calculated resources.
   */
  private Map<String, Resource> computeResourceBestPossibleStateWithWagedRebalancer(
      ResourceControllerDataProvider cache, CurrentStateOutput currentStateOutput,
      HelixManager helixManager, Map<String, Resource> resourceMap, BestPossibleStateOutput output,
      List<String> failureResources) {
    if (cache.isMaintenanceModeEnabled()) {
      // The WAGED rebalancer won't be used while maintenance mode is enabled.
      return Collections.emptyMap();
    }

    // Find the compatible resources: 1. FULL_AUTO 2. Configured to use the WAGED rebalancer
    Map<String, Resource> wagedRebalancedResourceMap =
        resourceMap.entrySet().stream().filter(resourceEntry -> {
          IdealState is = cache.getIdealState(resourceEntry.getKey());
          return is != null && is.getRebalanceMode().equals(IdealState.RebalanceMode.FULL_AUTO)
              && WagedRebalancer.class.getName().equals(is.getRebalancerClassName());
        }).collect(Collectors.toMap(resourceEntry -> resourceEntry.getKey(),
            resourceEntry -> resourceEntry.getValue()));

    Map<String, IdealState> newIdealStates = new HashMap<>();

    WagedRebalancer wagedRebalancer =
        getWagedRebalancer(helixManager, cache.getClusterConfig().getGlobalRebalancePreference());
    try {
      newIdealStates.putAll(wagedRebalancer.computeNewIdealStates(cache, wagedRebalancedResourceMap,
          currentStateOutput));
    } catch (HelixRebalanceException ex) {
      // Note that unlike the legacy rebalancer, the WAGED rebalance won't return partial result.
      // Since it calculates for all the eligible resources globally, a partial result is invalid.
      // TODO propagate the rebalancer failure information to updateRebalanceStatus for monitoring.
      LogUtil.logError(logger, _eventId,
          String.format(
              "Failed to calculate the new Ideal States using the rebalancer %s due to %s",
              wagedRebalancer.getClass().getSimpleName(), ex.getFailureType()),
          ex);
    }

    Iterator<Resource> itr = wagedRebalancedResourceMap.values().iterator();
    while (itr.hasNext()) {
      Resource resource = itr.next();
      IdealState is = newIdealStates.get(resource.getResourceName());
      // Check if the WAGED rebalancer has calculated the result for this resource or not.
      if (is != null && checkBestPossibleStateCalculation(is)) {
        // The WAGED rebalancer calculates a valid result, record in the output
        updateBestPossibleStateOutput(output, resource, is);
      } else {
        failureResources.add(resource.getResourceName());
        LogUtil.logWarn(logger, _eventId, String
            .format("Failed to calculate best possible states for %s.",
                resource.getResourceName()));
      }
    }
    return wagedRebalancedResourceMap;
  }

  private void updateBestPossibleStateOutput(BestPossibleStateOutput output, Resource resource,
      IdealState computedIdealState) {
    output.setPreferenceLists(resource.getResourceName(), computedIdealState.getPreferenceLists());
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> newStateMap =
          computedIdealState.getInstanceStateMap(partition.getPartitionName());
      output.setState(resource.getResourceName(), partition, newStateMap);
    }
  }

  private boolean computeSingleResourceBestPossibleState(ClusterEvent event,
      ResourceControllerDataProvider cache, CurrentStateOutput currentStateOutput,
      Resource resource, BestPossibleStateOutput output) {
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
    MappingCalculator<ResourceControllerDataProvider> mappingCalculator =
        getMappingCalculator(rebalancer, resourceName);

    if (rebalancer == null || mappingCalculator == null) {
      LogUtil.logError(logger, _eventId, "Error computing assignment for resource " + resourceName
          + ". no rebalancer found. rebalancer: " + rebalancer + " mappingCalculator: "
          + mappingCalculator);
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

        if (partitionStateAssignment == null) {
          LogUtil.logWarn(logger, _eventId,
              "PartitionStateAssignment is null, resource: " + resourceName);
          return false;
        }

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
            "Error computing assignment for resource " + resourceName + ". Skipping." , e);
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

  private Rebalancer<ResourceControllerDataProvider> getCustomizedRebalancer(
      String rebalancerClassName, String resourceName) {
    Rebalancer<ResourceControllerDataProvider> customizedRebalancer = null;
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
    return customizedRebalancer;
  }

  private Rebalancer<ResourceControllerDataProvider> getRebalancer(IdealState idealState,
      String resourceName, boolean isMaintenanceModeEnabled) {
    Rebalancer<ResourceControllerDataProvider> rebalancer = null;
    switch (idealState.getRebalanceMode()) {
    case FULL_AUTO:
      if (isMaintenanceModeEnabled) {
        rebalancer = new MaintenanceRebalancer();
      } else {
        Rebalancer<ResourceControllerDataProvider> customizedRebalancer =
            getCustomizedRebalancer(idealState.getRebalancerClassName(), resourceName);
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
      rebalancer = getCustomizedRebalancer(idealState.getRebalancerClassName(), resourceName);
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
