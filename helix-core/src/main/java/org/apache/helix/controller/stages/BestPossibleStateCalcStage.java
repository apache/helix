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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.PriorityQueue;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.AutoRebalancer;
import org.apache.helix.controller.rebalancer.CustomRebalancer;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobRebalancer;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskRebalancer;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

/**
 * For partition compute best possible (instance,state) pair based on
 * IdealState,StateModel,LiveInstance
 */
public class BestPossibleStateCalcStage extends AbstractBaseStage {
  private static final Logger logger = Logger.getLogger(BestPossibleStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.info("START BestPossibleStateCalcStage.process()");

    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (currentStateOutput == null || resourceMap == null || cache == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }

    // Reset current INIT/RUNNING tasks on participants for throttling
    cache.resetActiveTaskCount(currentStateOutput);

    BestPossibleStateOutput bestPossibleStateOutput =
        compute(event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);

    try {
      ClusterStatusMonitor clusterStatusMonitor = event.getAttribute("clusterStatusMonitor");
      if (clusterStatusMonitor != null) {
        clusterStatusMonitor
            .setPerInstanceResourceStatus(bestPossibleStateOutput, cache.getInstanceConfigMap(),
                resourceMap, cache.getStateModelDefMap());
      }
    } catch (Exception e) {
      logger.error("Could not update cluster status metrics!", e);
    }

    long endTime = System.currentTimeMillis();
    logger.info("END BestPossibleStateCalcStage.process() for cluster " + cache.getClusterName()
        + ". took: " + (endTime - startTime) + " ms");
  }

  private BestPossibleStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    BestPossibleStateOutput output = new BestPossibleStateOutput();

    PriorityQueue<ResourcePriority> resourcePriorityQueue = new PriorityQueue<ResourcePriority>();
    TaskDriver taskDriver = null;
    HelixManager helixManager = event.getAttribute("helixmanager");
    if (helixManager != null) {
      taskDriver = new TaskDriver(helixManager);
    }
    for (Resource resource : resourceMap.values()) {
      resourcePriorityQueue.add(new ResourcePriority(resource, cache.getIdealState(resource.getResourceName()),
          taskDriver));
    }

    Iterator<ResourcePriority> itr = resourcePriorityQueue.iterator();
    while (itr.hasNext()) {
      computeResourceBestPossibleState(event, cache, currentStateOutput, itr.next().getResource(), output);
    }

    return output;
  }

  private void computeResourceBestPossibleState(ClusterEvent event, ClusterDataCache cache,
      CurrentStateOutput currentStateOutput, Resource resource, BestPossibleStateOutput output) {
    // for each ideal state
    // read the state model def
    // for each resource
    // get the preference list
    // for each instanceName check if its alive then assign a state

    String resourceName = resource.getResourceName();
    logger.debug("Processing resource:" + resourceName);
    // Ideal state may be gone. In that case we need to get the state model name
    // from the current state
    IdealState idealState = cache.getIdealState(resourceName);
    if (idealState == null) {
      // if ideal state is deleted, use an empty one
      logger.info("resource:" + resourceName + " does not exist anymore");
      idealState = new IdealState(resourceName);
      idealState.setStateModelDefRef(resource.getStateModelDefRef());
    }

    Rebalancer rebalancer = getRebalancer(idealState, resourceName);
    MappingCalculator mappingCalculator = getMappingCalculator(rebalancer, resourceName);

    if (rebalancer == null || mappingCalculator == null) {
      logger.error(
          "Error computing assignment for resource " + resourceName + ". no rebalancer found. rebalancer: " + rebalancer
              + " mappingCaculator: " + mappingCalculator);
    }

    if (rebalancer != null && mappingCalculator != null) {
      if (rebalancer instanceof TaskRebalancer) {
        TaskRebalancer taskRebalancer = TaskRebalancer.class.cast(rebalancer);
        taskRebalancer.setClusterStatusMonitor((ClusterStatusMonitor) event.getAttribute("clusterStatusMonitor"));
      }

      try {
        HelixManager manager = event.getAttribute("helixmanager");
        rebalancer.init(manager);
        idealState = rebalancer.computeNewIdealState(resourceName, idealState, currentStateOutput, cache);

        output.setPreferenceLists(resourceName, idealState.getPreferenceLists());

        // Use the internal MappingCalculator interface to compute the final assignment
        // The next release will support rebalancers that compute the mapping from start to finish
        ResourceAssignment partitionStateAssignment =
            mappingCalculator.computeBestPossiblePartitionState(cache, idealState, resource, currentStateOutput);
        for (Partition partition : resource.getPartitions()) {
          Map<String, String> newStateMap = partitionStateAssignment.getReplicaMap(partition);
          output.setState(resourceName, partition, newStateMap);
        }
      } catch (Exception e) {
        logger.error("Error computing assignment for resource " + resourceName + ". Skipping.", e);
      }
    }
  }

  private Rebalancer getRebalancer(IdealState idealState, String resourceName) {

    Rebalancer customizedRebalancer = null;
    String rebalancerClassName = idealState.getRebalancerClassName();
    if (rebalancerClassName != null) {
      logger.debug("resource " + resourceName + " use idealStateRebalancer " + rebalancerClassName);
      try {
        customizedRebalancer = Rebalancer.class
            .cast(HelixUtil.loadClass(getClass(), rebalancerClassName).newInstance());
      } catch (Exception e) {
        logger.error("Exception while invoking custom rebalancer class:" + rebalancerClassName, e);
      }
    }

    Rebalancer rebalancer = null;
    switch (idealState.getRebalanceMode()) {
    case FULL_AUTO:
      if (customizedRebalancer != null) {
        rebalancer = customizedRebalancer;
      } else {
        rebalancer = new AutoRebalancer();
      }
      break;
    case SEMI_AUTO:
      rebalancer = new SemiAutoRebalancer();
      break;
    case CUSTOMIZED:
      rebalancer = new CustomRebalancer();
      break;
    case USER_DEFINED:
    case TASK:
      rebalancer = customizedRebalancer;
      break;
    default:
      logger.error(
          "Fail to find the rebalancer, invalid rebalance mode " + idealState.getRebalanceMode());
      break;
    }

    return rebalancer;
  }

  private MappingCalculator getMappingCalculator(Rebalancer rebalancer, String resourceName) {
    MappingCalculator mappingCalculator = null;

    if (rebalancer != null) {
      try {
        mappingCalculator = MappingCalculator.class.cast(rebalancer);
      } catch (ClassCastException e) {
        logger.warn(
            "Rebalancer does not have a mapping calculator, defaulting to SEMI_AUTO, resource: "
                + resourceName);
      }
    }
    if (mappingCalculator == null) {
      mappingCalculator = new SemiAutoRebalancer();
    }

    return mappingCalculator;
  }

  class ResourcePriority implements Comparable<ResourcePriority> {
    final Resource _resource;
    // By default, non-job resources and new jobs are assigned lowest priority
    Long _priority = Long.MAX_VALUE;

    Resource getResource() {
      return _resource;
    }

    public ResourcePriority(Resource resource, IdealState idealState, TaskDriver taskDriver) {
      _resource = resource;

      if (taskDriver != null && idealState != null
          && idealState.getRebalancerClassName() != null
          && idealState.getRebalancerClassName().equals(JobRebalancer.class.getName())) {
        // Update priority for job resources, note that older jobs will be processed earlier
        JobContext jobContext = taskDriver.getJobContext(resource.getResourceName());
        if (jobContext != null && jobContext.getStartTime() != WorkflowContext.UNSTARTED) {
          _priority = jobContext.getStartTime();
        }
      }
    }

    @Override
    public int compareTo(ResourcePriority otherJob) {
      return _priority.compareTo(otherJob._priority);
    }
  }
}
