package org.apache.helix.controller.stages.task;

import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobRebalancer;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskRebalancer;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskSchedulingStage extends AbstractBaseStage {
  private static final Logger logger = LoggerFactory.getLogger(TaskSchedulingStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    final Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());

    if (currentStateOutput == null || resourceMap == null || cache == null) {
      throw new StageException(
          "Missing attributes in event:" + event + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }

    // Reset current INIT/RUNNING tasks on participants for throttling
    cache.resetActiveTaskCount(currentStateOutput);

    final BestPossibleStateOutput bestPossibleStateOutput =
        compute(event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);

  }

  private BestPossibleStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    BestPossibleStateOutput output = new BestPossibleStateOutput();

    PriorityQueue<TaskSchedulingStage.ResourcePriority> resourcePriorityQueue =
        new PriorityQueue<>();
    TaskDriver taskDriver = null;
    HelixManager helixManager = event.getAttribute(AttributeName.helixmanager.name());
    if (helixManager != null) {
      taskDriver = new TaskDriver(helixManager);
    }
    for (Resource resource : resourceMap.values()) {
      resourcePriorityQueue.add(new TaskSchedulingStage.ResourcePriority(resource,
          cache.getIdealState(resource.getResourceName()), taskDriver));
    }

    // TODO: Replace this looping available resources with Workflow Queues
    for (Iterator<TaskSchedulingStage.ResourcePriority> itr = resourcePriorityQueue.iterator();
        itr.hasNext(); ) {
      Resource resource = itr.next().getResource();
      if (!computeResourceBestPossibleState(event, cache, currentStateOutput, resource, output)) {
        LogUtil
            .logWarn(logger, _eventId, "Failed to assign tasks for " + resource.getResourceName());
      }
    }

    return output;
  }


  private boolean computeResourceBestPossibleState(ClusterEvent event, ClusterDataCache cache,
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

    // Skip the resources are not belonging to task pipeline
    if (!idealState.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)) {
      LogUtil.logWarn(logger, _eventId, String
          .format("Resource %s should not be processed by %s pipeline", resourceName,
              cache.isTaskCache() ? "TASK" : "DEFAULT"));
      return false;
    }

    Rebalancer rebalancer = null;
    String rebalancerClassName = idealState.getRebalancerClassName();
    if (rebalancerClassName != null) {
      if (logger.isDebugEnabled()) {
        LogUtil.logDebug(logger, _eventId,
            "resource " + resourceName + " use idealStateRebalancer " + rebalancerClassName);
      }
      try {
        rebalancer = Rebalancer.class
            .cast(HelixUtil.loadClass(getClass(), rebalancerClassName).newInstance());
      } catch (Exception e) {
        LogUtil.logError(logger, _eventId,
            "Exception while invoking custom rebalancer class:" + rebalancerClassName, e);
      }
    }

    MappingCalculator mappingCalculator = null;
    if (rebalancer != null) {
      try {
        mappingCalculator = MappingCalculator.class.cast(rebalancer);
      } catch (ClassCastException e) {
        LogUtil.logWarn(logger, _eventId,
            "Rebalancer does not have a mapping calculator, defaulting to SEMI_AUTO, resource: "
                + resourceName);
      }
    } else {
      // Create dummy rebalancer for dropping existing current states
      rebalancer = new SemiAutoRebalancer();
      mappingCalculator = new SemiAutoRebalancer();
    }

    if (rebalancer instanceof TaskRebalancer) {
      TaskRebalancer taskRebalancer = TaskRebalancer.class.cast(rebalancer);
      taskRebalancer.setClusterStatusMonitor(
          (ClusterStatusMonitor) event.getAttribute(AttributeName.clusterStatusMonitor.name()));
    }
    ResourceAssignment partitionStateAssignment = null;
    try {
      HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
      rebalancer.init(manager);

      // Use the internal MappingCalculator interface to compute the final assignment
      // The next release will support rebalancers that compute the mapping from start to finish
        partitionStateAssignment = mappingCalculator
            .computeBestPossiblePartitionState(cache, idealState, resource, currentStateOutput);
        for (Partition partition : resource.getPartitions()) {
          Map<String, String> newStateMap = partitionStateAssignment.getReplicaMap(partition);
          output.setState(resourceName, partition, newStateMap);
        }

        // Check if calculation is done successfully
        return true;
      } catch (Exception e) {
        LogUtil
            .logError(logger, _eventId, "Error computing assignment for resource " + resourceName + ". Skipping.", e);
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

    // Exception or rebalancer is not found
    return false;
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
