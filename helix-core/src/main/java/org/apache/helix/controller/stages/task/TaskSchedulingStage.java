package org.apache.helix.controller.stages.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.common.collect.Maps;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskRebalancer;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.task.WorkflowDispatcher;
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskSchedulingStage extends AbstractBaseStage {
  private static final Logger logger = LoggerFactory.getLogger(TaskSchedulingStage.class.getName());
  private Map<String, PriorityQueue<WorkflowObject>> _quotaBasedWorkflowPQs = Maps.newHashMap();
  private WorkflowDispatcher _workflowDispatcher;

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    final Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    WorkflowControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());

    if (currentStateOutput == null || resourceMap == null || cache == null) {
      throw new StageException(
          "Missing attributes in event:" + event + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }

    // Reset current INIT/RUNNING tasks on participants for throttling
    cache.resetActiveTaskCount(currentStateOutput);

    buildQuotaBasedWorkflowPQsAndInitDispatchers(cache,
        (HelixManager) event.getAttribute(AttributeName.helixmanager.name()),
        (ClusterStatusMonitor) event.getAttribute(AttributeName.clusterStatusMonitor.name()));

    final BestPossibleStateOutput bestPossibleStateOutput =
        compute(event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
  }

  private BestPossibleStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    // After compute all workflows and jobs, there are still task resources need to be DROPPED
    Map<String, Resource> restOfResources = new HashMap<>(resourceMap);
    WorkflowControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    BestPossibleStateOutput output = new BestPossibleStateOutput();
    final List<String> failureResources = new ArrayList<>();
    // Queues only for Workflows
    scheduleWorkflows(resourceMap, cache, restOfResources, failureResources, currentStateOutput, output);
    for (String jobName : cache.getTaskDataCache().getDispatchedJobs()) {
      updateResourceMap(jobName, resourceMap, output.getPartitionStateMap(jobName).partitionSet());
      restOfResources.remove(jobName);
    }

    // Current rest of resources including: only current state left over ones
    // Original resource map contains workflows + jobs + other invalid resources
    // After removing workflows + jobs, only leftover ones will go over old rebalance pipeline.
    for (Resource resource : restOfResources.values()) {
      if (!computeResourceBestPossibleState(event, cache, currentStateOutput, resource, output)) {
        failureResources.add(resource.getResourceName());
        LogUtil.logWarn(logger, _eventId,
            "Failed to calculate best possible states for " + resource.getResourceName());
      }
    }

    return output;
  }


  private boolean computeResourceBestPossibleState(ClusterEvent event, WorkflowControllerDataProvider cache,
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
              cache.getPipelineName()));
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
        partitionStateAssignment = mappingCalculator
            .computeBestPossiblePartitionState(cache, idealState, resource, currentStateOutput);
        _workflowDispatcher.updateBestPossibleStateOutput(resource.getResourceName(), partitionStateAssignment, output);

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

  class WorkflowObject implements Comparable<WorkflowObject> {
    String _workflowId;
    Long _rankingValue;

    public WorkflowObject(String workflowId, long rankingValue) {
      _workflowId = workflowId;
      _rankingValue = rankingValue;
    }

    @Override
    public int compareTo(WorkflowObject o) {
      return (int) (_rankingValue - o._rankingValue);
    }
  }

  private void buildQuotaBasedWorkflowPQsAndInitDispatchers(WorkflowControllerDataProvider cache, HelixManager manager,
      ClusterStatusMonitor monitor) {
    _quotaBasedWorkflowPQs.clear();
    Map<String, String> quotaRatioMap = cache.getClusterConfig().getTaskQuotaRatioMap();

    // Create quota based queues
    if (quotaRatioMap == null || quotaRatioMap.size() == 0) {
      _quotaBasedWorkflowPQs
          .put(AssignableInstance.DEFAULT_QUOTA_TYPE, new PriorityQueue<WorkflowObject>());
    } else {
      for (String quotaType : quotaRatioMap.keySet()) {
        _quotaBasedWorkflowPQs.put(quotaType, new PriorityQueue<WorkflowObject>());
      }
    }

    for (String workflowId : cache.getWorkflowConfigMap().keySet()) {
      WorkflowConfig workflowConfig = cache.getWorkflowConfig(workflowId);
      String workflowType = getQuotaType(workflowConfig);
      // TODO: We can support customized sorting field for user. Currently sort by creation time
      _quotaBasedWorkflowPQs.get(workflowType)
          .add(new WorkflowObject(workflowId, workflowConfig.getRecord().getCreationTime()));
    }
    if (_workflowDispatcher == null) {
      _workflowDispatcher = new WorkflowDispatcher();
    }
    _workflowDispatcher.init(manager);
    _workflowDispatcher.setClusterStatusMonitor(monitor);
    _workflowDispatcher.updateCache(cache);
  }

  private void scheduleWorkflows(Map<String, Resource> resourceMap, WorkflowControllerDataProvider cache,
      Map<String, Resource> restOfResources, List<String> failureResources,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleOutput) {
    AssignableInstanceManager assignableInstanceManager = cache.getAssignableInstanceManager();
    for (PriorityQueue<WorkflowObject> quotaBasedWorkflowPQ : _quotaBasedWorkflowPQs.values()) {
      Iterator<WorkflowObject> it = quotaBasedWorkflowPQ.iterator();
      while (it.hasNext()) {
        String workflowId = it.next()._workflowId;
        Resource resource = resourceMap.get(workflowId);
        // TODO : Resource is null could be workflow just created without any IdealState.
        // Let's remove this check when Helix is independent from IdealState
        if (resource != null) {
          try {
            WorkflowContext context = _workflowDispatcher
                .getOrInitializeWorkflowContext(workflowId, cache.getTaskDataCache());
            _workflowDispatcher
                .updateWorkflowStatus(workflowId, cache.getWorkflowConfig(workflowId), context,
                    currentStateOutput, bestPossibleOutput);
            String quotaType = getQuotaType(cache.getWorkflowConfig(workflowId));
            restOfResources.remove(workflowId);
            if (assignableInstanceManager.hasGlobalCapacity(quotaType)) {
              _workflowDispatcher.assignWorkflow(workflowId, cache.getWorkflowConfig(workflowId),
                  context, currentStateOutput, bestPossibleOutput);
            } else {
              LogUtil.logInfo(logger, _eventId, String.format(
                  "Fail to schedule new jobs assignment for Workflow %s due to quota %s is full",
                  workflowId, quotaType));
            }
          } catch (Exception e) {
            LogUtil.logError(logger, _eventId,
                "Error computing assignment for Workflow " + workflowId + ". Skipping.", e);
            failureResources.add(workflowId);
          }
        }
      }
    }
  }

  private void updateResourceMap(String jobName, Map<String, Resource> resourceMap,
      Set<Partition> partitionSet) {
    Resource resource = new Resource(jobName);
    for (Partition partition : partitionSet) {
      resource.addPartition(partition.getPartitionName());
    }
    resource.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    resource.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
    resourceMap.put(jobName, resource);
  }

  private String getQuotaType(WorkflowConfig workflowConfig) {
    String workflowType = workflowConfig.getWorkflowType();
    if (workflowType == null || !_quotaBasedWorkflowPQs.containsKey(workflowType)) {
      workflowType = AssignableInstance.DEFAULT_QUOTA_TYPE;
    }
    return workflowType;
  }
}
