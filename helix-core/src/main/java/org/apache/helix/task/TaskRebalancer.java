/*
 * $Id$
 */
package org.apache.helix.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * Custom rebalancer implementation for the {@code Task} state model.
 */
public class TaskRebalancer implements Rebalancer, MappingCalculator {
  private static final Logger LOG = Logger.getLogger(TaskRebalancer.class);
  private HelixManager _manager;

  @Override
  public void init(HelixManager manager) {
    _manager = manager;
  }

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache clusterData,
      IdealState taskIs, Resource resource, CurrentStateOutput currStateOutput) {
    final String resourceName = resource.getResourceName();

    // Fetch task configuration
    TaskConfig taskCfg = TaskUtil.getTaskCfg(_manager, resourceName);
    String workflowResource = taskCfg.getWorkflow();

    // Fetch workflow configuration and context
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowCfg(_manager, workflowResource);
    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_manager, workflowResource);

    // Initialize workflow context if needed
    if (workflowCtx == null) {
      workflowCtx = new WorkflowContext(new ZNRecord("WorkflowContext"));
      workflowCtx.setStartTime(System.currentTimeMillis());
    }

    // Check parent dependencies
    for (String parent : workflowCfg.getTaskDag().getDirectParents(resourceName)) {
      if (workflowCtx.getTaskState(parent) == null
          || !workflowCtx.getTaskState(parent).equals(TaskState.COMPLETED)) {
        return emptyAssignment(resourceName);
      }
    }

    // Clean up if workflow marked for deletion
    TargetState targetState = workflowCfg.getTargetState();
    if (targetState == TargetState.DELETE) {
      cleanup(_manager, resourceName, workflowCfg, workflowResource);
      return emptyAssignment(resourceName);
    }

    // Check if this workflow has been finished past its expiry.
    if (workflowCtx.getFinishTime() != WorkflowContext.UNFINISHED
        && workflowCtx.getFinishTime() + workflowCfg.getExpiry() <= System.currentTimeMillis()) {
      markForDeletion(_manager, workflowResource);
      cleanup(_manager, resourceName, workflowCfg, workflowResource);
      return emptyAssignment(resourceName);
    }

    // Fetch any existing context information from the property store.
    TaskContext taskCtx = TaskUtil.getTaskContext(_manager, resourceName);
    if (taskCtx == null) {
      taskCtx = new TaskContext(new ZNRecord("TaskContext"));
      taskCtx.setStartTime(System.currentTimeMillis());
    }

    // The task is already in a final state (completed/failed).
    if (workflowCtx.getTaskState(resourceName) == TaskState.FAILED
        || workflowCtx.getTaskState(resourceName) == TaskState.COMPLETED) {
      return emptyAssignment(resourceName);
    }

    ResourceAssignment prevAssignment = TaskUtil.getPrevResourceAssignment(_manager, resourceName);
    if (prevAssignment == null) {
      prevAssignment = new ResourceAssignment(resourceName);
    }

    // Will contain the list of partitions that must be explicitly dropped from the ideal state that
    // is stored in zk.
    // Fetch the previous resource assignment from the property store. This is required because of
    // HELIX-230.
    Set<Integer> partitionsToDrop = new TreeSet<Integer>();

    ResourceAssignment newAssignment =
        computeResourceMapping(resourceName, workflowCfg, taskCfg, prevAssignment,
            clusterData.getIdealState(taskCfg.getTargetResource()), clusterData.getLiveInstances()
                .keySet(), currStateOutput, workflowCtx, taskCtx, partitionsToDrop);

    if (!partitionsToDrop.isEmpty()) {
      for (Integer pId : partitionsToDrop) {
        taskIs.getRecord().getMapFields().remove(pName(resourceName, pId));
      }
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      PropertyKey propertyKey = accessor.keyBuilder().idealStates(resourceName);
      accessor.setProperty(propertyKey, taskIs);
    }

    // Update rebalancer context, previous ideal state.
    TaskUtil.setTaskContext(_manager, resourceName, taskCtx);
    TaskUtil.setWorkflowContext(_manager, workflowResource, workflowCtx);
    TaskUtil.setPrevResourceAssignment(_manager, resourceName, newAssignment);

    return newAssignment;
  }

  private static ResourceAssignment computeResourceMapping(String taskResource,
      WorkflowConfig workflowConfig, TaskConfig taskCfg, ResourceAssignment prevAssignment,
      IdealState tgtResourceIs, Iterable<String> liveInstances, CurrentStateOutput currStateOutput,
      WorkflowContext workflowCtx, TaskContext taskCtx, Set<Integer> partitionsToDropFromIs) {
    TargetState taskTgtState = workflowConfig.getTargetState();

    // Update running status in workflow context
    if (taskTgtState == TargetState.STOP) {
      workflowCtx.setTaskState(taskResource, TaskState.STOPPED);
      // Workflow has been stopped if all tasks are stopped
      if (isWorkflowStopped(workflowCtx, workflowConfig)) {
        workflowCtx.setWorkflowState(TaskState.STOPPED);
      }
    } else {
      workflowCtx.setTaskState(taskResource, TaskState.IN_PROGRESS);
      // Workflow is in progress if any task is in progress
      workflowCtx.setWorkflowState(TaskState.IN_PROGRESS);
    }

    // Used to keep track of task partitions that have already been assigned to instances.
    Set<Integer> assignedPartitions = new HashSet<Integer>();

    // Keeps a mapping of (partition) -> (instance, state)
    Map<Integer, PartitionAssignment> paMap = new TreeMap<Integer, PartitionAssignment>();

    // Process all the current assignments of task partitions.
    Set<Integer> allPartitions = getAllTaskPartitions(tgtResourceIs, taskCfg);
    Map<String, SortedSet<Integer>> taskAssignments =
        getTaskPartitionAssignments(liveInstances, prevAssignment, allPartitions);
    for (String instance : taskAssignments.keySet()) {
      Set<Integer> pSet = taskAssignments.get(instance);
      // Used to keep track of partitions that are in one of the final states: COMPLETED, TIMED_OUT,
      // TASK_ERROR, ERROR.
      Set<Integer> donePartitions = new TreeSet<Integer>();
      for (int pId : pSet) {
        final String pName = pName(taskResource, pId);

        // Check for pending state transitions on this (partition, instance).
        String pendingState =
            currStateOutput.getPendingState(taskResource, new Partition(pName), instance);
        if (pendingState != null) {
          // There is a pending state transition for this (partition, instance). Just copy forward
          // the state
          // assignment from the previous ideal state.
          Map<String, String> stateMap = prevAssignment.getReplicaMap(new Partition(pName));
          if (stateMap != null) {
            String prevState = stateMap.get(instance);
            paMap.put(pId, new PartitionAssignment(instance, prevState));
            assignedPartitions.add(pId);
            LOG.debug(String
                .format(
                    "Task partition %s has a pending state transition on instance %s. Using the previous ideal state which was %s.",
                    pName, instance, prevState));
          }

          continue;
        }

        TaskPartitionState currState =
            TaskPartitionState.valueOf(currStateOutput.getCurrentState(taskResource, new Partition(
                pName), instance));

        // Process any requested state transitions.
        String requestedStateStr =
            currStateOutput.getRequestedState(taskResource, new Partition(pName), instance);
        if (requestedStateStr != null && !requestedStateStr.isEmpty()) {
          TaskPartitionState requestedState = TaskPartitionState.valueOf(requestedStateStr);
          if (requestedState.equals(currState)) {
            LOG.warn(String.format(
                "Requested state %s is the same as the current state for instance %s.",
                requestedState, instance));
          }

          paMap.put(pId, new PartitionAssignment(instance, requestedState.name()));
          assignedPartitions.add(pId);
          LOG.debug(String.format(
              "Instance %s requested a state transition to %s for partition %s.", instance,
              requestedState, pName));
          continue;
        }

        switch (currState) {
        case RUNNING:
        case STOPPED: {
          TaskPartitionState nextState;
          if (taskTgtState == TargetState.START) {
            nextState = TaskPartitionState.RUNNING;
          } else {
            nextState = TaskPartitionState.STOPPED;
          }

          paMap.put(pId, new PartitionAssignment(instance, nextState.name()));
          assignedPartitions.add(pId);
          LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
              nextState, instance));
        }
          break;
        case COMPLETED: {
          // The task has completed on this partition. Mark as such in the context object.
          donePartitions.add(pId);
          LOG.debug(String
              .format(
                  "Task partition %s has completed with state %s. Marking as such in rebalancer context.",
                  pName, currState));
          partitionsToDropFromIs.add(pId);
          markPartitionCompleted(taskCtx, pId);
        }
          break;
        case TIMED_OUT:
        case TASK_ERROR:
        case ERROR: {
          donePartitions.add(pId); // The task may be rescheduled on a different instance.
          LOG.debug(String.format(
              "Task partition %s has error state %s. Marking as such in rebalancer context.",
              pName, currState));
          markPartitionError(taskCtx, pId, currState);
          // The error policy is to fail the task as soon a single partition fails for a specified
          // maximum number of
          // attempts.
          if (taskCtx.getPartitionNumAttempts(pId) >= taskCfg.getMaxAttemptsPerPartition()) {
            workflowCtx.setTaskState(taskResource, TaskState.FAILED);
            workflowCtx.setWorkflowState(TaskState.FAILED);
            addAllPartitions(tgtResourceIs.getPartitionSet(), partitionsToDropFromIs);
            return emptyAssignment(taskResource);
          }
        }
          break;
        case INIT:
        case DROPPED: {
          // currState in [INIT, DROPPED]. Do nothing, the partition is eligible to be reassigned.
          donePartitions.add(pId);
          LOG.debug(String.format(
              "Task partition %s has state %s. It will be dropped from the current ideal state.",
              pName, currState));
        }
          break;
        default:
          throw new AssertionError("Unknown enum symbol: " + currState);
        }
      }

      // Remove the set of task partitions that are completed or in one of the error states.
      pSet.removeAll(donePartitions);
    }

    if (isTaskComplete(taskCtx, allPartitions)) {
      workflowCtx.setTaskState(taskResource, TaskState.COMPLETED);
      if (isWorkflowComplete(workflowCtx, workflowConfig)) {
        workflowCtx.setWorkflowState(TaskState.COMPLETED);
        workflowCtx.setFinishTime(System.currentTimeMillis());
      }
    }

    // Make additional task assignments if needed.
    if (taskTgtState == TargetState.START) {
      // Contains the set of task partitions that must be excluded from consideration when making
      // any new assignments.
      // This includes all completed, failed, already assigned partitions.
      Set<Integer> excludeSet = Sets.newTreeSet(assignedPartitions);
      addCompletedPartitions(excludeSet, taskCtx, allPartitions);
      // Get instance->[partition, ...] mappings for the target resource.
      Map<String, SortedSet<Integer>> tgtPartitionAssignments =
          getTgtPartitionAssignment(currStateOutput, liveInstances, tgtResourceIs,
              taskCfg.getTargetPartitionStates(), allPartitions);
      for (Map.Entry<String, SortedSet<Integer>> entry : taskAssignments.entrySet()) {
        String instance = entry.getKey();
        // Contains the set of task partitions currently assigned to the instance.
        Set<Integer> pSet = entry.getValue();
        int numToAssign = taskCfg.getNumConcurrentTasksPerInstance() - pSet.size();
        if (numToAssign > 0) {
          List<Integer> nextPartitions =
              getNextPartitions(tgtPartitionAssignments.get(instance), excludeSet, numToAssign);
          for (Integer pId : nextPartitions) {
            String pName = pName(taskResource, pId);
            paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.RUNNING.name()));
            excludeSet.add(pId);
            LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
                TaskPartitionState.RUNNING, instance));
          }
        }
      }
    }

    // Construct a ResourceAssignment object from the map of partition assignments.
    ResourceAssignment ra = new ResourceAssignment(taskResource);
    for (Map.Entry<Integer, PartitionAssignment> e : paMap.entrySet()) {
      PartitionAssignment pa = e.getValue();
      ra.addReplicaMap(new Partition(pName(taskResource, e.getKey())),
          ImmutableMap.of(pa._instance, pa._state));
    }

    return ra;
  }

  /**
   * Checks if the task has completed.
   * @param ctx The rebalancer context.
   * @param allPartitions The set of partitions to check.
   * @return true if all task partitions have been marked with status
   *         {@link TaskPartitionState#COMPLETED} in the rebalancer
   *         context, false otherwise.
   */
  private static boolean isTaskComplete(TaskContext ctx, Set<Integer> allPartitions) {
    for (Integer pId : allPartitions) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state != TaskPartitionState.COMPLETED) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the workflow has completed.
   * @param ctx Workflow context containing task states
   * @param cfg Workflow config containing set of tasks
   * @return returns true if all tasks are {@link TaskState#COMPLETED}, false otherwise.
   */
  private static boolean isWorkflowComplete(WorkflowContext ctx, WorkflowConfig cfg) {
    for (String task : cfg.getTaskDag().getAllNodes()) {
      if (ctx.getTaskState(task) != TaskState.COMPLETED) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the workflow has been stopped.
   * @param ctx Workflow context containing task states
   * @param cfg Workflow config containing set of tasks
   * @return returns true if all tasks are {@link TaskState#STOPPED}, false otherwise.
   */
  private static boolean isWorkflowStopped(WorkflowContext ctx, WorkflowConfig cfg) {
    for (String task : cfg.getTaskDag().getAllNodes()) {
      if (ctx.getTaskState(task) != TaskState.STOPPED && ctx.getTaskState(task) != null) {
        return false;
      }
    }
    return true;
  }

  private static void markForDeletion(HelixManager mgr, String resourceName) {
    mgr.getConfigAccessor().set(
        TaskUtil.getResourceConfigScope(mgr.getClusterName(), resourceName),
        WorkflowConfig.TARGET_STATE, TargetState.DELETE.name());
  }

  /**
   * Cleans up all Helix state associated with this task, wiping workflow-level information if this
   * is the last
   * remaining task in its workflow.
   */
  private static void cleanup(HelixManager mgr, String resourceName, WorkflowConfig cfg,
      String workflowResource) {
    HelixDataAccessor accessor = mgr.getHelixDataAccessor();
    // Delete resource configs.
    PropertyKey cfgKey = getConfigPropertyKey(accessor, resourceName);
    if (!accessor.removeProperty(cfgKey)) {
      throw new RuntimeException(
          String
              .format(
                  "Error occurred while trying to clean up task %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
                  resourceName, cfgKey));
    }
    // Delete property store information for this resource.
    String propStoreKey = getRebalancerPropStoreKey(resourceName);
    if (!mgr.getHelixPropertyStore().remove(propStoreKey, AccessOption.PERSISTENT)) {
      throw new RuntimeException(
          String
              .format(
                  "Error occurred while trying to clean up task %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
                  resourceName, propStoreKey));
    }
    // Finally, delete the ideal state itself.
    PropertyKey isKey = getISPropertyKey(accessor, resourceName);
    if (!accessor.removeProperty(isKey)) {
      throw new RuntimeException(String.format(
          "Error occurred while trying to clean up task %s. Failed to remove node %s from Helix.",
          resourceName, isKey));
    }
    LOG.info(String.format("Successfully cleaned up task resource %s.", resourceName));

    boolean lastInWorkflow = true;
    for (String task : cfg.getTaskDag().getAllNodes()) {
      // check if property store information or resource configs exist for this task
      if (mgr.getHelixPropertyStore().exists(getRebalancerPropStoreKey(task),
          AccessOption.PERSISTENT)
          || accessor.getProperty(getConfigPropertyKey(accessor, task)) != null
          || accessor.getProperty(getISPropertyKey(accessor, task)) != null) {
        lastInWorkflow = false;
      }
    }

    // clean up task-level info if this was the last in workflow
    if (lastInWorkflow) {
      // delete workflow config
      PropertyKey workflowCfgKey = getConfigPropertyKey(accessor, workflowResource);
      if (!accessor.removeProperty(workflowCfgKey)) {
        throw new RuntimeException(
            String
                .format(
                    "Error occurred while trying to clean up workflow %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
                    workflowResource, workflowCfgKey));
      }
      // Delete property store information for this workflow
      String workflowPropStoreKey = getRebalancerPropStoreKey(workflowResource);
      if (!mgr.getHelixPropertyStore().remove(workflowPropStoreKey, AccessOption.PERSISTENT)) {
        throw new RuntimeException(
            String
                .format(
                    "Error occurred while trying to clean up workflow %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
                    workflowResource, workflowPropStoreKey));
      }
    }

  }

  private static String getRebalancerPropStoreKey(String resource) {
    return Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resource);
  }

  private static PropertyKey getISPropertyKey(HelixDataAccessor accessor, String resource) {
    return accessor.keyBuilder().idealStates(resource);
  }

  private static PropertyKey getConfigPropertyKey(HelixDataAccessor accessor, String resource) {
    return accessor.keyBuilder().resourceConfig(resource);
  }

  private static void addAllPartitions(Set<String> pNames, Set<Integer> pIds) {
    for (String pName : pNames) {
      pIds.add(pId(pName));
    }
  }

  private static ResourceAssignment emptyAssignment(String name) {
    return new ResourceAssignment(name);
  }

  private static void addCompletedPartitions(Set<Integer> set, TaskContext ctx,
      Iterable<Integer> pIds) {
    for (Integer pId : pIds) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state == TaskPartitionState.COMPLETED) {
        set.add(pId);
      }
    }
  }

  /**
   * Returns the set of all partition ids for a task.
   * <p/>
   * If a set of partition ids was explicitly specified in the config, that is used. Otherwise, we
   * use the list of all partition ids from the target resource.
   */
  private static Set<Integer> getAllTaskPartitions(IdealState tgtResourceIs, TaskConfig taskCfg) {
    Set<Integer> taskPartitions = new HashSet<Integer>();
    if (taskCfg.getTargetPartitions() != null) {
      for (Integer pId : taskCfg.getTargetPartitions()) {
        taskPartitions.add(pId);
      }
    } else {
      for (String pName : tgtResourceIs.getPartitionSet()) {
        taskPartitions.add(pId(pName));
      }
    }

    return taskPartitions;
  }

  private static List<Integer> getNextPartitions(SortedSet<Integer> candidatePartitions,
      Set<Integer> excluded, int n) {
    List<Integer> result = new ArrayList<Integer>(n);
    for (Integer pId : candidatePartitions) {
      if (result.size() >= n) {
        break;
      }

      if (!excluded.contains(pId)) {
        result.add(pId);
      }
    }

    return result;
  }

  private static void markPartitionCompleted(TaskContext ctx, int pId) {
    ctx.setPartitionState(pId, TaskPartitionState.COMPLETED);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
    ctx.incrementNumAttempts(pId);
  }

  private static void markPartitionError(TaskContext ctx, int pId, TaskPartitionState state) {
    ctx.setPartitionState(pId, state);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
    ctx.incrementNumAttempts(pId);
  }

  /**
   * Get partition assignments for the target resource, but only for the partitions of interest.
   * @param currStateOutput The current state of the instances in the cluster.
   * @param instanceList The set of instances.
   * @param tgtIs The ideal state of the target resource.
   * @param tgtStates Only partitions in this set of states will be considered. If null, partitions
   *          do not need to
   *          be in any specific state to be considered.
   * @param includeSet The set of partitions to consider.
   * @return A map of instance vs set of partition ids assigned to that instance.
   */
  private static Map<String, SortedSet<Integer>> getTgtPartitionAssignment(
      CurrentStateOutput currStateOutput, Iterable<String> instanceList, IdealState tgtIs,
      Set<String> tgtStates, Set<Integer> includeSet) {
    Map<String, SortedSet<Integer>> result = new HashMap<String, SortedSet<Integer>>();
    for (String instance : instanceList) {
      result.put(instance, new TreeSet<Integer>());
    }

    for (String pName : tgtIs.getPartitionSet()) {
      int pId = pId(pName);
      if (includeSet.contains(pId)) {
        for (String instance : instanceList) {
          String state =
              currStateOutput.getCurrentState(tgtIs.getResourceName(), new Partition(pName),
                  instance);
          if (tgtStates == null || tgtStates.contains(state)) {
            result.get(instance).add(pId);
          }
        }
      }
    }

    return result;
  }

  /**
   * Return the assignment of task partitions per instance.
   */
  private static Map<String, SortedSet<Integer>> getTaskPartitionAssignments(
      Iterable<String> instanceList, ResourceAssignment assignment, Set<Integer> includeSet) {
    Map<String, SortedSet<Integer>> result = new HashMap<String, SortedSet<Integer>>();
    for (String instance : instanceList) {
      result.put(instance, new TreeSet<Integer>());
    }

    for (Partition partition : assignment.getMappedPartitions()) {
      int pId = pId(partition.getPartitionName());
      if (includeSet.contains(pId)) {
        Map<String, String> replicaMap = assignment.getReplicaMap(partition);
        for (String instance : replicaMap.keySet()) {
          SortedSet<Integer> pList = result.get(instance);
          if (pList != null) {
            pList.add(pId);
          }
        }
      }
    }

    return result;
  }

  /**
   * Computes the partition name given the resource name and partition id.
   */
  private static String pName(String resource, int pId) {
    return resource + "_" + pId;
  }

  /**
   * Extracts the partition id from the given partition name.
   */
  private static int pId(String pName) {
    String[] tokens = pName.split("_");
    return Integer.valueOf(tokens[tokens.length - 1]);
  }

  /**
   * An (instance, state) pair.
   */
  private static class PartitionAssignment {
    private final String _instance;
    private final String _state;

    private PartitionAssignment(String instance, String state) {
      _instance = instance;
      _state = state;
    }
  }

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    return currentIdealState;
  }
}
