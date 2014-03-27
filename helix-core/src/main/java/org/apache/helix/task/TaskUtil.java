/*
 * $Id$
 */
package org.apache.helix.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

/**
 * Static utility methods.
 */
public class TaskUtil {
  private static final Logger LOG = Logger.getLogger(TaskUtil.class);
  private static final String CONTEXT_NODE = "Context";
  private static final String PREV_RA_NODE = "PreviousResourceAssignment";

  /**
   * Parses task resource configurations in Helix into a {@link TaskConfig} object.
   * @param manager HelixManager object used to connect to Helix.
   * @param taskResource The name of the task resource.
   * @return A {@link TaskConfig} object if Helix contains valid configurations for the task, null
   *         otherwise.
   */
  public static TaskConfig getTaskCfg(HelixManager manager, String taskResource) {
    Map<String, String> taskCfg = getResourceConfigMap(manager, taskResource);
    TaskConfig.Builder b = TaskConfig.Builder.fromMap(taskCfg);

    return b.build();
  }

  public static WorkflowConfig getWorkflowCfg(HelixManager manager, String workflowResource) {
    Map<String, String> workflowCfg = getResourceConfigMap(manager, workflowResource);
    WorkflowConfig.Builder b = WorkflowConfig.Builder.fromMap(workflowCfg);

    return b.build();
  }

  public static boolean setRequestedState(HelixDataAccessor accessor, String instance,
      String sessionId, String resource, String partition, TaskPartitionState state) {
    LOG.debug(String.format("Requesting a state transition to %s for partition %s.", state,
        partition));
    try {
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      PropertyKey key = keyBuilder.currentState(instance, sessionId, resource);
      CurrentState currStateDelta = new CurrentState(resource);
      currStateDelta.setRequestedState(partition, state.name());

      return accessor.updateProperty(key, currStateDelta);
    } catch (Exception e) {
      LOG.error(String.format("Error when requesting a state transition to %s for partition %s.",
          state, partition), e);
      return false;
    }
  }

  public static HelixConfigScope getResourceConfigScope(String clusterName, String resource) {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
        .forCluster(clusterName).forResource(resource).build();
  }

  public static ResourceAssignment getPrevResourceAssignment(HelixManager manager,
      String resourceName) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new ResourceAssignment(r) : null;
  }

  public static void setPrevResourceAssignment(HelixManager manager, String resourceName,
      ResourceAssignment ra) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
        ra.getRecord(), AccessOption.PERSISTENT);
  }

  public static TaskContext getTaskContext(HelixManager manager, String taskResource) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, taskResource, CONTEXT_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new TaskContext(r) : null;
  }

  public static void setTaskContext(HelixManager manager, String taskResource, TaskContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, taskResource, CONTEXT_NODE),
        ctx.getRecord(), AccessOption.PERSISTENT);
  }

  public static WorkflowContext getWorkflowContext(HelixManager manager, String workflowResource) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource,
                CONTEXT_NODE), null, AccessOption.PERSISTENT);
    return r != null ? new WorkflowContext(r) : null;
  }

  public static void setWorkflowContext(HelixManager manager, String workflowResource,
      WorkflowContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource, CONTEXT_NODE),
        ctx.getRecord(), AccessOption.PERSISTENT);
  }

  public static String getNamespacedTaskName(String singleTaskWorkflow) {
    return getNamespacedTaskName(singleTaskWorkflow, singleTaskWorkflow);
  }

  public static String getNamespacedTaskName(String workflowResource, String taskName) {
    return workflowResource + "_" + taskName;
  }

  private static Map<String, String> getResourceConfigMap(HelixManager manager, String resource) {
    HelixConfigScope scope = getResourceConfigScope(manager.getClusterName(), resource);
    ConfigAccessor configAccessor = manager.getConfigAccessor();

    Map<String, String> taskCfg = new HashMap<String, String>();
    List<String> cfgKeys = configAccessor.getKeys(scope);
    if (cfgKeys == null || cfgKeys.isEmpty()) {
      return null;
    }

    for (String cfgKey : cfgKeys) {
      taskCfg.put(cfgKey, configAccessor.get(scope, cfgKey));
    }

    return taskCfg;
  }
}
