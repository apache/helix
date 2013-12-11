package org.apache.helix.task;

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

import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;

/**
 * Static utility methods.
 */
public class TaskUtil {
  private static final Logger LOG = Logger.getLogger(TaskUtil.class);

  enum TaskUtilEnum {
    CONTEXT_NODE("Context"),
    PREV_RA_NODE("PreviousResourceAssignment");

    final String _value;

    private TaskUtilEnum(String value) {
      _value = value;
    }

    public String value() {
      return _value;
    }
  }

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
      currStateDelta.setRequestedState(PartitionId.from(partition), State.from(state.name()));

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
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName,
                TaskUtilEnum.PREV_RA_NODE.value()), null, AccessOption.PERSISTENT);
    return r != null ? new ResourceAssignment(r) : null;
  }

  public static void setPrevResourceAssignment(HelixManager manager, String resourceName,
      ResourceAssignment ra) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName,
            TaskUtilEnum.PREV_RA_NODE.value()), ra.getRecord(), AccessOption.PERSISTENT);
  }

  public static TaskContext getTaskContext(HelixManager manager, String taskResource) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, taskResource,
                TaskUtilEnum.CONTEXT_NODE.value()), null, AccessOption.PERSISTENT);
    return r != null ? new TaskContext(r) : null;
  }

  public static void setTaskContext(HelixManager manager, String taskResource, TaskContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, taskResource,
            TaskUtilEnum.CONTEXT_NODE.value()), ctx.getRecord(), AccessOption.PERSISTENT);
  }

  public static WorkflowContext getWorkflowContext(HelixManager manager, String workflowResource) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource,
                TaskUtilEnum.CONTEXT_NODE.value()), null, AccessOption.PERSISTENT);
    return r != null ? new WorkflowContext(r) : null;
  }

  public static void setWorkflowContext(HelixManager manager, String workflowResource,
      WorkflowContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource,
            TaskUtilEnum.CONTEXT_NODE.value()), ctx.getRecord(), AccessOption.PERSISTENT);
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
