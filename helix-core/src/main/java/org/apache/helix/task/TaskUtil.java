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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Static utility methods.
 */
public class TaskUtil {
  private static final Logger LOG = Logger.getLogger(TaskUtil.class);
  private static final String CONTEXT_NODE = "Context";
  private static final String PREV_RA_NODE = "PreviousResourceAssignment";

  /**
   * Parses job resource configurations in Helix into a {@link JobConfig} object.
   * @param manager HelixManager object used to connect to Helix.
   * @param jobResource The name of the job resource.
   * @return A {@link JobConfig} object if Helix contains valid configurations for the job, null
   *         otherwise.
   */
  public static JobConfig getJobCfg(HelixManager manager, String jobResource) {
    HelixProperty jobResourceConfig = getResourceConfig(manager, jobResource);
    JobConfig.Builder b =
        JobConfig.Builder.fromMap(jobResourceConfig.getRecord().getSimpleFields());
    Map<String, Map<String, String>> rawTaskConfigMap =
        jobResourceConfig.getRecord().getMapFields();
    Map<String, TaskConfig> taskConfigMap = Maps.newHashMap();
    for (Map<String, String> rawTaskConfig : rawTaskConfigMap.values()) {
      TaskConfig taskConfig = TaskConfig.from(rawTaskConfig);
      taskConfigMap.put(taskConfig.getId(), taskConfig);
    }
    b.addTaskConfigMap(taskConfigMap);
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

  public static JobContext getJobContext(HelixManager manager, String jobResource) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, CONTEXT_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new JobContext(r) : null;
  }

  public static void setJobContext(HelixManager manager, String jobResource, JobContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, CONTEXT_NODE),
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

  public static String getNamespacedJobName(String singleJobWorkflow) {
    return getNamespacedJobName(singleJobWorkflow, singleJobWorkflow);
  }

  public static String getNamespacedJobName(String workflowResource, String jobName) {
    return workflowResource + "_" + jobName;
  }

  public static String serializeJobConfigMap(Map<String, String> commandConfig) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      String serializedMap = mapper.writeValueAsString(commandConfig);
      return serializedMap;
    } catch (IOException e) {
      LOG.error("Error serializing " + commandConfig, e);
    }
    return null;
  }

  public static Map<String, String> deserializeJobConfigMap(String commandConfig) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      Map<String, String> commandConfigMap =
          mapper.readValue(commandConfig, new TypeReference<HashMap<String, String>>() {
          });
      return commandConfigMap;
    } catch (IOException e) {
      LOG.error("Error deserializing " + commandConfig, e);
    }
    return Collections.emptyMap();
  }

  /**
   * Trigger a controller pipeline execution for a given resource.
   * @param manager Helix connection
   * @param resource the name of the resource changed to triggering the execution
   */
  public static void invokeRebalance(HelixManager manager, String resource) {
    // The pipeline is idempotent, so touching an ideal state is enough to trigger a pipeline run
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    accessor.updateProperty(accessor.keyBuilder().idealStates(resource), new IdealState(resource));
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

    return getResourceConfig(manager, resource).getRecord().getSimpleFields();
  }

  private static HelixProperty getResourceConfig(HelixManager manager, String resource) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.resourceConfig(resource));
  }
}
