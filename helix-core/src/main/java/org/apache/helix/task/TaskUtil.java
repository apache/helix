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
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Static utility methods.
 */
public class TaskUtil {
  private static final Logger LOG = Logger.getLogger(TaskUtil.class);
  public static final String CONTEXT_NODE = "Context";
  public static final String PREV_RA_NODE = "PreviousResourceAssignment";

  /**
   * Parses job resource configurations in Helix into a {@link JobConfig} object.
   * @param manager HelixManager object used to connect to Helix.
   * @param jobResource The name of the job resource.
   * @return A {@link JobConfig} object if Helix contains valid configurations for the job, null
   *         otherwise.
   */
  public static JobConfig getJobCfg(HelixManager manager, String jobResource) {
    HelixProperty jobResourceConfig = getResourceConfig(manager, jobResource);
    return getJobCfg(jobResourceConfig);
  }

  /**
   * Parses job resource configurations directly from a property into a {@link JobConfig}.
   * @param jobResourceConfig the property containing the configuration
   * @return A {@link JobConfig} object if the property valid configurations for the job, null
   *         otherwise.
   */
  public static JobConfig getJobCfg(HelixProperty jobResourceConfig) {
    if (jobResourceConfig == null) {
      return null;
    }
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

  /**
   * Parses workflow resource configurations in Helix into a {@link WorkflowConfig} object.
   * @param manager Helix manager object used to connect to Helix.
   * @param workflowResource The name of the workflow resource.
   * @return A {@link WorkflowConfig} object if Helix contains valid configurations for the
   *         workflow, null otherwise.
   */
  public static WorkflowConfig getWorkflowCfg(HelixManager manager, String workflowResource) {
    Map<String, String> workflowCfg = getResourceConfigMap(manager, workflowResource);
    return getWorkflowCfg(workflowCfg);
  }

  /**
   * Parses workflow resource configurations in Helix into a {@link WorkflowConfig} object.
   * @param workflowResourceConfig the proeprty containing the configurations
   * @return A {@link WorkflowConfig} object if the property contains valid configurations for the
   *         workflow, null otherwise.
   */
  public static WorkflowConfig getWorkflowCfg(HelixProperty workflowResourceConfig) {
    if (workflowResourceConfig == null) {
      return null;
    }
    return getWorkflowCfg(workflowResourceConfig.getRecord().getSimpleFields());
  }

  /**
   * Parses a key-value map into a {@link WorkflowConfig} object.
   * @param workflowCfg the map of configurations
   * @return A {@link WorkflowConfig} object if the map contains valid configurations for the
   *         workflow, null otherwise.
   */
  private static WorkflowConfig getWorkflowCfg(Map<String, String> workflowCfg) {
    if (workflowCfg == null) {
      return null;
    }
    WorkflowConfig.Builder b = WorkflowConfig.Builder.fromMap(workflowCfg);

    return b.build();
  }

  /**
   * Request a state change for a specific task.
   * @param accessor connected Helix data accessor
   * @param instance the instance serving the task
   * @param sessionId the current session of the instance
   * @param resource the job name
   * @param partition the task partition name
   * @param state the requested state
   * @return true if the request was persisted, false otherwise
   */
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

  /**
   * Get a Helix configuration scope at a resource (i.e. job and workflow) level
   * @param clusterName the cluster containing the resource
   * @param resource the resource name
   * @return instantiated {@link HelixConfigScope}
   */
  public static HelixConfigScope getResourceConfigScope(String clusterName, String resource) {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
        .forCluster(clusterName).forResource(resource).build();
  }

  /**
   * Get the last task assignment for a given job
   * @param manager a connection to Helix
   * @param resourceName the name of the job
   * @return {@link ResourceAssignment} instance, or null if no assignment is available
   */
  public static ResourceAssignment getPrevResourceAssignment(HelixManager manager,
      String resourceName) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new ResourceAssignment(r) : null;
  }

  /**
   * Set the last task assignment for a given job
   * @param manager a connection to Helix
   * @param resourceName the name of the job
   * @param ra {@link ResourceAssignment} containing the task assignment
   */
  public static void setPrevResourceAssignment(HelixManager manager, String resourceName,
      ResourceAssignment ra) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
        ra.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Get the runtime context of a single job
   * @param manager a connection to Helix
   * @param jobResource the name of the job
   * @return the {@link JobContext}, or null if none is available
   */
  public static JobContext getJobContext(HelixManager manager, String jobResource) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, CONTEXT_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new JobContext(r) : null;
  }

  /**
   * Set the runtime context of a single job
   * @param manager a connection to Helix
   * @param jobResource the name of the job
   * @param ctx the up-to-date {@link JobContext} for the job
   */
  public static void setJobContext(HelixManager manager, String jobResource, JobContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, CONTEXT_NODE),
        ctx.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Get the rumtime context of a single workflow
   * @param manager a connection to Helix
   * @param workflowResource the name of the workflow
   * @return the {@link WorkflowContext}, or null if none is available
   */
  public static WorkflowContext getWorkflowContext(HelixManager manager, String workflowResource) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource,
                CONTEXT_NODE), null, AccessOption.PERSISTENT);
    return r != null ? new WorkflowContext(r) : null;
  }

  /**
   * Set the rumtime context of a single workflow
   * @param manager a connection to Helix
   * @param workflowResource the name of the workflow
   * @param ctx the up-to-date {@link WorkflowContext} for the workflow
   */
  public static void setWorkflowContext(HelixManager manager, String workflowResource,
      WorkflowContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource, CONTEXT_NODE),
        ctx.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Get a workflow-qualified job name for a single-job workflow
   * @param singleJobWorkflow the name of the single-job workflow
   * @return The namespaced job name, which is just singleJobWorkflow_singleJobWorkflow
   */
  public static String getNamespacedJobName(String singleJobWorkflow) {
    return getNamespacedJobName(singleJobWorkflow, singleJobWorkflow);
  }

  /**
   * Get a workflow-qualified job name for a job in that workflow
   * @param workflowResource the name of the workflow
   * @param jobName the un-namespaced name of the job
   * @return The namespaced job name, which is just workflowResource_jobName
   */
  public static String getNamespacedJobName(String workflowResource, String jobName) {
    return workflowResource + "_" + jobName;
  }

  /**
   * Remove the workflow namespace from the job name
   * @param workflowResource the name of the workflow that owns the job
   * @param jobName the namespaced job name
   * @return the denamespaced job name, or the same job name if it is already denamespaced
   */
  public static String getDenamespacedJobName(String workflowResource, String jobName) {
    if (jobName.contains(workflowResource)) {
      // skip the entire length of the work plus the underscore
      return jobName.substring(jobName.indexOf(workflowResource) + workflowResource.length() + 1);
    } else {
      return jobName;
    }
  }

  /**
   * Serialize a map of job-level configurations as a single string
   * @param commandConfig map of job config key to config value
   * @return serialized string
   */
  public static String serializeJobCommandConfigMap(Map<String, String> commandConfig) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      String serializedMap = mapper.writeValueAsString(commandConfig);
      return serializedMap;
    } catch (IOException e) {
      LOG.error("Error serializing " + commandConfig, e);
    }
    return null;
  }

  /**
   * Deserialize a single string into a map of job-level configurations
   * @param commandConfig the serialized job config map
   * @return a map of job config key to config value
   */
  public static Map<String, String> deserializeJobCommandConfigMap(String commandConfig) {
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

  /**
   * Get a ScheduleConfig from a workflow config string map
   * @param cfg the string map
   * @return a ScheduleConfig if one exists, otherwise null
   */
  public static ScheduleConfig parseScheduleFromConfigMap(Map<String, String> cfg) {
    // Parse schedule-specific configs, if they exist
    Date startTime = null;
    if (cfg.containsKey(WorkflowConfig.START_TIME)) {
      try {
        startTime = WorkflowConfig.getDefaultDateFormat().parse(cfg.get(WorkflowConfig.START_TIME));
      } catch (ParseException e) {
        LOG.error("Unparseable date " + cfg.get(WorkflowConfig.START_TIME), e);
        return null;
      }
    }
    if (cfg.containsKey(WorkflowConfig.RECURRENCE_UNIT)
        && cfg.containsKey(WorkflowConfig.RECURRENCE_INTERVAL)) {
      return ScheduleConfig.recurringFromDate(startTime,
          TimeUnit.valueOf(cfg.get(WorkflowConfig.RECURRENCE_UNIT)),
          Long.parseLong(cfg.get(WorkflowConfig.RECURRENCE_INTERVAL)));
    } else if (startTime != null) {
      return ScheduleConfig.oneTimeDelayedStart(startTime);
    }
    return null;
  }

  /**
   * Create a new workflow based on an existing one
   * @param manager connection to Helix
   * @param origWorkflowName the name of the existing workflow
   * @param newWorkflowName the name of the new workflow
   * @param newStartTime a provided start time that deviates from the desired start time
   * @return the cloned workflow, or null if there was a problem cloning the existing one
   */
  public static Workflow cloneWorkflow(HelixManager manager, String origWorkflowName,
      String newWorkflowName, Date newStartTime) {
    // Read all resources, including the workflow and jobs of interest
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Map<String, HelixProperty> resourceConfigMap =
        accessor.getChildValuesMap(keyBuilder.resourceConfigs());
    if (!resourceConfigMap.containsKey(origWorkflowName)) {
      LOG.error("No such workflow named " + origWorkflowName);
      return null;
    }
    if (resourceConfigMap.containsKey(newWorkflowName)) {
      LOG.error("Workflow with name " + newWorkflowName + " already exists!");
      return null;
    }

    // Create a new workflow with a new name
    HelixProperty workflowConfig = resourceConfigMap.get(origWorkflowName);
    Map<String, String> wfSimpleFields = workflowConfig.getRecord().getSimpleFields();
    JobDag jobDag = JobDag.fromJson(wfSimpleFields.get(WorkflowConfig.DAG));
    Map<String, Set<String>> parentsToChildren = jobDag.getParentsToChildren();
    Workflow.Builder builder = new Workflow.Builder(newWorkflowName);

    // Set the workflow expiry
    builder.setExpiry(Long.parseLong(wfSimpleFields.get(WorkflowConfig.EXPIRY)));

    // Set the schedule, if applicable
    ScheduleConfig scheduleConfig;
    if (newStartTime != null) {
      scheduleConfig = ScheduleConfig.oneTimeDelayedStart(newStartTime);
    } else {
      scheduleConfig = parseScheduleFromConfigMap(wfSimpleFields);
    }
    if (scheduleConfig != null) {
      builder.setScheduleConfig(scheduleConfig);
    }

    // Add each job back as long as the original exists
    Set<String> namespacedJobs = jobDag.getAllNodes();
    for (String namespacedJob : namespacedJobs) {
      if (resourceConfigMap.containsKey(namespacedJob)) {
        // Copy over job-level and task-level configs
        String job = getDenamespacedJobName(origWorkflowName, namespacedJob);
        HelixProperty jobConfig = resourceConfigMap.get(namespacedJob);
        Map<String, String> jobSimpleFields = jobConfig.getRecord().getSimpleFields();
        jobSimpleFields.put(JobConfig.WORKFLOW_ID, newWorkflowName); // overwrite workflow name
        for (Map.Entry<String, String> e : jobSimpleFields.entrySet()) {
          builder.addConfig(job, e.getKey(), e.getValue());
        }
        Map<String, Map<String, String>> rawTaskConfigMap = jobConfig.getRecord().getMapFields();
        List<TaskConfig> taskConfigs = Lists.newLinkedList();
        for (Map<String, String> rawTaskConfig : rawTaskConfigMap.values()) {
          TaskConfig taskConfig = TaskConfig.from(rawTaskConfig);
          taskConfigs.add(taskConfig);
        }
        builder.addTaskConfigs(job, taskConfigs);

        // Add dag dependencies
        Set<String> children = parentsToChildren.get(namespacedJob);
        if (children != null) {
          for (String namespacedChild : children) {
            String child = getDenamespacedJobName(origWorkflowName, namespacedChild);
            builder.addParentChildDependency(job, child);
          }
        }
      }
    }
    return builder.build();
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
