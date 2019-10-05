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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.util.RebalanceUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

/**
 * Static utility methods.
 */
public class TaskUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TaskUtil.class);
  public static final String CONTEXT_NODE = "Context";
  public static final String USER_CONTENT_NODE = "UserContent";
  public static final String WORKFLOW_CONTEXT_KW = "WorkflowContext";
  public static final String TASK_CONTEXT_KW = "TaskContext";

  /**
   * Parses job resource configurations in Helix into a {@link JobConfig} object.
   * This method is internal API, please use the corresponding one in TaskDriver.getJobConfig();
   * @param accessor Accessor to access Helix configs
   * @param job The name of the job resource
   * @return A {@link JobConfig} object if Helix contains valid configurations for the job, null
   *         otherwise.
   */
  protected static JobConfig getJobConfig(HelixDataAccessor accessor, String job) {
    HelixProperty jobResourceConfig = getResourceConfig(accessor, job);
    if (jobResourceConfig == null) {
      return null;
    }
    return new JobConfig(jobResourceConfig);
  }

  /**
   * Parses job resource configurations in Helix into a {@link JobConfig} object.
   * This method is internal API, please use the corresponding one in TaskDriver.getJobConfig();
   * @param manager HelixManager object used to connect to Helix.
   * @param job The name of the job resource.
   * @return A {@link JobConfig} object if Helix contains valid configurations for the job, null
   *         otherwise.
   */
  protected static JobConfig getJobConfig(HelixManager manager, String job) {
    return getJobConfig(manager.getHelixDataAccessor(), job);
  }

  /**
   * Creates a job config. Returns false if the job of the same name already exists.
   * @param accessor Accessor to Helix configs
   * @param job The job name
   * @param jobConfig The job config to be set
   * @return True if set successfully, otherwise false
   */
  protected static boolean createJobConfig(HelixDataAccessor accessor, String job,
      JobConfig jobConfig) {
    return createResourceConfig(accessor, job, jobConfig);
  }

  /**
   * Remove a job config.
   * @param accessor
   * @param job
   * @return
   */
  protected static boolean removeJobConfig(HelixDataAccessor accessor, String job) {
    return removeWorkflowJobConfig(accessor, job);
  }

  /**
   * Parses workflow resource configurations in Helix into a {@link WorkflowConfig} object.
   * This method is internal API, please use the corresponding one in
   * TaskDriver.getWorkflowConfig();
   * @param accessor Accessor to access Helix configs
   * @param workflow The name of the workflow.
   * @return A {@link WorkflowConfig} object if Helix contains valid configurations for the
   *         workflow, null otherwise.
   */
  protected static WorkflowConfig getWorkflowConfig(HelixDataAccessor accessor, String workflow) {
    HelixProperty workflowCfg = getResourceConfig(accessor, workflow);
    if (workflowCfg == null) {
      return null;
    }

    return new WorkflowConfig(workflowCfg);
  }

  /**
   * Parses workflow resource configurations in Helix into a {@link WorkflowConfig} object.
   * This method is internal API, please use the corresponding one in
   * TaskDriver.getWorkflowConfig();
   * @param manager Helix manager object used to connect to Helix.
   * @param workflow The name of the workflow resource.
   * @return A {@link WorkflowConfig} object if Helix contains valid configurations for the
   *         workflow, null otherwise.
   */
  protected static WorkflowConfig getWorkflowConfig(HelixManager manager, String workflow) {
    return getWorkflowConfig(manager.getHelixDataAccessor(), workflow);
  }

  /**
   * Create the workflow config. Fails if the ZNode already exists.
   * @param accessor
   * @param workflow
   * @param workflowConfig
   * @return
   */
  protected static boolean createWorkflowConfig(HelixDataAccessor accessor, String workflow,
      WorkflowConfig workflowConfig) {
    return createResourceConfig(accessor, workflow, workflowConfig);
  }

  /**
   * Set the workflow config
   * @param accessor Accessor to Helix configs
   * @param workflow The workflow name
   * @param workflowConfig The workflow config to be set
   * @return True if set successfully, otherwise false
   */
  protected static boolean setWorkflowConfig(HelixDataAccessor accessor, String workflow,
      WorkflowConfig workflowConfig) {
    return setResourceConfig(accessor, workflow, workflowConfig);
  }

  /**
   * Remove a workflow config.
   * @param accessor
   * @param workflow
   * @return
   */
  protected static boolean removeWorkflowConfig(HelixDataAccessor accessor, String workflow) {
    return removeWorkflowJobConfig(accessor, workflow);
  }

  /**
   * Get a Helix configuration scope at a resource (i.e. job and workflow) level
   * @param clusterName the cluster containing the resource
   * @param resource the resource name
   * @return instantiated {@link HelixConfigScope}
   */
  protected static HelixConfigScope getResourceConfigScope(String clusterName, String resource) {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
        .forCluster(clusterName).forResource(resource).build();
  }

  /**
   * Get the runtime context of a single job.
   * This method is internal API, please use TaskDriver.getJobContext();
   * @param propertyStore Property store for the cluster
   * @param jobResource The name of the job
   * @return the {@link JobContext}, or null if none is available
   */
  protected static JobContext getJobContext(HelixPropertyStore<ZNRecord> propertyStore,
      String jobResource) {
    ZNRecord r = propertyStore.get(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, CONTEXT_NODE), null,
        AccessOption.PERSISTENT);
    return r != null ? new JobContext(r) : null;
  }

  /**
   * Get the runtime context of a single job.
   * This method is internal API, please use TaskDriver.getJobContext();
   * @param manager a connection to Helix
   * @param jobResource the name of the job
   * @return the {@link JobContext}, or null if none is available
   */
  protected static JobContext getJobContext(HelixManager manager, String jobResource) {
    return getJobContext(manager.getHelixPropertyStore(), jobResource);
  }

  /**
   * Set the runtime context of a single job
   * This method is internal API;
   * @param manager a connection to Helix
   * @param jobResource the name of the job
   * @param ctx the up-to-date {@link JobContext} for the job
   */
  protected static void setJobContext(HelixManager manager, String jobResource, JobContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, CONTEXT_NODE),
        ctx.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Remove the runtime context of a single job.
   * This method is internal API.
   * @param manager A connection to Helix
   * @param jobResource The name of the job
   * @return True if remove success, otherwise false
   */
  protected static boolean removeJobContext(HelixManager manager, String jobResource) {
    return removeJobContext(manager.getHelixPropertyStore(), jobResource);
  }

  /**
   * Remove the runtime context of a single job.
   * This method is internal API.
   * @param propertyStore Property store for the cluster
   * @param job The name of the job
   * @return True if remove success, otherwise false
   */
  protected static boolean removeJobContext(HelixPropertyStore<ZNRecord> propertyStore,
      String job) {
    return removeWorkflowJobContext(propertyStore, job);
  }

  /**
   * Get the runtime context of a single workflow.
   * This method is internal API, please use the corresponding one in
   * TaskDriver.getWorkflowContext();
   * @param propertyStore Property store of the cluster
   * @param workflow The name of the workflow
   * @return the {@link WorkflowContext}, or null if none is available
   */
  protected static WorkflowContext getWorkflowContext(HelixPropertyStore<ZNRecord> propertyStore,
      String workflow) {
    ZNRecord r = propertyStore.get(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflow, CONTEXT_NODE), null,
        AccessOption.PERSISTENT);
    return r != null ? new WorkflowContext(r) : null;
  }

  /**
   * Get the runtime context of a single workflow.
   * This method is internal API, please use the corresponding one in
   * TaskDriver.getWorkflowContext();
   * @param manager a connection to Helix
   * @param workflow the name of the workflow
   * @return the {@link WorkflowContext}, or null if none is available
   */
  protected static WorkflowContext getWorkflowContext(HelixManager manager, String workflow) {
    return getWorkflowContext(manager.getHelixPropertyStore(), workflow);
  }

  /**
   * Set the runtime context of a single workflow
   * @param manager a connection to Helix
   * @param workflow the name of the workflow
   * @param workflowContext the up-to-date {@link WorkflowContext} for the workflow
   */
  protected static void setWorkflowContext(HelixManager manager, String workflow,
      WorkflowContext workflowContext) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflow, CONTEXT_NODE),
        workflowContext.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Remove the runtime context of a single workflow.
   * This method is internal API.
   * @param manager A connection to Helix
   * @param workflow The name of the workflow
   * @return True if remove success, otherwise false
   */
  protected static boolean removeWorkflowContext(HelixManager manager, String workflow) {
    return removeWorkflowContext(manager.getHelixPropertyStore(), workflow);
  }

  /**
   * Remove the runtime context of a single workflow.
   * This method is internal API.
   * @param propertyStore Property store for the cluster
   * @param workflow The name of the workflow
   * @return True if remove success, otherwise false
   */
  protected static boolean removeWorkflowContext(HelixPropertyStore<ZNRecord> propertyStore,
      String workflow) {
    return removeWorkflowJobContext(propertyStore, workflow);
  }

  /**
   * Intialize the user content store znode setup
   * @param propertyStore zookeeper property store
   * @param workflowJobResource the name of workflow or job
   * @param record the initial data
   */
  protected static void createUserContent(HelixPropertyStore<ZNRecord> propertyStore,
      String workflowJobResource, ZNRecord record) {
    propertyStore.create(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT,
        workflowJobResource, TaskUtil.USER_CONTENT_NODE), record, AccessOption.PERSISTENT);
  }

  /**
   * Get user-defined workflow/job scope key-value pair data. This method takes
   * HelixPropertyStore<ZNRecord>.
   * @param propertyStore
   * @param workflowJobResource
   * @param key
   * @return null if there is no such pair, otherwise return a String
   */
  protected static String getWorkflowJobUserContent(HelixPropertyStore<ZNRecord> propertyStore,
      String workflowJobResource, String key) {
    Map<String, String> userContentMap =
        getWorkflowJobUserContentMap(propertyStore, workflowJobResource);
    return userContentMap != null ? userContentMap.get(key) : null;
  }

  /**
   * get workflow/job user content map
   * @param propertyStore property store
   * @param workflowJobResource workflow name or namespaced job name
   * @return user content map
   */
  protected static Map<String, String> getWorkflowJobUserContentMap(
      HelixPropertyStore<ZNRecord> propertyStore, String workflowJobResource) {
    ZNRecord record = propertyStore.get(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT,
        workflowJobResource, USER_CONTENT_NODE), null, AccessOption.PERSISTENT);
    return record != null ? record.getSimpleFields() : null;
  }

  /**
   * Add an user defined key-value pair data to workflow or job level
   * @param manager a connection to Helix
   * @param workflowJobResource the name of workflow or job
   * @param key the key of key-value pair
   * @param value the value of key-value pair
   */
  protected static void addWorkflowJobUserContent(final HelixManager manager,
      String workflowJobResource, final String key, final String value) {
    addOrUpdateWorkflowJobUserContentMap(manager.getHelixPropertyStore(), workflowJobResource,
        Collections.singletonMap(key, value));
  }

  /* package */
  static void addOrUpdateWorkflowJobUserContentMap(final HelixPropertyStore<ZNRecord> propertyStore,
      String workflowJobResource, final Map<String, String> contentToAddOrUpdate) {
    if (workflowJobResource == null) {
      throw new IllegalArgumentException(
          "workflowJobResource must be not null when adding workflow / job user content");
    }
    String path = Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowJobResource,
        USER_CONTENT_NODE);

    if (!propertyStore.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord znRecord) {
        if (znRecord == null) {
          // This indicates that somehow the UserContentStore ZNode is missing
          // This should not happen, but if it is missing, create one
          znRecord = new ZNRecord(new ZNRecord(TaskUtil.USER_CONTENT_NODE));
        }
        znRecord.getSimpleFields().putAll(contentToAddOrUpdate);
        return znRecord;
      }
    }, AccessOption.PERSISTENT)) {
      LOG.error("Failed to update the UserContentStore for {}", workflowJobResource);
    }
  }

  /**
   * Get user defined task level key-value pair data
   * @param propertyStore
   * @param job the name of job
   * @param task the name of the task
   * @param key the key of key-value pair
   * @return null if there is no such pair, otherwise return a String
   */
  protected static String getTaskUserContent(HelixPropertyStore<ZNRecord> propertyStore, String job,
      String task, String key) {
    Map<String, String> userContentStore = getTaskUserContentMap(propertyStore, job, task);
    return userContentStore != null ? userContentStore.get(key) : null;
  }

  /**
   * Return full task user content map
   * @param propertyStore property store
   * @param namespacedJobName namespaced job name
   * @param taskPartitionId task partition id
   * @return
   */
  protected static Map<String, String> getTaskUserContentMap(
      HelixPropertyStore<ZNRecord> propertyStore, String namespacedJobName,
      String taskPartitionId) {
    ZNRecord record = propertyStore.get(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT,
        namespacedJobName, USER_CONTENT_NODE), null, AccessOption.PERSISTENT);
    return record != null ? record.getMapField(taskPartitionId) : null;
  }

  /**
   * Add an user defined key-value pair data to task level
   * @param manager a connection to Helix
   * @param job the name of job
   * @param task the name of task
   * @param key the key of key-value pair
   * @param value the value of key-value pair
   */
  protected static void addTaskUserContent(final HelixManager manager, String job,
      final String task, final String key, final String value) {
    addOrUpdateTaskUserContentMap(manager.getHelixPropertyStore(), job, task,
        Collections.singletonMap(key, value));
  }

  /* package */
  static void addOrUpdateTaskUserContentMap(final HelixPropertyStore<ZNRecord> propertyStore,
      final String job, final String task, final Map<String, String> contentToAddOrUpdate) {
    if (job == null || task == null) {
      throw new IllegalArgumentException(
          "job and task must be not null when adding task user content");
    }
    String path =
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, job, USER_CONTENT_NODE);

    if (!propertyStore.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord znRecord) {
        if (znRecord == null) {
          // This indicates that somehow the UserContentStore ZNode is missing
          // This should not happen, but if it is missing, create one
          znRecord = new ZNRecord(new ZNRecord(TaskUtil.USER_CONTENT_NODE));
        }
        if (znRecord.getMapField(task) == null) {
          znRecord.setMapField(task, new HashMap<String, String>());
        }
        znRecord.getMapField(task).putAll(contentToAddOrUpdate);
        return znRecord;
      }
    }, AccessOption.PERSISTENT)) {
      LOG.error("Failed to update the task UserContentStore for task {} in job {}", task, job);
    }
  }

  /**
   * Helper method for looking up UserContentStore content.
   * @param propertyStore
   * @param key
   * @param scope
   * @param workflowName
   * @param jobName
   * @param taskName
   * @return value corresponding to the key
   */
  protected static String getUserContent(HelixPropertyStore propertyStore, String key,
      UserContentStore.Scope scope, String workflowName, String jobName, String taskName) {
    switch (scope) {
    case WORKFLOW:
      return TaskUtil.getWorkflowJobUserContent(propertyStore, workflowName, key);
    case JOB:
      return TaskUtil.getWorkflowJobUserContent(propertyStore, jobName, key);
    case TASK:
      return TaskUtil.getTaskUserContent(propertyStore, jobName, taskName, key);
    default:
      throw new HelixException("Invalid scope : " + scope.name());
    }
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
   * @param workflow the name of the workflow
   * @param jobName the un-namespaced name of the job
   * @return The namespaced job name, which is just workflowResource_jobName
   */
  public static String getNamespacedJobName(String workflow, String jobName) {
    return workflow + "_" + jobName;
  }

  /**
   * get a task name, namespaced by it's job and workflow
   * @param namespacedJobName namespaced job name
   * @param taskPartitionId task partition id
   * @return
   */
  public static String getNamespacedTaskName(String namespacedJobName, String taskPartitionId) {
    return String.format("%s_%s", namespacedJobName, taskPartitionId);
  }

  /**
   * Remove the workflow namespace from the job name
   * @param workflow the name of the workflow that owns the job
   * @param jobName the namespaced job name
   * @return the denamespaced job name, or the same job name if it is already denamespaced
   */
  public static String getDenamespacedJobName(String workflow, String jobName) {
    if (jobName.contains(workflow)) {
      // skip the entire length of the work plus the underscore
      return jobName.substring(jobName.indexOf(workflow) + workflow.length() + 1);
    } else {
      return jobName;
    }
  }

  /**
   * Serialize a map of job-level configurations as a single string
   * @param commandConfig map of job config key to config value
   * @return serialized string
   */
  // TODO: move this to the JobConfig
  @Deprecated
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
  // TODO: move this to the JobConfig
  @Deprecated
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
   * Extracts the partition id from the given partition name.
   * @param pName
   * @return
   */
  public static int getPartitionId(String pName) {
    int index = pName.lastIndexOf("_");
    if (index == -1) {
      throw new HelixException(String.format("Invalid partition name %s", pName));
    }
    return Integer.valueOf(pName.substring(index + 1));
  }

  @Deprecated
  public static String getWorkflowContextKey(String workflow) {
    // TODO: fix this to use the keyBuilder.
    return Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflow);
  }

  @Deprecated
  public static PropertyKey getWorkflowConfigKey(final HelixDataAccessor accessor,
      String workflow) {
    return accessor.keyBuilder().resourceConfig(workflow);
  }

  /**
   * Cleans up IdealState and external view associated with a job.
   * @param accessor
   * @param job
   * @return True if remove success, otherwise false
   */
  protected static boolean cleanupJobIdealStateExtView(final HelixDataAccessor accessor,
      String job) {
    return cleanupIdealStateExtView(accessor, job);
  }

  /**
   * Cleans up IdealState and external view associated with a workflow.
   * @param accessor
   * @param workflow
   * @return True if remove success, otherwise false
   */
  protected static boolean cleanupWorkflowIdealStateExtView(final HelixDataAccessor accessor,
      String workflow) {
    return cleanupIdealStateExtView(accessor, workflow);
  }

  /**
   * Cleans up IdealState and external view associated with a job/workflow resource.
   */
  private static boolean cleanupIdealStateExtView(final HelixDataAccessor accessor,
      String workflowJobResource) {
    boolean success = true;
    PropertyKey isKey = accessor.keyBuilder().idealStates(workflowJobResource);
    if (accessor.getPropertyStat(isKey) != null) {
      if (!accessor.removeProperty(isKey)) {
        LOG.warn(String.format(
            "Error occurred while trying to remove IdealState for %s. Failed to remove node %s.",
            workflowJobResource, isKey));
        success = false;
      }
    }

    // Delete external view
    PropertyKey evKey = accessor.keyBuilder().externalView(workflowJobResource);
    if (accessor.getPropertyStat(evKey) != null) {
      if (!accessor.removeProperty(evKey)) {
        LOG.warn(String.format(
            "Error occurred while trying to remove ExternalView of resource %s. Failed to remove node %s.",
            workflowJobResource, evKey));
        success = false;
      }
    }

    return success;
  }

  /**
   * Remove a workflow and all jobs for the workflow. This removes the workflow config, idealstate,
   * externalview and workflow contexts associated with this workflow, and all jobs information,
   * including their configs, context, IS and EV.
   * @param accessor
   * @param propertyStore
   * @param workflow the workflow name.
   * @param jobs all job names in this workflow.
   * @return True if remove success, otherwise false
   */
  protected static boolean removeWorkflow(final HelixDataAccessor accessor,
      final HelixPropertyStore<ZNRecord> propertyStore, String workflow, Set<String> jobs) {
    // clean up all jobs
    for (String job : jobs) {
      if (!removeJob(accessor, propertyStore, job)) {
        return false;
      }
    }

    if (!removeWorkflowConfig(accessor, workflow)) {
      LOG.warn(
          String.format("Error occurred while trying to remove workflow config for %s.", workflow));
      return false;
    }
    if (!cleanupWorkflowIdealStateExtView(accessor, workflow)) {
      LOG.warn(String.format(
          "Error occurred while trying to remove workflow idealstate/externalview for %s.",
          workflow));
      return false;
    }
    if (!removeWorkflowContext(propertyStore, workflow)) {
      LOG.warn(String.format("Error occurred while trying to remove workflow context for %s.",
          workflow));
      return false;
    }
    return true;
  }

  /**
   * Remove a set of jobs from a workflow. This removes the config, context, IS and EV associated
   * with each individual job, and removes all the jobs from the WorkflowConfig, and job states from
   * WorkflowContext.
   * @param dataAccessor
   * @param propertyStore
   * @param jobs
   * @param workflow
   * @param maintainDependency
   * @return True if remove success, otherwise false
   */
  protected static boolean removeJobsFromWorkflow(final HelixDataAccessor dataAccessor,
      final HelixPropertyStore<ZNRecord> propertyStore, final String workflow,
      final Set<String> jobs, boolean maintainDependency) {
    boolean success = true;
    if (!removeJobsFromDag(dataAccessor, workflow, jobs, maintainDependency)) {
      LOG.warn("Error occurred while trying to remove jobs + " + jobs + " from the workflow "
          + workflow);
      success = false;
    }
    if (!removeJobsState(propertyStore, workflow, jobs)) {
      LOG.warn("Error occurred while trying to remove jobs states from workflow + " + workflow
          + " jobs " + jobs);
      success = false;
    }
    for (String job : jobs) {
      if (!removeJob(dataAccessor, propertyStore, job)) {
        success = false;
      }
    }

    return success;
  }

  /**
   * Update all jobs that are COMPLETED and passes its expiry time and the jobs that are missing
   * config.
   * @param dataAccessor
   * @param propertyStore
   * @param workflowConfig
   * @param workflowContext
   * @return
   */
  protected static void getExpiredJobs(HelixDataAccessor dataAccessor,
      HelixPropertyStore<ZNRecord> propertyStore, WorkflowConfig workflowConfig,
      WorkflowContext workflowContext, Set<String> expiredJobs, Set<String> jobsWithoutConfig) {

    if (workflowContext != null) {
      Map<String, TaskState> jobStates = workflowContext.getJobStates();
      for (String job : workflowConfig.getJobDag().getAllNodes()) {
        JobConfig jobConfig = TaskUtil.getJobConfig(dataAccessor, job);
        JobContext jobContext = TaskUtil.getJobContext(propertyStore, job);
        if (jobConfig != null) {
          long expiry = jobConfig.getExpiry();
          if (jobContext != null && jobStates.get(job) == TaskState.COMPLETED) {
            if (jobContext.getFinishTime() != WorkflowContext.UNFINISHED
                && System.currentTimeMillis() >= jobContext.getFinishTime() + expiry) {
              expiredJobs.add(job);
            }
          }
        } else {
          LOG.error(String.format(
              "Job %s exists in JobDAG but JobConfig is missing! Job might have been deleted manually from the JobQueue: %s, or left in the DAG due to a failed clean-up attempt from last purge.",
              job, workflowConfig.getWorkflowId()));
          jobsWithoutConfig.add(job);
        }
      }
    }
  }

  /**
   * Remove Job Config, IS/EV, and Context in order. Job name here must be a namespaced job name.
   * @param accessor
   * @param propertyStore
   * @param job namespaced job name
   * @return
   */
  protected static boolean removeJob(HelixDataAccessor accessor,
      HelixPropertyStore<ZNRecord> propertyStore, String job) {
    if (!removeJobConfig(accessor, job)) {
      LOG.warn(String.format("Error occurred while trying to remove job config for %s.", job));
      return false;
    }
    if (!cleanupJobIdealStateExtView(accessor, job)) {
      LOG.warn(String.format(
          "Error occurred while trying to remove job idealstate/externalview for %s.", job));
      return false;
    }
    if (!removeJobContext(propertyStore, job)) {
      LOG.warn(String.format("Error occurred while trying to remove job context for %s.", job));
      return false;
    }
    return true;
  }

  /** Remove the job name from the DAG from the queue configuration */
  // Job name should be namespaced job name here.
  protected static boolean removeJobsFromDag(final HelixDataAccessor accessor,
      final String workflow, final Set<String> jobsToRemove, final boolean maintainDependency) {
    // Now atomically clear the DAG
    DataUpdater<ZNRecord> dagRemover = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData != null) {
          JobDag jobDag = JobDag.fromJson(
              currentData.getSimpleField(WorkflowConfig.WorkflowConfigProperty.Dag.name()));
          if (jobDag == null) {
            LOG.warn("Could not update DAG for workflow: " + workflow + " JobDag is null.");
            return null;
          }
          for (String job : jobsToRemove) {
            jobDag.removeNode(job, maintainDependency);
          }
          try {
            currentData.setSimpleField(WorkflowConfig.WorkflowConfigProperty.Dag.name(),
                jobDag.toJson());
          } catch (IOException e) {
            throw new IllegalArgumentException(e);
          }
        }
        return currentData;
      }
    };

    String configPath = accessor.keyBuilder().resourceConfig(workflow).getPath();
    if (!accessor.getBaseDataAccessor().update(configPath, dagRemover, AccessOption.PERSISTENT)) {
      LOG.warn("Failed to remove jobs " + jobsToRemove + " from DAG of workflow " + workflow);
      return false;
    }

    return true;
  }

  /**
   * update workflow's property to remove jobs from JOB_STATES if there are already started.
   */
  protected static boolean removeJobsState(final HelixPropertyStore<ZNRecord> propertyStore,
      final String workflow, final Set<String> jobs) {
    String contextPath =
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflow, TaskUtil.CONTEXT_NODE);

    // If the queue is not started, there is no JobState need to be removed.
    if (!propertyStore.exists(contextPath, 0)) {
      return true;
    }

    DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData != null) {
          WorkflowContext workflowContext = new WorkflowContext(currentData);
          workflowContext.removeJobStates(jobs);
          workflowContext.removeJobStartTime(jobs);
          currentData = workflowContext.getRecord();
        }
        return currentData;
      }
    };
    if (!propertyStore.update(contextPath, updater, AccessOption.PERSISTENT)) {
      LOG.warn("Fail to remove job state for jobs " + jobs + " from workflow " + workflow);
      return false;
    }
    return true;
  }

  private static boolean removeWorkflowJobContext(HelixPropertyStore<ZNRecord> propertyStore,
      String workflowJobResource) {
    String path = Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowJobResource);
    if (propertyStore.exists(path, AccessOption.PERSISTENT)) {
      if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
        LOG.warn(String.format(
            "Error occurred while trying to remove workflow/jobcontext for %s. Failed to remove node %s.",
            workflowJobResource, path));
        return false;
      }
    }
    return true;
  }

  /**
   * Remove workflow or job config.
   * @param accessor
   * @param workflowJobResource the workflow or job name
   */
  private static boolean removeWorkflowJobConfig(HelixDataAccessor accessor,
      String workflowJobResource) {
    PropertyKey cfgKey = accessor.keyBuilder().resourceConfig(workflowJobResource);
    if (accessor.getPropertyStat(cfgKey) != null) {
      if (!accessor.removeProperty(cfgKey)) {
        LOG.warn(String.format(
            "Error occurred while trying to remove config for %s. Failed to remove node %s.",
            workflowJobResource, cfgKey));
        return false;
      }
    }

    return true;
  }

  /**
   * Create the resource config. Fails if it already exists in ZK.
   * @param accessor
   * @param resource
   * @param resourceConfig
   * @return
   */
  private static boolean createResourceConfig(HelixDataAccessor accessor, String resource,
      ResourceConfig resourceConfig) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getBaseDataAccessor().create(keyBuilder.resourceConfig(resource).getPath(),
        resourceConfig.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Set the resource config
   * @param accessor Accessor to Helix configs
   * @param resource The resource name
   * @param resourceConfig The resource config to be set
   * @return True if set successfully, otherwise false
   */
  private static boolean setResourceConfig(HelixDataAccessor accessor, String resource,
      ResourceConfig resourceConfig) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.setProperty(keyBuilder.resourceConfig(resource), resourceConfig);
  }

  private static HelixProperty getResourceConfig(HelixDataAccessor accessor, String resource) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.resourceConfig(resource));
  }

  public static Set<Integer> getNonReadyPartitions(JobContext ctx, long now) {
    Set<Integer> nonReadyPartitions = Sets.newHashSet();
    for (int p : ctx.getPartitionSet()) {
      long toStart = ctx.getNextRetryTime(p);
      if (now < toStart) {
        nonReadyPartitions.add(p);
      }
    }
    return nonReadyPartitions;
  }

  /**
   * Returns whether if a given job is a generic job (not a targeted job).
   * @param jobConfig
   * @return
   */
  public static boolean isGenericTaskJob(JobConfig jobConfig) {
    // Targeted jobs may have TaskConfigs, so we check whether the target resource is set
    return jobConfig.getTargetResource() == null || jobConfig.getTargetResource().equals("");
  }

  /**
   * Check whether tasks are just started or still running
   * @param jobContext The job context
   * @return False if still tasks not in final state. Otherwise return true
   */
  public static boolean checkJobStopped(JobContext jobContext) {
    for (int partition : jobContext.getPartitionSet()) {
      TaskPartitionState taskState = jobContext.getPartitionState(partition);
      if (taskState == TaskPartitionState.RUNNING) {
        return false;
      }
    }
    return true;
  }

  /**
   * Count the number of jobs in a workflow that are not in final state.
   * @param workflowCfg
   * @param workflowCtx
   * @return
   */
  public static int getInCompleteJobCount(WorkflowConfig workflowCfg, WorkflowContext workflowCtx) {
    int inCompleteCount = 0;
    for (String jobName : workflowCfg.getJobDag().getAllNodes()) {
      TaskState jobState = workflowCtx.getJobState(jobName);
      if (jobState == TaskState.IN_PROGRESS || jobState == TaskState.STOPPED
          || jobState == TaskState.STOPPING) {
        ++inCompleteCount;
      }
    }

    return inCompleteCount;
  }

  public static boolean isJobStarted(String job, WorkflowContext workflowContext) {
    TaskState jobState = workflowContext.getJobState(job);
    return (jobState != null && jobState != TaskState.NOT_STARTED);
  }

  /**
   * Clean up all jobs that are COMPLETED and passes its expiry time.
   * @param workflowConfig
   * @param workflowContext
   */
  public static void purgeExpiredJobs(String workflow, WorkflowConfig workflowConfig,
      WorkflowContext workflowContext, HelixManager manager,
      RebalanceScheduler rebalanceScheduler) {
    if (workflowContext == null) {
      LOG.warn(String.format("Workflow %s context does not exist!", workflow));
      return;
    }
    long purgeInterval = workflowConfig.getJobPurgeInterval();
    long currentTime = System.currentTimeMillis();
    Set<String> expiredJobs = Sets.newHashSet();
    Set<String> jobsWithoutConfig = Sets.newHashSet();
    if (purgeInterval > 0 && workflowContext.getLastJobPurgeTime() + purgeInterval <= currentTime) {
      TaskUtil.getExpiredJobs(manager.getHelixDataAccessor(),
          manager.getHelixPropertyStore(), workflowConfig, workflowContext, expiredJobs, jobsWithoutConfig);

      if (expiredJobs.isEmpty() && jobsWithoutConfig.isEmpty()) {
        LOG.info("No job to purge for the queue " + workflow);
      } else {
        if (!expiredJobs.isEmpty()) {
          LOG.info("Purge jobs " + expiredJobs + " from queue " + workflow);
          Set<String> failedJobRemovals = new HashSet<>();
          for (String job : expiredJobs) {
            if (!TaskUtil.removeJob(manager.getHelixDataAccessor(), manager.getHelixPropertyStore(),
                job)) {
              failedJobRemovals.add(job);
              LOG.warn("Failed to clean up expired and completed jobs from workflow " + workflow);
            }
            rebalanceScheduler.removeScheduledRebalance(job);
          }

          // If the job removal failed, make sure we do NOT prematurely delete it from DAG so that
          // the removal will be tried again at next purge
          expiredJobs.removeAll(failedJobRemovals);

          if (!TaskUtil.removeJobsFromDag(manager.getHelixDataAccessor(), workflow, expiredJobs,
              true)) {
            LOG.warn("Error occurred while trying to remove jobs + " + expiredJobs
                + " from the workflow " + workflow);
          }
        }
        if (!jobsWithoutConfig.isEmpty()) {
          LOG.info("Purge jobs " + jobsWithoutConfig + " from queue " + workflow);
          for (String job : jobsWithoutConfig) {
            if (!TaskUtil.removeJob(manager.getHelixDataAccessor(), manager.getHelixPropertyStore(),
                job)) {
              LOG.warn("Failed to clean up jobsWithoutConfig jobs from workflow " + workflow);
            }
            rebalanceScheduler.removeScheduledRebalance(job);
          }
          if (!TaskUtil.removeJobsFromDag(manager.getHelixDataAccessor(), workflow,
              jobsWithoutConfig, true)) {
            LOG.warn("Error occurred while trying to remove jobs + " + jobsWithoutConfig
                + " from the workflow " + workflow);
          }
        }
      }
      if (expiredJobs.size() > 0) {
        // Update workflow context will be in main pipeline not here. Otherwise, it will cause
        // concurrent write issue. It is possible that jobs got purged but there is no event to
        // trigger the pipeline to clean context.
        HelixDataAccessor accessor = manager.getHelixDataAccessor();
        List<String> resourceConfigs =
            accessor.getChildNames(accessor.keyBuilder().resourceConfigs());
        if (resourceConfigs.size() > 0) {
          RebalanceUtil.scheduleOnDemandPipeline(manager.getClusterName(), 0L);
        } else {
          LOG.warn(
              "No resource config to trigger rebalance for clean up contexts for " + expiredJobs);
        }
      }
    }
    setNextJobPurgeTime(workflow, currentTime, purgeInterval, rebalanceScheduler, manager);
  }

  private static void setNextJobPurgeTime(String workflow, long currentTime, long purgeInterval,
      RebalanceScheduler rebalanceScheduler, HelixManager manager) {
    long nextPurgeTime = currentTime + purgeInterval;
    long currentScheduledTime = rebalanceScheduler.getRebalanceTime(workflow);
    if (currentScheduledTime == -1 || currentScheduledTime > nextPurgeTime) {
      rebalanceScheduler.scheduleRebalance(manager, workflow, nextPurgeTime);
    }
  }
}
