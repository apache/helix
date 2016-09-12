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
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.HelixPropertyStore;
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
  public static final String CONTEXT_NODE = "Context";
  public static final String USER_CONTENT_NODE = "UserContent";

  /**
   * Parses job resource configurations in Helix into a {@link JobConfig} object.
   * This method is internal API, please use the corresponding one in TaskDriver.getJobConfig();
   *
   * @param accessor    Accessor to access Helix configs
   * @param jobResource The name of the job resource
   * @return A {@link JobConfig} object if Helix contains valid configurations for the job, null
   * otherwise.
   */
  protected static JobConfig getJobCfg(HelixDataAccessor accessor, String jobResource) {
    HelixProperty jobResourceConfig = getResourceConfig(accessor, jobResource);
    if (jobResourceConfig == null) {
      return null;
    }
    return new JobConfig(jobResourceConfig);
  }

  /**
   * Parses job resource configurations in Helix into a {@link JobConfig} object.
   * This method is internal API, please use the corresponding one in TaskDriver.getJobConfig();
   *
   * @param manager     HelixManager object used to connect to Helix.
   * @param jobResource The name of the job resource.
   * @return A {@link JobConfig} object if Helix contains valid configurations for the job, null
   * otherwise.
   */
  protected static JobConfig getJobCfg(HelixManager manager, String jobResource) {
    return getJobCfg(manager.getHelixDataAccessor(), jobResource);
  }

  /**
   * Parses workflow resource configurations in Helix into a {@link WorkflowConfig} object.
   * This method is internal API, please use the corresponding one in TaskDriver.getWorkflowConfig();
   *
   * @param accessor  Accessor to access Helix configs
   * @param workflow The name of the workflow.
   * @return A {@link WorkflowConfig} object if Helix contains valid configurations for the
   * workflow, null otherwise.
   */
  protected static WorkflowConfig getWorkflowCfg(HelixDataAccessor accessor, String workflow) {
    HelixProperty workflowCfg = getResourceConfig(accessor, workflow);
    if (workflowCfg == null) {
      return null;
    }

   return new WorkflowConfig(workflowCfg);
  }

  /**
   * Parses workflow resource configurations in Helix into a {@link WorkflowConfig} object.
   * This method is internal API, please use the corresponding one in TaskDriver.getWorkflowConfig();
   *
   * @param manager          Helix manager object used to connect to Helix.
   * @param workflow The name of the workflow resource.
   * @return A {@link WorkflowConfig} object if Helix contains valid configurations for the
   * workflow, null otherwise.
   */
  protected static WorkflowConfig getWorkflowCfg(HelixManager manager, String workflow) {
    return getWorkflowCfg(manager.getHelixDataAccessor(), workflow);
  }

  /**
   * Set the resource config
   * @param accessor        Accessor to Helix configs
   * @param resource        The resource name
   * @param resourceConfig  The resource config to be set
   * @return                True if set successfully, otherwise false
   */
  protected static boolean setResourceConfig(HelixDataAccessor accessor, String resource,
      ResourceConfig resourceConfig) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.setProperty(keyBuilder.resourceConfig(resource), resourceConfig);
  }

  /**
   * Get a Helix configuration scope at a resource (i.e. job and workflow) level
   *
   * @param clusterName the cluster containing the resource
   * @param resource    the resource name
   * @return instantiated {@link HelixConfigScope}
   */
  protected static HelixConfigScope getResourceConfigScope(String clusterName, String resource) {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
        .forCluster(clusterName).forResource(resource).build();
  }

  /**
   * Get the runtime context of a single job.
   * This method is internal API, please use TaskDriver.getJobContext();
   *
   * @param propertyStore Property store for the cluster
   * @param jobResource   The name of the job
   * @return the {@link JobContext}, or null if none is available
   */
  protected static JobContext getJobContext(HelixPropertyStore<ZNRecord> propertyStore,
      String jobResource) {
    ZNRecord r = propertyStore
        .get(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, CONTEXT_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new JobContext(r) : null;
  }

  /**
   * Get the runtime context of a single job.
   * This method is internal API, please use TaskDriver.getJobContext();
   *
   * @param manager     a connection to Helix
   * @param jobResource the name of the job
   * @return the {@link JobContext}, or null if none is available
   */
  protected static JobContext getJobContext(HelixManager manager, String jobResource) {
    return getJobContext(manager.getHelixPropertyStore(), jobResource);
  }

  /**
   * Set the runtime context of a single job
   * This method is internal API;
   *
   * @param manager     a connection to Helix
   * @param jobResource the name of the job
   * @param ctx         the up-to-date {@link JobContext} for the job
   */
  protected static void setJobContext(HelixManager manager, String jobResource, JobContext ctx) {
    manager.getHelixPropertyStore()
        .set(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, CONTEXT_NODE),
            ctx.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Remove the runtime context of a single job.
   * This method is internal API.
   *
   * @param manager     A connection to Helix
   * @param jobResource The name of the job
   * @return            True if remove success, otherwise false
   */
  protected static boolean removeJobContext(HelixManager manager, String jobResource) {
    return removeJobContext(manager.getHelixPropertyStore(), jobResource);
  }

  /**
   * Remove the runtime context of a single job.
   * This method is internal API.
   *
   * @param propertyStore Property store for the cluster
   * @param jobResource   The name of the job
   * @return              True if remove success, otherwise false
   */
  protected static boolean removeJobContext(HelixPropertyStore<ZNRecord> propertyStore,
      String jobResource) {
    return propertyStore.remove(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource),
        AccessOption.PERSISTENT);
  }

  /**
   * Get the runtime context of a single workflow.
   * This method is internal API, please use the corresponding one in TaskDriver.getWorkflowContext();
   *
   * @param propertyStore    Property store of the cluster
   * @param workflowResource The name of the workflow
   * @return the {@link WorkflowContext}, or null if none is available
   */
  protected static WorkflowContext getWorkflowContext(HelixPropertyStore<ZNRecord> propertyStore,
      String workflowResource) {
    ZNRecord r = propertyStore.get(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource, CONTEXT_NODE),
        null, AccessOption.PERSISTENT);
    return r != null ? new WorkflowContext(r) : null;
  }

  /**
   * Get the runtime context of a single workflow.
   * This method is internal API, please use the corresponding one in TaskDriver.getWorkflowContext();
   *
   * @param manager          a connection to Helix
   * @param workflowResource the name of the workflow
   * @return the {@link WorkflowContext}, or null if none is available
   */
  protected static WorkflowContext getWorkflowContext(HelixManager manager, String workflowResource) {
    return getWorkflowContext(manager.getHelixPropertyStore(), workflowResource);
  }

  /**
   * Set the runtime context of a single workflow
   *
   * @param manager          a connection to Helix
   * @param workflowResource the name of the workflow
   * @param ctx              the up-to-date {@link WorkflowContext} for the workflow
   */
  protected static void setWorkflowContext(HelixManager manager, String workflowResource,
      WorkflowContext ctx) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource, CONTEXT_NODE),
        ctx.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Remove the runtime context of a single workflow.
   * This method is internal API.
   *
   * @param manager     A connection to Helix
   * @param workflowResource The name of the workflow
   * @return            True if remove success, otherwise false
   */
  protected static boolean removeWorkflowContext(HelixManager manager, String workflowResource) {
    return removeWorkflowContext(manager.getHelixPropertyStore(), workflowResource);
  }

  /**
   * Remove the runtime context of a single workflow.
   * This method is internal API.
   *
   * @param propertyStore      Property store for the cluster
   * @param workflowResource   The name of the workflow
   * @return                   True if remove success, otherwise false
   */
  protected static boolean removeWorkflowContext(HelixPropertyStore<ZNRecord> propertyStore,
      String workflowResource) {
    return propertyStore.remove(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowResource),
        AccessOption.PERSISTENT);
  }

  /**
   * Intialize the user content store znode setup
   * @param propertyStore       zookeeper property store
   * @param workflowJobResource the name of workflow or job
   * @param record              the initial data
   */
  protected static void createUserContent(HelixPropertyStore propertyStore, String workflowJobResource,
      ZNRecord record) {
    propertyStore.create(Joiner.on("/")
        .join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowJobResource,
            TaskUtil.USER_CONTENT_NODE), record, AccessOption.PERSISTENT);
  }

  /**
   * Get user defined workflow or job level key-value pair data
   *
   * @param manager             a connection to Helix
   * @param workflowJobResource the name of workflow
   * @param key                 the key of key-value pair
   *
   * @return null if there is no such pair, otherwise return a String
   */
  protected static String getWorkflowJobUserContent(HelixManager manager,
      String workflowJobResource, String key) {
    ZNRecord r = manager.getHelixPropertyStore().get(Joiner.on("/")
            .join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowJobResource, USER_CONTENT_NODE), null,
        AccessOption.PERSISTENT);
    return r != null ? r.getSimpleField(key) : null;
  }

  /**
   * Add an user defined key-value pair data to workflow or job level
   *
   * @param manager             a connection to Helix
   * @param workflowJobResource the name of workflow or job
   * @param key                 the key of key-value pair
   * @param value               the value of key-value pair
   */
  protected static void addWorkflowJobUserContent(final HelixManager manager,
      String workflowJobResource, final String key, final String value) {
    String path = Joiner.on("/")
        .join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflowJobResource, USER_CONTENT_NODE);

    manager.getHelixPropertyStore().update(path, new DataUpdater<ZNRecord>() {
      @Override public ZNRecord update(ZNRecord znRecord) {
        znRecord.setSimpleField(key, value);
        return znRecord;
      }
    }, AccessOption.PERSISTENT);
  }

  /**
   * Get user defined task level key-value pair data
   *
   * @param manager      a connection to Helix
   * @param jobResource  the name of job
   * @param taskResource the name of the task
   * @param key          the key of key-value pair
   *
   * @return null if there is no such pair, otherwise return a String
   */
  protected static String getTaskUserContent(HelixManager manager, String jobResource,
      String taskResource, String key) {
    ZNRecord r = manager.getHelixPropertyStore().get(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, USER_CONTENT_NODE),
        null, AccessOption.PERSISTENT);
    return r != null ? (r.getMapField(taskResource) != null
        ? r.getMapField(taskResource).get(key)
        : null) : null;
  }

  /**
   * Add an user defined key-value pair data to task level
   *
   * @param manager       a connection to Helix
   * @param jobResource   the name of job
   * @param taskResource  the name of task
   * @param key           the key of key-value pair
   * @param value         the value of key-value pair
   */
  protected static void addTaskUserContent(final HelixManager manager, String jobResource,
      final String taskResource, final String key, final String value) {
    String path =
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobResource, USER_CONTENT_NODE);

    manager.getHelixPropertyStore().update(path, new DataUpdater<ZNRecord>() {
      @Override public ZNRecord update(ZNRecord znRecord) {
        if (znRecord.getMapField(taskResource) == null) {
          znRecord.setMapField(taskResource, new HashMap<String, String>());
        }
        znRecord.getMapField(taskResource).put(key, value);
        return znRecord;
      }
    }, AccessOption.PERSISTENT);
  }
  /**
   * Get a workflow-qualified job name for a single-job workflow
   *
   * @param singleJobWorkflow the name of the single-job workflow
   * @return The namespaced job name, which is just singleJobWorkflow_singleJobWorkflow
   */
  public static String getNamespacedJobName(String singleJobWorkflow) {
    return getNamespacedJobName(singleJobWorkflow, singleJobWorkflow);
  }

  /**
   * Get a workflow-qualified job name for a job in that workflow
   *
   * @param workflowResource the name of the workflow
   * @param jobName          the un-namespaced name of the job
   * @return The namespaced job name, which is just workflowResource_jobName
   */
  public static String getNamespacedJobName(String workflowResource, String jobName) {
    return workflowResource + "_" + jobName;
  }

  /**
   * Remove the workflow namespace from the job name
   *
   * @param workflowResource the name of the workflow that owns the job
   * @param jobName          the namespaced job name
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
   *
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
   *
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

  private static HelixProperty getResourceConfig(HelixDataAccessor accessor, String resource) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.resourceConfig(resource));
  }

  /**
   * Extracts the partition id from the given partition name.
   *
   * @param pName
   * @return
   */
  public static int getPartitionId(String pName) {
    int index = pName.lastIndexOf("_");
    if (index == -1) {
      throw new HelixException("Invalid partition name " + pName);
    }
    return Integer.valueOf(pName.substring(index + 1));
  }

  public static String getWorkflowContextKey(String resource) {
    // TODO: fix this to use the keyBuilder.
    return Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resource);
  }

  public static PropertyKey getWorkflowConfigKey(HelixDataAccessor accessor, String workflow) {
    return accessor.keyBuilder().resourceConfig(workflow);
  }
}
