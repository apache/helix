package org.apache.helix.common.caches;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.RuntimeJobDag;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for holding all task related cluster data, such as WorkflowConfig, JobConfig and Contexts.
 */
public class TaskDataCache extends AbstractDataCache {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDataCache.class.getName());
  private static final String NAME = "NAME";

  private Map<String, JobConfig> _jobConfigMap = new HashMap<>();
  private Map<String, RuntimeJobDag> _runtimeJobDagMap = new HashMap<>();
  private Map<String, WorkflowConfig> _workflowConfigMap = new ConcurrentHashMap<>();

  // TODO: context and previous assignment should be wrapped into a class. Otherwise, int the future,
  // concurrency will be hard to handle.
  private Map<String, ZNRecord> _contextMap = new HashMap<>();
  private Map<String, ZNRecord> _prevAssignmentMap = new HashMap<>();
  private Set<String> _prevAssignmentToUpdate = new HashSet<>();
  private Set<String> _prevAssignmentToRemove = new HashSet<>();
  private Set<String> _contextToUpdate = new HashSet<>();
  private Set<String> _contextToRemove = new HashSet<>();
  // The following fields have been added for quota-based task scheduling
  private final AssignableInstanceManager _assignableInstanceManager =
      new AssignableInstanceManager();
  // Current usage for this scheduled jobs is used for differentiate the jobs has been processed in
  // JobDispatcher from RESOURCE_TO_BALANCE to reduce the redundant computation.
  private Set<String> _dispatchedJobs = new HashSet<>();

  private enum TaskDataType {
    CONTEXT,
    PREV_ASSIGNMENT
  }


  public TaskDataCache(ControlContextProvider contextProvider) {
    super(contextProvider);
  }

  /**
   * Original constructor for TaskDataCache.
   *
   * @param clusterName
   */
  public TaskDataCache(String clusterName) {
    this(createDefaultControlContextProvider(clusterName));
  }

  /**
   * This refreshes the cluster data by re-fetching the data from zookeeper in an efficient way
   *
   * @param accessor
   *
   * @return
   */
  public synchronized boolean refresh(HelixDataAccessor accessor,
      Map<String, ResourceConfig> resourceConfigMap) {
    refreshContextsAndPreviousAssignments(accessor);
    // update workflow and job configs.
    _workflowConfigMap.clear();
    Map<String, JobConfig> newJobConfigs = new HashMap<>();
    Set<String> workflowsUpdated = new HashSet<>();
    for (Map.Entry<String, ResourceConfig> entry : resourceConfigMap.entrySet()) {
      if (entry.getValue().getRecord().getSimpleFields()
          .containsKey(WorkflowConfig.WorkflowConfigProperty.Dag.name())) {
        _workflowConfigMap.put(entry.getKey(), new WorkflowConfig(entry.getValue()));
        if (!_runtimeJobDagMap.containsKey(entry.getKey())) {
          WorkflowConfig workflowConfig = _workflowConfigMap.get(entry.getKey());
          _runtimeJobDagMap.put(entry.getKey(), new RuntimeJobDag(workflowConfig.getJobDag(),
              workflowConfig.isJobQueue() || !workflowConfig.isTerminable(),
              workflowConfig.getParallelJobs()));
        }
      } else if (entry.getValue().getRecord().getSimpleFields()
          .containsKey(WorkflowConfig.WorkflowConfigProperty.WorkflowID.name())) {
        newJobConfigs.put(entry.getKey(), new JobConfig(entry.getValue()));
      }
    }

    // The following 3 blocks is for finding a list of workflows whose JobDAGs have been changed
    // because their RuntimeJobDags would need to be re-built
    // newly added jobs
    for (String jobName : newJobConfigs.keySet()) {
      if (!_jobConfigMap.containsKey(jobName) && newJobConfigs.get(jobName).getWorkflow() != null) {
        workflowsUpdated.add(newJobConfigs.get(jobName).getWorkflow());
      }

      // Only for JobQueues when a new job is enqueued, there exists a race condition where only
      // JobConfig is updated and the RuntimeJobDag does not get updated because when the client
      // (TaskDriver) submits, it creates JobConfig ZNode first and modifies its parent JobDag next.
      // To ensure that they are both properly updated, check that workflow's DAG and existing
      // JobConfigs are consistent for JobQueues
      JobConfig jobConfig = newJobConfigs.get(jobName);
      if (_workflowConfigMap.containsKey(jobConfig.getWorkflow())) {
        WorkflowConfig workflowConfig = _workflowConfigMap.get(jobConfig.getWorkflow());
        // Check that the job's parent workflow's DAG contains this job
        if ((workflowConfig.isJobQueue() || !workflowConfig.isTerminable()) && !_runtimeJobDagMap
            .get(workflowConfig.getWorkflowId()).getAllNodes().contains(jobName)) {
          // Inconsistency between JobConfigs and DAGs found. Add the workflow to workflowsUpdated
          // to rebuild the RuntimeJobDag
          workflowsUpdated.add(jobConfig.getWorkflow());
        }
      }
    }

    // Removed jobs
    for (String jobName : _jobConfigMap.keySet()) {
      if (!newJobConfigs.containsKey(jobName) && _jobConfigMap.get(jobName).getWorkflow() != null) {
        workflowsUpdated.add(_jobConfigMap.get(jobName).getWorkflow());
      }
    }

    // Combine all the workflows' job dag which need update
    for (String changedWorkflow : workflowsUpdated) {
      if (_workflowConfigMap.containsKey(changedWorkflow)) {
        WorkflowConfig workflowConfig = _workflowConfigMap.get(changedWorkflow);
        _runtimeJobDagMap.put(changedWorkflow, new RuntimeJobDag(workflowConfig.getJobDag(),
            workflowConfig.isJobQueue() || !workflowConfig.isTerminable(),
            workflowConfig.getParallelJobs()));
      }
    }

    _dispatchedJobs.clear();
    _runtimeJobDagMap.keySet().retainAll(_workflowConfigMap.keySet());
    _jobConfigMap = newJobConfigs;
    return true;
  }

  private void refreshContextsAndPreviousAssignments(HelixDataAccessor accessor) {
    // TODO: Need an optimize for reading context only if the refresh is needed.
    long start = System.currentTimeMillis();
    _contextMap.clear();
    _prevAssignmentMap.clear();
    if (_controlContextProvider.getClusterName() == null || _controlContextProvider.getClusterName()
        .equalsIgnoreCase(UNKNOWN_CLUSTER)) {
      return;
    }
    String path = String.format("/%s/%s%s", _controlContextProvider.getClusterName(),
        PropertyType.PROPERTYSTORE.name(), TaskConstants.REBALANCER_CONTEXT_ROOT);
    List<String> contextPaths = new ArrayList<>();
    List<String> prevAssignmentPaths = new ArrayList<>();
    List<String> childNames = accessor.getBaseDataAccessor().getChildNames(path, 0);
    if (childNames == null) {
      return;
    }
    for (String resourceName : childNames) {
      contextPaths.add(getTaskDataPath(resourceName, TaskDataType.CONTEXT));
      //Workflow does not have previous assignment
      if (!_workflowConfigMap.containsKey(resourceName)) {
        prevAssignmentPaths.add(getTaskDataPath(resourceName, TaskDataType.PREV_ASSIGNMENT));
      }
    }

    List<ZNRecord> contexts = accessor.getBaseDataAccessor().get(contextPaths, null, 0);
    List<ZNRecord> prevAssignments =
        accessor.getBaseDataAccessor().get(prevAssignmentPaths, null, 0);

    for (int i = 0; i < contexts.size(); i++) {
      ZNRecord context = contexts.get(i);
      if (context != null && context.getSimpleField(NAME) != null) {
        _contextMap.put(context.getSimpleField(NAME), context);
      } else {
        _contextMap.put(childNames.get(i), context);
        LogUtil.logDebug(LOG, genEventInfo(),
            String.format("Context for %s is null or miss the context NAME!", childNames.get((i))));
      }
    }

    for (ZNRecord prevAssignment : prevAssignments) {
      if (prevAssignment != null) {
        _prevAssignmentMap.put(prevAssignment.getId(), prevAssignment);
      }
    }

    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, genEventInfo(),
          "# of workflow/job context read from zk: " + _contextMap.size() + ". Take " + (
              System.currentTimeMillis() - start) + " ms");
    }
  }

  /**
   * Returns job config map
   *
   * @return
   */
  public Map<String, JobConfig> getJobConfigMap() {
    return _jobConfigMap;
  }

  /**
   * Returns job config
   *
   * @param resource
   *
   * @return
   */
  public JobConfig getJobConfig(String resource) {
    return _jobConfigMap.get(resource);
  }

  /**
   * Returns workflow config map
   *
   * @return
   */
  public Map<String, WorkflowConfig> getWorkflowConfigMap() {
    return _workflowConfigMap;
  }

  /**
   * Returns workflow config
   *
   * @param resource
   *
   * @return
   */
  public WorkflowConfig getWorkflowConfig(String resource) {
    return _workflowConfigMap.get(resource);
  }

  /**
   * Return the JobContext by resource name
   *
   * @param resourceName
   *
   * @return
   */
  public JobContext getJobContext(String resourceName) {
    if (_contextMap.containsKey(resourceName) && _contextMap.get(resourceName) != null) {
      return new JobContext(_contextMap.get(resourceName));
    }
    return null;
  }

  /**
   * Return the WorkflowContext by resource name
   *
   * @param resourceName
   *
   * @return
   */
  public WorkflowContext getWorkflowContext(String resourceName) {
    if (_contextMap.containsKey(resourceName) && _contextMap.get(resourceName) != null) {
      return new WorkflowContext(_contextMap.get(resourceName));
    }
    return null;
  }

  /**
   * Update context of the Job
   */
  public void updateJobContext(String resourceName, JobContext jobContext) {
    updateContext(resourceName, jobContext.getRecord());
  }

  /**
   * Update context of the Workflow
   */
  public void updateWorkflowContext(String resourceName, WorkflowContext workflowContext) {
    updateContext(resourceName, workflowContext.getRecord());
  }

  /**
   * Update context of the Workflow or Job
   */
  private void updateContext(String resourceName, ZNRecord record) {
    _contextMap.put(resourceName, record);
    _contextToUpdate.add(resourceName);
  }

  public void persistDataChanges(HelixDataAccessor accessor) {
    // Do not update it if the is need to be remove
    _contextToUpdate.removeAll(_contextToRemove);
    batchUpdateData(accessor, new ArrayList<>(_contextToUpdate), _contextMap, _contextToUpdate,
        TaskDataType.CONTEXT);
    batchDeleteData(accessor, new ArrayList<>(_contextToRemove), TaskDataType.CONTEXT);
    _contextToRemove.clear();

    _prevAssignmentToUpdate.removeAll(_prevAssignmentToRemove);
    batchUpdateData(accessor, new ArrayList<>(_prevAssignmentToUpdate), _prevAssignmentMap,
        _prevAssignmentToUpdate, TaskDataType.PREV_ASSIGNMENT);
    batchDeleteData(accessor, new ArrayList<>(_prevAssignmentToRemove),
        TaskDataType.PREV_ASSIGNMENT);
    _prevAssignmentToRemove.clear();
  }

  private void batchUpdateData(HelixDataAccessor accessor, List<String> dataUpdateNames,
      Map<String, ZNRecord> dataMap, Set<String> dataToUpdate, TaskDataType taskDataType) {
    List<String> contextUpdatePaths = new ArrayList<>();
    List<ZNRecord> updatedData = new ArrayList<>();
    for (String resourceName : dataUpdateNames) {
      if (dataMap.get(resourceName) != null) {
        contextUpdatePaths.add(getTaskDataPath(resourceName, taskDataType));
        updatedData.add(dataMap.get(resourceName));
      }
    }

    boolean[] updateSuccess = accessor.getBaseDataAccessor()
        .setChildren(contextUpdatePaths, updatedData, AccessOption.PERSISTENT);

    for (int i = 0; i < updateSuccess.length; i++) {
      if (updateSuccess[i]) {
        dataToUpdate.remove(dataUpdateNames.get(i));
      } else {
        LogUtil.logWarn(LOG, _controlContextProvider.getClusterEventId(), String
            .format("Failed to update the %s for %s", taskDataType.name(), dataUpdateNames.get(i)));
      }
    }
  }

  private void batchDeleteData(HelixDataAccessor accessor, List<String> contextNamesToRemove,
      TaskDataType taskDataType) {

    // Delete contexts
    // We can not leave the context here since some of the deletion happens for cleaning workflow
    // If we leave it in the memory, Helix will not allow user create it with same name.
    // TODO: Let's have periodical clean up thread that could remove deletion failed contexts.
    List<String> contextPathsToRemove = new ArrayList<>();
    for (String resourceName : contextNamesToRemove) {
      contextPathsToRemove.add(getTaskDataPath(resourceName, taskDataType));
    }

    // TODO: current behavior is when you delete non-existing data will return false.
    // Once the behavior fixed, we can add retry logic back. Otherwise, it will stay in memory and
    // not allow same workflow name recreation.
    accessor.getBaseDataAccessor().remove(contextPathsToRemove, AccessOption.PERSISTENT);
  }

  /**
   * Return map of WorkflowContexts or JobContexts
   *
   * @return
   */
  public Map<String, ZNRecord> getContexts() {
    return _contextMap;
  }

  /**
   * Returns the current AssignableInstanceManager instance.
   *
   * @return
   */
  public AssignableInstanceManager getAssignableInstanceManager() {
    return _assignableInstanceManager;
  }

  /**
   * Remove Workflow or Job context from cache
   *
   * @param resourceName
   */
  public void removeContext(String resourceName) {
    if (_contextMap.containsKey(resourceName)) {
      _contextMap.remove(resourceName);
      _contextToRemove.add(resourceName);
    }
  }

  @Override
  public String toString() {
    return "TaskDataCache{" + "_jobConfigMap=" + _jobConfigMap + ", _workflowConfigMap="
        + _workflowConfigMap + ", _contextMap=" + _contextMap + ", _clusterName='"
        + _controlContextProvider.getClusterName() + '\'' + '}';
  }

  /**
   * Get the path based on different data types. If the type does not exist, it will return null
   * instead.
   *
   * @param resourceName
   * @param taskDataType
   * @return
   */
  private String getTaskDataPath(String resourceName, TaskDataType taskDataType) {
    String prevFix = String.format("/%s/%s%s/%s", _controlContextProvider.getClusterName(),
        PropertyType.PROPERTYSTORE.name(), TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName);
    switch (taskDataType) {
    case CONTEXT:
      return String.format("%s/%s", prevFix, TaskConstants.CONTEXT_NODE);
    case PREV_ASSIGNMENT:
      return String.format("%s/%s", prevFix, TaskConstants.PREV_RA_NODE);
    }
    return null;
  }

  public void dispatchJob(String jobName) {
    _dispatchedJobs.add(jobName);
  }

  public void removeDispatchedJob(String jobName) {
    _dispatchedJobs.remove(jobName);
  }

  public Set<String> getDispatchedJobs() {
    return _dispatchedJobs;
  }

  public RuntimeJobDag getRuntimeJobDag(String workflowName) {
    if (_runtimeJobDagMap.containsKey(workflowName)) {
      return _runtimeJobDagMap.get(workflowName);
    }
    return null;
  }

  public ResourceAssignment getPreviousAssignment(String resourceName) {
    return _prevAssignmentMap.get(resourceName) != null ? new ResourceAssignment(
        _prevAssignmentMap.get(resourceName)) : null;
  }

  public void setPreviousAssignment(String resourceName, ResourceAssignment prevAssignment) {
    _prevAssignmentMap.put(resourceName, prevAssignment.getRecord());
    _prevAssignmentToUpdate.add(resourceName);
  }

  public void removePrevAssignment(String resourceName) {
    _prevAssignmentMap.remove(resourceName);
    _prevAssignmentToRemove.add(resourceName);
  }
}
