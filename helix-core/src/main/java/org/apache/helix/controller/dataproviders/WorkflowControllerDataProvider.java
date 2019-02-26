package org.apache.helix.controller.dataproviders;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.caches.AbstractDataCache;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data provider for workflow controller.
 *
 * This class will be moved to helix-workflow-controller module in the future
 */
public class WorkflowControllerDataProvider extends BaseControllerDataProvider {
  private static final Logger logger =
      LoggerFactory.getLogger(WorkflowControllerDataProvider.class);
  private static final String PIPELINE_NAME = "WorkflowPipeline";

  private TaskDataCache _taskDataCache;
  private Map<String, Integer> _participantActiveTaskCount;

  // For detecting live instance and target resource partition state change in task assignment
  // Used in AbstractTaskDispatcher
  private boolean _existsLiveInstanceOrCurrentStateChange = false;

  public WorkflowControllerDataProvider() {
    this(AbstractDataCache.UNKNOWN_CLUSTER);
  }

  public WorkflowControllerDataProvider(String clusterName) {
    super(clusterName, PIPELINE_NAME);
    _participantActiveTaskCount = new HashMap<>();
    _taskDataCache = new TaskDataCache(this);
  }

  private void refreshClusterStateChangeFlags() {
    // This is for targeted jobs' task assignment. It needs to watch for current state changes for
    // when targeted resources' state transitions complete
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.CURRENT_STATE)
        || _propertyDataChangedMap.get(HelixConstants.ChangeType.LIVE_INSTANCE)) {
      _existsLiveInstanceOrCurrentStateChange = true;

      // BaseControllerDataProvider will take care of marking live instance change
      _propertyDataChangedMap.put(HelixConstants.ChangeType.CURRENT_STATE, false);
    } else {
      _existsLiveInstanceOrCurrentStateChange = false;
    }
  }

  public synchronized void refresh(HelixDataAccessor accessor) {
    long startTime = System.currentTimeMillis();
    refreshClusterStateChangeFlags();
    super.refresh(accessor);

    // Refresh TaskCache
    _taskDataCache.refresh(accessor, getResourceConfigMap());

    // Refresh AssignableInstanceManager
    AssignableInstanceManager assignableInstanceManager =
        _taskDataCache.getAssignableInstanceManager();

    // Build from scratch every time
    assignableInstanceManager.buildAssignableInstances(getClusterConfig(), _taskDataCache,
        getLiveInstances(), getInstanceConfigMap());

    // TODO: (Hunter) Consider this for optimization after fixing the problem of quotas not being
    assignableInstanceManager.logQuotaProfileJSON(false);

    long duration = System.currentTimeMillis() - startTime;
    LogUtil.logInfo(logger, getClusterEventId(), String.format(
        "END: WorkflowControllerDataProvider.refresh() for cluster %s took %s for %s pipeline",
        getClusterName(), duration, getPipelineName()));
    dumpDebugInfo();
  }

  protected void dumpDebugInfo() {
    super.dumpDebugInfo();
    LogUtil.logDebug(logger, getClusterEventId(),
        "JobContexts: " + _taskDataCache.getContexts().keySet());

    if (logger.isTraceEnabled()) {
      logger.trace("Cache content: " + toString());
    }
  }

  public synchronized void setLiveInstances(List<LiveInstance> liveInstances) {
    _existsLiveInstanceOrCurrentStateChange = true;
    super.setLiveInstances(liveInstances);
  }

  /**
   * Returns job config map
   * @return
   */
  public Map<String, JobConfig> getJobConfigMap() {
    return _taskDataCache.getJobConfigMap();
  }

  /**
   * Returns job config
   * @param resource
   * @return
   */
  public JobConfig getJobConfig(String resource) {
    return _taskDataCache.getJobConfig(resource);
  }

  /**
   * Returns workflow config map
   * @return
   */
  public Map<String, WorkflowConfig> getWorkflowConfigMap() {
    return _taskDataCache.getWorkflowConfigMap();
  }

  /**
   * Returns workflow config
   * @param resource
   * @return
   */
  public WorkflowConfig getWorkflowConfig(String resource) {
    return _taskDataCache.getWorkflowConfig(resource);
  }

  public Integer getParticipantActiveTaskCount(String instance) {
    return _participantActiveTaskCount.get(instance);
  }

  public void setParticipantActiveTaskCount(String instance, int taskCount) {
    _participantActiveTaskCount.put(instance, taskCount);
  }

  /**
   * Reset RUNNING/INIT tasks count in JobRebalancer
   */
  public void resetActiveTaskCount(CurrentStateOutput currentStateOutput) {
    // init participant map
    for (String liveInstance : getLiveInstances().keySet()) {
      _participantActiveTaskCount.put(liveInstance, 0);
    }
    // Active task == init and running tasks
    fillActiveTaskCount(
        currentStateOutput.getPartitionCountWithPendingState(TaskConstants.STATE_MODEL_NAME,
            TaskPartitionState.INIT.name()),
        _participantActiveTaskCount);
    fillActiveTaskCount(
        currentStateOutput.getPartitionCountWithPendingState(TaskConstants.STATE_MODEL_NAME,
            TaskPartitionState.RUNNING.name()),
        _participantActiveTaskCount);
    fillActiveTaskCount(
        currentStateOutput.getPartitionCountWithCurrentState(TaskConstants.STATE_MODEL_NAME,
            TaskPartitionState.INIT.name()),
        _participantActiveTaskCount);
    fillActiveTaskCount(
        currentStateOutput.getPartitionCountWithCurrentState(TaskConstants.STATE_MODEL_NAME,
            TaskPartitionState.RUNNING.name()),
        _participantActiveTaskCount);
  }

  private void fillActiveTaskCount(Map<String, Integer> additionPartitionMap,
      Map<String, Integer> partitionMap) {
    for (String participant : additionPartitionMap.keySet()) {
      partitionMap.put(participant,
          partitionMap.get(participant) + additionPartitionMap.get(participant));
    }
  }

  /**
   * Return the JobContext by resource name
   * @param resourceName
   * @return
   */
  public JobContext getJobContext(String resourceName) {
    return _taskDataCache.getJobContext(resourceName);
  }

  /**
   * Return the WorkflowContext by resource name
   * @param resourceName
   * @return
   */
  public WorkflowContext getWorkflowContext(String resourceName) {
    return _taskDataCache.getWorkflowContext(resourceName);
  }

  /**
   * Update context of the Job
   */
  public void updateJobContext(String resourceName, JobContext jobContext) {
    _taskDataCache.updateJobContext(resourceName, jobContext);
  }

  /**
   * Update context of the Workflow
   */
  public void updateWorkflowContext(String resourceName, WorkflowContext workflowContext) {
    _taskDataCache.updateWorkflowContext(resourceName, workflowContext);
  }

  public TaskDataCache getTaskDataCache() {
    return _taskDataCache;
  }

  /**
   * Return map of WorkflowContexts or JobContexts
   * @return
   */
  public Map<String, ZNRecord> getContexts() {
    return _taskDataCache.getContexts();
  }

  /**
   * Returns AssignableInstanceManager.
   * @return
   */
  public AssignableInstanceManager getAssignableInstanceManager() {
    return _taskDataCache.getAssignableInstanceManager();
  }

  /**
   * Returns whether there has been LiveInstance or CurrentState change. To be used for
   * task-assigning in AbstractTaskDispatcher.
   * @return
   */
  public boolean getExistsLiveInstanceOrCurrentStateChange() {
    return _existsLiveInstanceOrCurrentStateChange;
  }

  @Override
  public String toString() {
    StringBuilder sb = genCacheContentStringBuilder();
    sb.append(String.format("taskDataCache: %s", _taskDataCache)).append("\n");
    return sb.toString();
  }
}
