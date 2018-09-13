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

import com.google.common.base.Joiner;
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
import org.apache.helix.controller.LogUtil;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
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

  private String _clusterName;
  private Map<String, JobConfig> _jobConfigMap = new HashMap<>();
  private Map<String, WorkflowConfig> _workflowConfigMap = new ConcurrentHashMap<>();
  private Map<String, ZNRecord> _contextMap = new HashMap<>();
  private Set<String> _contextToUpdate = new HashSet<>();
  private Set<String> _contextToRemove = new HashSet<>();
  // The following fields have been added for quota-based task scheduling
  private final AssignableInstanceManager _assignableInstanceManager = new AssignableInstanceManager();

  /**
   * Original constructor for TaskDataCache.
   * @param clusterName
   */
  public TaskDataCache(String clusterName) {
    _clusterName = clusterName;
  }

  /**
   * This refreshes the cluster data by re-fetching the data from zookeeper in an efficient way
   * @param accessor
   * @return
   */
  public synchronized boolean refresh(HelixDataAccessor accessor,
      Map<String, ResourceConfig> resourceConfigMap) {
    refreshJobContexts(accessor);
    // update workflow and job configs.
    _workflowConfigMap.clear();
    _jobConfigMap.clear();
    for (Map.Entry<String, ResourceConfig> entry : resourceConfigMap.entrySet()) {
      if (entry.getValue().getRecord().getSimpleFields()
          .containsKey(WorkflowConfig.WorkflowConfigProperty.Dag.name())) {
        _workflowConfigMap.put(entry.getKey(), new WorkflowConfig(entry.getValue()));
      } else if (entry.getValue().getRecord().getSimpleFields()
          .containsKey(WorkflowConfig.WorkflowConfigProperty.WorkflowID.name())) {
        _jobConfigMap.put(entry.getKey(), new JobConfig(entry.getValue()));
      }
    }
    return true;
  }

  private void refreshJobContexts(HelixDataAccessor accessor) {
    // TODO: Need an optimize for reading context only if the refresh is needed.
    long start = System.currentTimeMillis();
    _contextMap.clear();
    if (_clusterName == null) {
      return;
    }
    String path = String.format("/%s/%s%s", _clusterName, PropertyType.PROPERTYSTORE.name(),
        TaskConstants.REBALANCER_CONTEXT_ROOT);
    List<String> contextPaths = new ArrayList<>();
    List<String> childNames = accessor.getBaseDataAccessor().getChildNames(path, 0);
    if (childNames == null) {
      return;
    }
    for (String context : childNames) {
      contextPaths.add(Joiner.on("/").join(path, context, TaskConstants.CONTEXT_NODE));
    }

    List<ZNRecord> contexts = accessor.getBaseDataAccessor().get(contextPaths, null, 0);
    for (int i = 0; i < contexts.size(); i++) {
      ZNRecord context = contexts.get(i);
      if (context != null && context.getSimpleField(NAME) != null) {
        _contextMap.put(context.getSimpleField(NAME), context);
      } else {
        _contextMap.put(childNames.get(i), context);
        LogUtil.logDebug(LOG, getEventId(),
            String.format("Context for %s is null or miss the context NAME!", childNames.get((i))));
      }
    }

    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, getEventId(),
          "# of workflow/job context read from zk: " + _contextMap.size() + ". Take " + (
              System.currentTimeMillis() - start) + " ms");
    }
  }

  /**
   * Returns job config map
   * @return
   */
  public Map<String, JobConfig> getJobConfigMap() {
    return _jobConfigMap;
  }

  /**
   * Returns job config
   * @param resource
   * @return
   */
  public JobConfig getJobConfig(String resource) {
    return _jobConfigMap.get(resource);
  }

  /**
   * Returns workflow config map
   * @return
   */
  public Map<String, WorkflowConfig> getWorkflowConfigMap() {
    return _workflowConfigMap;
  }

  /**
   * Returns workflow config
   * @param resource
   * @return
   */
  public WorkflowConfig getWorkflowConfig(String resource) {
    return _workflowConfigMap.get(resource);
  }

  /**
   * Return the JobContext by resource name
   * @param resourceName
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
   * @param resourceName
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
    // Flush Context
    List<String> contextUpdatePaths = new ArrayList<>();
    List<ZNRecord> contextUpdateData = new ArrayList<>();
    // Do not update it if the is need to be remove
    _contextToUpdate.removeAll(_contextToRemove);
    List<String> contextUpdateNames = new ArrayList<>(_contextToUpdate);
    for (String resourceName : contextUpdateNames) {
      if (_contextMap.get(resourceName) != null) {
        contextUpdatePaths.add(getContextPath(resourceName));
        contextUpdateData.add(_contextMap.get(resourceName));
      }
    }

    boolean[] updateSuccess =
        accessor.getBaseDataAccessor().setChildren(contextUpdatePaths, contextUpdateData, AccessOption.PERSISTENT);

    for (int i = 0; i < updateSuccess.length; i++) {
      if (updateSuccess[i]) {
        _contextToUpdate.remove(contextUpdateNames.get(i));
      }
    }

    // Delete contexts
    // We can not leave the context here since some of the deletion happens for cleaning workflow
    // If we leave it in the memory, Helix will not allow user create it with same name.
    // TODO: Let's have periodical clean up thread that could remove deletion failed contexts.
    List<String> contextPathsToRemove = new ArrayList<>();
    List<String> contextNamesToRemove = new ArrayList<>(_contextToRemove);
    for (String resourceName : contextNamesToRemove) {
      contextPathsToRemove.add(getContextPath(resourceName));
    }

    // TODO: current behavior is when you delete non-existing data will return false.
    // Once the behavior fixed, we can add retry logic back. Otherwise, it will stay in memory and
    // not allow same workflow name recreation.
    accessor.getBaseDataAccessor().remove(contextPathsToRemove, AccessOption.PERSISTENT);

    _contextToRemove.clear();
  }

  /**
   * Return map of WorkflowContexts or JobContexts
   * @return
   */
  public Map<String, ZNRecord> getContexts() {
    return _contextMap;
  }

  /**
   * Returns the current AssignableInstanceManager instance.
   * @return
   */
  public AssignableInstanceManager getAssignableInstanceManager() {
    return _assignableInstanceManager;
  }

  /**
   * Remove Workflow or Job context from cache
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
    return "TaskDataCache{"
        + "_jobConfigMap=" + _jobConfigMap
        + ", _workflowConfigMap=" + _workflowConfigMap
        + ", _contextMap=" + _contextMap
        + ", _clusterName='" + _clusterName
        + '\'' + '}';
  }

  private String getContextPath(String resourceName) {
    return String.format("/%s/%s%s/%s/%s", _clusterName, PropertyType.PROPERTYSTORE.name(),
        TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, TaskConstants.CONTEXT_NODE);
  }
}