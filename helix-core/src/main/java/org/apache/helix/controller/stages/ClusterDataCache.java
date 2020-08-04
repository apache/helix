package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.common.caches.CurrentStateCache;
import org.apache.helix.common.caches.IdealStateCache;
import org.apache.helix.common.caches.InstanceMessagesCache;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.HelixConstants.ChangeType;

/**
 * **Only left here for backward-compatibility. Cache has been replaced by the DataProvider
 * interface**
 * Reads the data from the cluster using data accessor. This output ClusterData which
 * provides useful methods to search/lookup properties
 */
@Deprecated
public class ClusterDataCache extends ResourceControllerDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterDataCache.class.getName());

  private Map<String, LiveInstance> _liveInstanceMap;
  private Map<String, InstanceConfig> _instanceConfigCacheMap;
  private String _eventId = "NO_ID";

  private IdealStateCache _idealStateCache;
  private CurrentStateCache _currentStateCache;
  private TaskDataCache _taskDataCache;
  private InstanceMessagesCache _instanceMessagesCache;

  private Map<ChangeType, Boolean> _propertyDataChangedMap;

  boolean _isTaskCache;
  private String _clusterName;

  @Deprecated
  public ClusterDataCache() {
    this(null);
  }

  @Deprecated
  public ClusterDataCache(String clusterName) {
    _propertyDataChangedMap = new ConcurrentHashMap<>();
    for (ChangeType type : ChangeType.values()) {
      _propertyDataChangedMap.put(type, true);
    }
    _clusterName = clusterName;
    _idealStateCache = new IdealStateCache(_clusterName);
    _currentStateCache = new CurrentStateCache(_clusterName);
    _taskDataCache = new TaskDataCache(_clusterName);
    _instanceMessagesCache = new InstanceMessagesCache(_clusterName);
  }

  /**
   * This refresh() has been instrumented so that any previous usage of ClusterDataCache would
   * continue to work. This is only left here for backward-compatibility.
   * @param accessor
   */
  @Override
  @Deprecated
  public synchronized void refresh(HelixDataAccessor accessor) {
    super.refresh(accessor);
    if (_isTaskCache) {
      // Refresh TaskCache
      _taskDataCache.refresh(accessor, getResourceConfigMap());
    }
  }

  /**
   * Set the cache is serving for Task pipeline or not
   * @param taskCache
   */
  @Deprecated
  public void setTaskCache(boolean taskCache) {
    _isTaskCache = taskCache;
  }

  /**
   * Get the cache is serving for Task pipeline or not
   * @return
   */
  @Deprecated
  public boolean isTaskCache() {
    return _isTaskCache;
  }

  /**
   * Returns job config
   * @param resource
   * @return
   */
  @Deprecated
  public JobConfig getJobConfig(String resource) {
    return _taskDataCache.getJobConfig(resource);
  }

  /**
   * Returns workflow config
   * @param resource
   * @return
   */
  @Deprecated
  public WorkflowConfig getWorkflowConfig(String resource) {
    return _taskDataCache.getWorkflowConfig(resource);
  }

  @Deprecated
  public synchronized void setInstanceConfigs(List<InstanceConfig> instanceConfigs) {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      instanceConfigMap.put(instanceConfig.getId(), instanceConfig);
    }
    _instanceConfigCacheMap = instanceConfigMap;
  }

  /**
   * Returns the number of replicas for a given resource.
   * @param resourceName
   * @return
   */
  @Deprecated
  public int getReplicas(String resourceName) {
    int replicas = -1;
    Map<String, IdealState> idealStateMap = _idealStateCache.getIdealStateMap();

    if (idealStateMap.containsKey(resourceName)) {
      String replicasStr = idealStateMap.get(resourceName).getReplicas();

      if (replicasStr != null) {
        if (replicasStr.equals(IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString())) {
          replicas = _liveInstanceMap.size();
        } else {
          try {
            replicas = Integer.parseInt(replicasStr);
          } catch (Exception e) {
            LogUtil.logError(LOG, _eventId, "invalid replicas string: " + replicasStr + " for "
                + (_isTaskCache ? "TASK" : "DEFAULT") + "pipeline");
          }
        }
      } else {
        LogUtil.logError(LOG, _eventId, "idealState for resource: " + resourceName
            + " does NOT have replicas for " + (_isTaskCache ? "TASK" : "DEFAULT") + "pipeline");
      }
    }
    return replicas;
  }

  /**
   * Return the JobContext by resource name
   * @param resourceName
   * @return
   */
  @Deprecated
  public JobContext getJobContext(String resourceName) {
    return _taskDataCache.getJobContext(resourceName);
  }

  /**
   * Return the WorkflowContext by resource name
   * @param resourceName
   * @return
   */
  @Deprecated
  public WorkflowContext getWorkflowContext(String resourceName) {
    return _taskDataCache.getWorkflowContext(resourceName);
  }

  /**
   * Return map of WorkflowContexts or JobContexts
   * @return
   */
  @Deprecated
  public Map<String, ZNRecord> getContexts() {
    return _taskDataCache.getContexts();
  }

  @Deprecated
  public String getEventId() {
    return _eventId;
  }

  @Deprecated
  public void setEventId(String eventId) {
    _eventId = eventId;
  }
}
