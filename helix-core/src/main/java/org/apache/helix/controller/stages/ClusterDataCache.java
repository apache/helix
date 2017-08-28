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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.apache.helix.HelixConstants.ChangeType;

/**
 * Reads the data from the cluster using data accessor. This output ClusterData which
 * provides useful methods to search/lookup properties
 */
public class ClusterDataCache {

  private static final String IDEAL_STATE_RULE_PREFIX = "IdealStateRule!";
  private static final String NAME = "NAME";

  private Map<ChangeType, Boolean> _propertyChangedMap;
  private ClusterConfig _clusterConfig;
  private Map<String, LiveInstance> _liveInstanceMap;
  private Map<String, LiveInstance> _liveInstanceCacheMap;
  private Map<String, IdealState> _idealStateMap;
  private Map<String, IdealState> _idealStateCacheMap;
  private Map<String, StateModelDefinition> _stateModelDefMap;
  private Map<String, InstanceConfig> _instanceConfigMap;
  private Map<String, InstanceConfig> _instanceConfigCacheMap;
  private Map<String, Long> _instanceOfflineTimeMap;
  private Map<String, ResourceConfig> _resourceConfigMap;
  private Map<String, ResourceConfig> _resourceConfigCacheMap;
  private Map<String, ClusterConstraints> _constraintMap;
  private Map<String, Map<String, Map<String, CurrentState>>> _currentStateMap;
  private Map<String, Map<String, Message>> _messageMap;
  private Map<String, Map<String, String>> _idealStateRuleMap;
  private Map<String, Map<String, Long>> _missingTopStateMap = new HashMap<>();
  private Map<String, JobConfig> _jobConfigMap = new HashMap<>();
  private Map<String, WorkflowConfig> _workflowConfigMap = new HashMap<>();
  private Map<String, ZNRecord> _contextMap = new HashMap<>();

  // maintain a cache of participant messages across pipeline runs
  Map<String, Map<String, Message>> _messageCache = Maps.newHashMap();

  Map<String, Integer> _participantActiveTaskCount = new HashMap<>();

  private ExecutorService _asyncTasksThreadPool;

  boolean _updateInstanceOfflineTime = true;
  boolean _isTaskCache;

  private String _clusterName;

  private static final Logger LOG = Logger.getLogger(ClusterDataCache.class.getName());


  public ClusterDataCache () {
    _propertyChangedMap = new ConcurrentHashMap<>();
    for(ChangeType type : ChangeType.values()) {
      _propertyChangedMap.put(type, Boolean.valueOf(true));
    }
  }

  public ClusterDataCache (String clusterName) {
    this();
    _clusterName = clusterName;
  }

  /**
   * This refreshes the cluster data by re-fetching the data from zookeeper in
   * an efficient way
   * @param accessor
   * @return
   */
  public synchronized boolean refresh(HelixDataAccessor accessor) {
    LOG.info("START: ClusterDataCache.refresh()");
    long startTime = System.currentTimeMillis();

    Builder keyBuilder = accessor.keyBuilder();

    if (_propertyChangedMap.get(ChangeType.IDEAL_STATE)) {
      _propertyChangedMap.put(ChangeType.IDEAL_STATE, Boolean.valueOf(false));
      _idealStateCacheMap = accessor.getChildValuesMap(keyBuilder.idealStates());
    }

    if (_propertyChangedMap.get(ChangeType.LIVE_INSTANCE)) {
      _propertyChangedMap.put(ChangeType.LIVE_INSTANCE, Boolean.valueOf(false));
      _liveInstanceCacheMap = accessor.getChildValuesMap(keyBuilder.liveInstances());
      _updateInstanceOfflineTime = true;
    }

    if (_propertyChangedMap.get(ChangeType.INSTANCE_CONFIG)) {
      _propertyChangedMap.put(ChangeType.INSTANCE_CONFIG, Boolean.valueOf(false));
      _instanceConfigCacheMap = accessor.getChildValuesMap(keyBuilder.instanceConfigs());
    }

    _idealStateMap = Maps.newHashMap(_idealStateCacheMap);
    _liveInstanceMap = Maps.newHashMap(_liveInstanceCacheMap);
    _instanceConfigMap = new ConcurrentHashMap<>(_instanceConfigCacheMap);

    if (_updateInstanceOfflineTime) {
      updateOfflineInstanceHistory(accessor);
    }

    // TODO: Need an optimize for reading context only if the refresh is needed.
    refreshContexts(accessor);

    // TODO: We should listen on resource config change instead of fetching every time
    //       And add back resourceConfigCacheMap
    refreshResourceConfigs(accessor);

    Map<String, StateModelDefinition> stateDefMap =
        accessor.getChildValuesMap(keyBuilder.stateModelDefs());
    _stateModelDefMap = new ConcurrentHashMap<>(stateDefMap);
    _constraintMap = accessor.getChildValuesMap(keyBuilder.constraints());

    if (LOG.isTraceEnabled()) {
      for (LiveInstance instance : _liveInstanceMap.values()) {
        LOG.trace("live instance: " + instance.getInstanceName() + " " + instance.getSessionId());
      }
    }

    Map<String, Map<String, Message>> msgMap = new HashMap<String, Map<String, Message>>();
    List<PropertyKey> newMessageKeys = Lists.newLinkedList();
    long purgeSum = 0;
    for (String instanceName : _liveInstanceMap.keySet()) {
      // get the cache
      Map<String, Message> cachedMap = _messageCache.get(instanceName);
      if (cachedMap == null) {
        cachedMap = Maps.newHashMap();
        _messageCache.put(instanceName, cachedMap);
      }
      msgMap.put(instanceName, cachedMap);

      // get the current names
      Set<String> messageNames =
          Sets.newHashSet(accessor.getChildNames(keyBuilder.messages(instanceName)));

      long purgeStart = System.currentTimeMillis();
      // clear stale names
      Iterator<String> cachedNamesIter = cachedMap.keySet().iterator();
      while (cachedNamesIter.hasNext()) {
        String messageName = cachedNamesIter.next();
        if (!messageNames.contains(messageName)) {
          cachedNamesIter.remove();
        }
      }
      long purgeEnd = System.currentTimeMillis();
      purgeSum += purgeEnd - purgeStart;

      // get the keys for the new messages
      for (String messageName : messageNames) {
        if (!cachedMap.containsKey(messageName)) {
          newMessageKeys.add(keyBuilder.message(instanceName, messageName));
        }
      }
    }

    // get the new messages
    if (newMessageKeys.size() > 0) {
      List<Message> newMessages = accessor.getProperty(newMessageKeys);
      for (Message message : newMessages) {
        if (message != null) {
          Map<String, Message> cachedMap = _messageCache.get(message.getTgtName());
          cachedMap.put(message.getId(), message);
        }
      }
    }
    _messageMap = Collections.unmodifiableMap(msgMap);
    LOG.debug("Purge took: " + purgeSum);

    List<PropertyKey> currentStateKeys = Lists.newLinkedList();
    Map<String, Map<String, Map<String, CurrentState>>> allCurStateMap =
        new HashMap<>();
    for (String instanceName : _liveInstanceMap.keySet()) {
      LiveInstance liveInstance = _liveInstanceMap.get(instanceName);
      String sessionId = liveInstance.getSessionId();
      List<String> currentStateNames =
          accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId));
      for (String currentStateName : currentStateNames) {
        currentStateKeys.add(keyBuilder.currentState(instanceName, sessionId, currentStateName));
      }

      // ensure an empty current state map for all live instances and sessions
      Map<String, Map<String, CurrentState>> instanceCurStateMap = allCurStateMap.get(instanceName);
      if (instanceCurStateMap == null) {
        instanceCurStateMap = Maps.newHashMap();
        allCurStateMap.put(instanceName, instanceCurStateMap);
      }
      Map<String, CurrentState> sessionCurStateMap = instanceCurStateMap.get(sessionId);
      if (sessionCurStateMap == null) {
        sessionCurStateMap = Maps.newHashMap();
        instanceCurStateMap.put(sessionId, sessionCurStateMap);
      }
    }
    List<CurrentState> currentStates = accessor.getProperty(currentStateKeys);
    Iterator<PropertyKey> csKeyIter = currentStateKeys.iterator();
    for (CurrentState currentState : currentStates) {
      PropertyKey key = csKeyIter.next();
      String[] params = key.getParams();
      if (currentState != null && params.length >= 4) {
        Map<String, Map<String, CurrentState>> instanceCurStateMap = allCurStateMap.get(params[1]);
        Map<String, CurrentState> sessionCurStateMap = instanceCurStateMap.get(params[2]);
        sessionCurStateMap.put(params[3], currentState);
      }
    }

    for (String instance : allCurStateMap.keySet()) {
      allCurStateMap.put(instance, Collections.unmodifiableMap(allCurStateMap.get(instance)));
    }
    _currentStateMap = Collections.unmodifiableMap(allCurStateMap);

    _idealStateRuleMap = Maps.newHashMap();
    _clusterConfig = accessor.getProperty(keyBuilder.clusterConfig());
    if (_clusterConfig != null) {
      for (String simpleKey : _clusterConfig.getRecord().getSimpleFields().keySet()) {
        if (simpleKey.startsWith(IDEAL_STATE_RULE_PREFIX)) {
          String simpleValue = _clusterConfig.getRecord().getSimpleField(simpleKey);
          String[] rules = simpleValue.split("(?<!\\\\),");
          Map<String, String> singleRule = Maps.newHashMap();
          for (String rule : rules) {
            String[] keyValue = rule.split("(?<!\\\\)=");
            if (keyValue.length >= 2) {
              singleRule.put(keyValue[0], keyValue[1]);
            }
          }
          _idealStateRuleMap.put(simpleKey, singleRule);
        }
      }
    } else {
      LOG.error("Cluster config is null!");
    }

    long endTime = System.currentTimeMillis();
    LOG.info(
        "END: ClusterDataCache.refresh() for cluster " + getClusterName() + ", took " + (endTime
            - startTime) + " ms");

    if (LOG.isDebugEnabled()) {
      int numPaths = _liveInstanceMap.size() + _idealStateMap.size() + _stateModelDefMap.size()
          + _instanceConfigMap.size() + _resourceConfigMap.size() + _constraintMap.size()
          + newMessageKeys.size() + currentStateKeys.size();
      LOG.debug("Paths read: " + numPaths);
    }

    return true;
  }

  public ClusterConfig getClusterConfig() {
    return _clusterConfig;
  }

  public void setClusterConfig(ClusterConfig clusterConfig) {
    _clusterConfig = clusterConfig;
  }

  public String getClusterName() {
    return _clusterConfig != null ? _clusterConfig.getClusterName() : _clusterName;
  }

  /**
   * Return the last offline time map for all offline instances.
   *
   * @return
   */
  public Map<String, Long> getInstanceOfflineTimeMap() {
    return _instanceOfflineTimeMap;
  }

  private void updateOfflineInstanceHistory(HelixDataAccessor accessor) {
    List<String> offlineNodes = new ArrayList<>(_instanceConfigMap.keySet());
    offlineNodes.removeAll(_liveInstanceMap.keySet());
    _instanceOfflineTimeMap = new HashMap<>();

    for (String instance : offlineNodes) {
      Builder keyBuilder = accessor.keyBuilder();
      PropertyKey propertyKey = keyBuilder.participantHistory(instance);
      ParticipantHistory history = accessor.getProperty(propertyKey);
      if (history == null) {
        history = new ParticipantHistory(instance);
      }
      if (history.getLastOfflineTime() == ParticipantHistory.ONLINE) {
        history.reportOffline();
        // persist history back to ZK.
        if (!accessor.setProperty(propertyKey, history)) {
          LOG.error("Fails to persist participant online history back to ZK!");
        }
      }
      _instanceOfflineTimeMap.put(instance, history.getLastOfflineTime());
    }
    _updateInstanceOfflineTime = false;
  }

  /**
   * Retrieves the idealstates for all resources
   * @return
   */
  public Map<String, IdealState> getIdealStates() {
    return _idealStateMap;
  }

  public synchronized void setIdealStates(List<IdealState> idealStates) {
    Map<String, IdealState> idealStateMap = Maps.newHashMap();
    for (IdealState idealState : idealStates) {
      idealStateMap.put(idealState.getId(), idealState);
    }
    _idealStateCacheMap = idealStateMap;
  }

  public Map<String, Map<String, String>> getIdealStateRules() {
    return _idealStateRuleMap;
  }

  /**
   * Returns the LiveInstances for each of the instances that are curretnly up and running
   * @return
   */
  public Map<String, LiveInstance> getLiveInstances() {
    return _liveInstanceMap;
  }


  /**
   * Return the set of all instances names.
   */
  public Set<String> getAllInstances() {
    return new HashSet<String>(_instanceConfigMap.keySet());
  }

  /**
   * Return all the live nodes that are enabled
   *
   * @return A new set contains live instance name and that are marked enabled
   */
  public Set<String> getEnabledLiveInstances() {
    Set<String> enabledLiveInstances = new HashSet<>(getLiveInstances().keySet());
    enabledLiveInstances.removeAll(getDisabledInstances());

    return enabledLiveInstances;
  }

  /**
   * Return all nodes that are enabled.
   *
   * @return
   */
  public Set<String> getEnabledInstances() {
    Set<String> enabledNodes = new HashSet<>(getInstanceConfigMap().keySet());
    enabledNodes.removeAll(getDisabledInstances());

    return enabledNodes;
  }

  /**
   * Return all the live nodes that are enabled and tagged with given instanceTag.
   *
   * @param instanceTag The instance group tag.
   * @return A new set contains live instance name and that are marked enabled and have the specified
   * tag.
   */
  public Set<String> getEnabledLiveInstancesWithTag(String instanceTag) {
    Set<String> enabledLiveInstancesWithTag = new HashSet<>(getLiveInstances().keySet());
    Set<String> instancesWithTag = getInstancesWithTag(instanceTag);
    enabledLiveInstancesWithTag.retainAll(instancesWithTag);
    enabledLiveInstancesWithTag.removeAll(getDisabledInstances());

    return enabledLiveInstancesWithTag;
  }

  /**
   * Return all the nodes that are tagged with given instance tag.
   *
   * @param instanceTag The instance group tag.
   */
  public Set<String> getInstancesWithTag(String instanceTag) {
    Set<String> taggedInstances = new HashSet<>();
    for (String instance : _instanceConfigMap.keySet()) {
      InstanceConfig instanceConfig = _instanceConfigMap.get(instance);
      if (instanceConfig != null && instanceConfig.containsTag(instanceTag)) {
        taggedInstances.add(instance);
      }
    }

    return taggedInstances;
  }


  public synchronized void setLiveInstances(List<LiveInstance> liveInstances) {
    Map<String, LiveInstance> liveInstanceMap = Maps.newHashMap();
    for (LiveInstance liveInstance : liveInstances) {
      liveInstanceMap.put(liveInstance.getId(), liveInstance);
    }
    _liveInstanceCacheMap = liveInstanceMap;
    _updateInstanceOfflineTime = true;
  }

  /**
   * Provides the current state of the node for a given session id,
   * the sessionid can be got from LiveInstance
   * @param instanceName
   * @param clientSessionId
   * @return
   */
  public Map<String, CurrentState> getCurrentState(String instanceName, String clientSessionId) {
    if (!_currentStateMap.containsKey(instanceName)
        || !_currentStateMap.get(instanceName).containsKey(clientSessionId)) {
      return Collections.emptyMap();
    }
    return _currentStateMap.get(instanceName).get(clientSessionId);
  }

  /**
   * Provides a list of current outstanding transitions on a given instance.
   * @param instanceName
   * @return
   */
  public Map<String, Message> getMessages(String instanceName) {
    Map<String, Message> map = _messageMap.get(instanceName);
    if (map != null) {
      return map;
    } else {
      return Collections.emptyMap();
    }
  }

  public void cacheMessages(List<Message> messages) {
    for (Message message : messages) {
      String instanceName = message.getTgtName();
      Map<String, Message> instMsgMap = null;
      if (_messageCache.containsKey(instanceName)) {
        instMsgMap = _messageCache.get(instanceName);
      } else {
        instMsgMap = Maps.newHashMap();
        _messageCache.put(instanceName, instMsgMap);
      }
      instMsgMap.put(message.getId(), message);
    }
  }

  /**
   * Provides the state model definition for a given state model
   * @param stateModelDefRef
   * @return
   */
  public StateModelDefinition getStateModelDef(String stateModelDefRef) {
    if (stateModelDefRef == null) {
      return null;
    }
    return _stateModelDefMap.get(stateModelDefRef);
  }

  /**
   * Provides all state model definitions
   * @return state model definition map
   */
  public Map<String, StateModelDefinition> getStateModelDefMap() {
    return _stateModelDefMap;
  }

  /**
   * Provides the idealstate for a given resource
   * @param resourceName
   * @return
   */
  public IdealState getIdealState(String resourceName) {
    return _idealStateMap.get(resourceName);
  }

  /**
   * Returns the instance config map
   * @return
   */
  public Map<String, InstanceConfig> getInstanceConfigMap() {
    return _instanceConfigMap;
  }

  /**
   * Set the instance config map
   * @param instanceConfigMap
   */
  public void setInstanceConfigMap(Map<String, InstanceConfig> instanceConfigMap) {
    _instanceConfigMap = instanceConfigMap;
  }

  /**
   * Returns the resource config map
   *
   * @return
   */
  public Map<String, ResourceConfig> getResourceConfigMap() {
    return _resourceConfigMap;
  }

  /**
   * Returns the resource config
   * Notify the cache that some part of the cluster data has been changed.
   */
  public void updateDataChange(ChangeType changeType) {
    _propertyChangedMap.put(changeType, Boolean.valueOf(true));
  }

  /**
   * Returns the instance config map
   *
   * @return
   */
  public ResourceConfig getResourceConfig(String resource) {
    return _resourceConfigMap.get(resource);
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


  public synchronized void setInstanceConfigs(List<InstanceConfig> instanceConfigs) {
    Map<String, InstanceConfig> instanceConfigMap = Maps.newHashMap();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      instanceConfigMap.put(instanceConfig.getId(), instanceConfig);
    }
    _instanceConfigCacheMap = instanceConfigMap;
  }

  /**
   * Some partitions might be disabled on specific nodes.
   * This method allows one to fetch the set of nodes where a given partition is disabled
   * @param partition
   * @return
   */
  public Set<String> getDisabledInstancesForPartition(String resource, String partition) {
    Set<String> disabledInstancesSet = new HashSet<String>();
    for (String instance : _instanceConfigMap.keySet()) {
      InstanceConfig config = _instanceConfigMap.get(instance);
      if (config.getInstanceEnabled() == false
          || config.getInstanceEnabledForPartition(resource, partition) == false) {
        disabledInstancesSet.add(instance);
      }
    }
    return disabledInstancesSet;
  }

  /**
   * This method allows one to fetch the set of nodes that are disabled
   *
   * @return
   */
  public Set<String> getDisabledInstances() {
    Set<String> disabledInstancesSet = new HashSet<>();
    for (String instance : _instanceConfigMap.keySet()) {
      InstanceConfig config = _instanceConfigMap.get(instance);
      if (!config.getInstanceEnabled()) {
        disabledInstancesSet.add(instance);
      }
    }
    return disabledInstancesSet;
  }

  /**
   * Returns the number of replicas for a given resource.
   * @param resourceName
   * @return
   */
  public int getReplicas(String resourceName) {
    int replicas = -1;

    if (_idealStateMap.containsKey(resourceName)) {
      String replicasStr = _idealStateMap.get(resourceName).getReplicas();

      if (replicasStr != null) {
        if (replicasStr.equals(IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString())) {
          replicas = _liveInstanceMap.size();
        } else {
          try {
            replicas = Integer.parseInt(replicasStr);
          } catch (Exception e) {
            LOG.error("invalid replicas string: " + replicasStr);
          }
        }
      } else {
        LOG.error("idealState for resource: " + resourceName + " does NOT have replicas");
      }
    }
    return replicas;
  }

  /**
   * Returns the ClusterConstraints for a given constraintType
   * @param type
   * @return
   */
  public ClusterConstraints getConstraint(ConstraintType type) {
    if (_constraintMap != null) {
      return _constraintMap.get(type.toString());
    }
    return null;
  }

  public Map<String, Map<String, Long>> getMissingTopStateMap() {
    return _missingTopStateMap;
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
    fillActiveTaskCount(currentStateOutput.getPartitionCountWithPendingState(TaskConstants.STATE_MODEL_NAME,
        TaskPartitionState.INIT.name()), _participantActiveTaskCount);
    fillActiveTaskCount(currentStateOutput.getPartitionCountWithPendingState(TaskConstants.STATE_MODEL_NAME,
        TaskPartitionState.RUNNING.name()), _participantActiveTaskCount);
    fillActiveTaskCount(currentStateOutput.getPartitionCountWithCurrentState(TaskConstants.STATE_MODEL_NAME,
        TaskPartitionState.INIT.name()), _participantActiveTaskCount);
    fillActiveTaskCount(currentStateOutput.getPartitionCountWithCurrentState(TaskConstants.STATE_MODEL_NAME,
        TaskPartitionState.RUNNING.name()), _participantActiveTaskCount);
  }

  private void fillActiveTaskCount(Map<String, Integer> additionPartitionMap,
      Map<String, Integer> partitionMap) {
    for (String participant : additionPartitionMap.keySet()) {
      partitionMap.put(participant, partitionMap.get(participant) + additionPartitionMap.get(participant));
    }
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
  public void updateJobContext(String resourceName, JobContext jobContext,
      HelixDataAccessor accessor) {
    updateContext(resourceName, jobContext.getRecord(), accessor);
  }

  /**
   * Update context of the Workflow
   */
  public void updateWorkflowContext(String resourceName, WorkflowContext workflowContext,
      HelixDataAccessor accessor) {
    updateContext(resourceName, workflowContext.getRecord(), accessor);
  }

  /**
   * Update context of the Workflow or Job
   */
  private void updateContext(String resourceName, ZNRecord record, HelixDataAccessor accessor) {
    String path = String.format("/%s/%s%s/%s/%s", _clusterName, PropertyType.PROPERTYSTORE.name(),
        TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, TaskConstants.CONTEXT_NODE);
    accessor.getBaseDataAccessor().set(path, record, AccessOption.PERSISTENT);
    _contextMap.put(resourceName, record);
  }

  /**
   * Return map of WorkflowContexts or JobContexts
   * @return
   */
  public Map<String, ZNRecord> getContexts() {
    return _contextMap;
  }

  /**
   * Indicate that a full read should be done on the next refresh
   */
  public synchronized void requireFullRefresh() {
    for(ChangeType type : ChangeType.values()) {
      _propertyChangedMap.put(type, Boolean.valueOf(true));
    }
  }

  /**
   * Get async update thread pool
   * @return
   */
  public ExecutorService getAsyncTasksThreadPool() {
    return _asyncTasksThreadPool;
  }

  /**
   * Set async update thread pool
   * @param asyncTasksThreadPool
   */
  public void setAsyncTasksThreadPool(ExecutorService asyncTasksThreadPool) {
    _asyncTasksThreadPool = asyncTasksThreadPool;
  }

  /**
   * Set the cache is serving for Task pipleline or not
   * @param taskCache
   */
  public void setTaskCache(boolean taskCache) {
    _isTaskCache = taskCache;
  }

  /**
   * Get the cache is serving for Task pipleline or not
   * @return
   */
  public boolean isTaskCache() {
    return _isTaskCache;
  }

  /**
   * toString method to print the entire cluster state
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("liveInstaceMap:" + _liveInstanceMap).append("\n");
    sb.append("idealStateMap:" + _idealStateMap).append("\n");
    sb.append("stateModelDefMap:" + _stateModelDefMap).append("\n");
    sb.append("instanceConfigMap:" + _instanceConfigMap).append("\n");
    sb.append("resourceConfigMap:" + _resourceConfigMap).append("\n");
    sb.append("messageMap:" + _messageMap).append("\n");

    return sb.toString();
  }

  private void refreshContexts(HelixDataAccessor accessor) {
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
        LOG.info(
            String.format("Context for %s is null or miss the context NAME!", childNames.get((i))));
      }
    }
  }

  private void refreshResourceConfigs(HelixDataAccessor accessor) {
    _workflowConfigMap.clear();
    _jobConfigMap.clear();
    _resourceConfigMap = accessor.getChildValuesMap(accessor.keyBuilder().resourceConfigs());
    for (Map.Entry<String, ResourceConfig> entry : _resourceConfigMap.entrySet()) {
      if (entry.getValue().getRecord().getSimpleFields()
          .containsKey(WorkflowConfig.WorkflowConfigProperty.Dag.name())) {
        _workflowConfigMap.put(entry.getKey(), new WorkflowConfig(entry.getValue()));
      } else if (entry.getValue().getRecord().getSimpleFields()
          .containsKey(WorkflowConfig.WorkflowConfigProperty.WorkflowID.name())) {
        _jobConfigMap.put(entry.getKey(), new JobConfig(entry.getValue()));
      }
    }
  }
}
