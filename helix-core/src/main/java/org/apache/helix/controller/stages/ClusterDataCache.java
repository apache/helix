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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
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
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskPartitionState;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Reads the data from the cluster using data accessor. This output ClusterData which
 * provides useful methods to search/lookup properties
 */
public class ClusterDataCache {

  private static final String IDEAL_STATE_RULE_PREFIX = "IdealStateRule!";
  private ClusterConfig _clusterConfig;
  Map<String, LiveInstance> _liveInstanceMap;
  Map<String, LiveInstance> _liveInstanceCacheMap;
  Map<String, IdealState> _idealStateMap;
  Map<String, IdealState> _idealStateCacheMap;
  Map<String, StateModelDefinition> _stateModelDefMap;
  Map<String, InstanceConfig> _instanceConfigMap;
  Map<String, InstanceConfig> _instanceConfigCacheMap;
  Map<String, Long> _instanceOfflineTimeMap;
  Map<String, ResourceConfig> _resourceConfigMap;
  Map<String, ResourceConfig> _resourceConfigCacheMap;
  Map<String, ClusterConstraints> _constraintMap;
  Map<String, Map<String, Map<String, CurrentState>>> _currentStateMap;
  Map<String, Map<String, Message>> _messageMap;
  Map<String, Map<String, String>> _idealStateRuleMap;

  // maintain a cache of participant messages across pipeline runs
  Map<String, Map<String, Message>> _messageCache = Maps.newHashMap();

  Map<String, Integer> _participantActiveTaskCount = new HashMap<String, Integer>();

  boolean _init = true;

  boolean _updateInstanceOfflineTime = true;

  private static final Logger LOG = Logger.getLogger(ClusterDataCache.class.getName());

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

    if (_init) {
      _idealStateCacheMap = accessor.getChildValuesMap(keyBuilder.idealStates());
      _liveInstanceCacheMap = accessor.getChildValuesMap(keyBuilder.liveInstances());
      _instanceConfigCacheMap = accessor.getChildValuesMap(keyBuilder.instanceConfigs());
    }
    _idealStateMap = Maps.newHashMap(_idealStateCacheMap);
    _liveInstanceMap = Maps.newHashMap(_liveInstanceCacheMap);
    _instanceConfigMap = Maps.newHashMap(_instanceConfigCacheMap);

    // TODO: We should listen on resource config change instead of fetching every time
    //       And add back resourceConfigCacheMap
    _resourceConfigMap = accessor.getChildValuesMap(keyBuilder.resourceConfigs());

    _stateModelDefMap = accessor.getChildValuesMap(keyBuilder.stateModelDefs());
    _constraintMap = accessor.getChildValuesMap(keyBuilder.constraints());

    if (_init || _updateInstanceOfflineTime) {
      updateOfflineInstanceHistory(accessor);
    }

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
        new HashMap<String, Map<String, Map<String, CurrentState>>>();
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
    }

    long endTime = System.currentTimeMillis();
    LOG.info("END: ClusterDataCache.refresh(), took " + (endTime - startTime) + " ms");

    if (LOG.isDebugEnabled()) {
      int numPaths = _liveInstanceMap.size() + _idealStateMap.size() + _stateModelDefMap.size()
          + _instanceConfigMap.size() + _resourceConfigMap.size() + _constraintMap.size()
          + newMessageKeys.size() + currentStateKeys.size();
      LOG.debug("Paths read: " + numPaths);
    }

    _init = false;
    return true;
  }

  public ClusterConfig getClusterConfig() {
    return _clusterConfig;
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
    List<String> offlineNodes = new ArrayList<String>(_instanceConfigMap.keySet());
    offlineNodes.removeAll(_liveInstanceMap.keySet());
    _instanceOfflineTimeMap = new HashMap<String, Long>();

    for(String instance : offlineNodes) {
      Builder keyBuilder = accessor.keyBuilder();
      PropertyKey propertyKey = keyBuilder.participantHistory(instance);
      ParticipantHistory history = accessor.getProperty(propertyKey);
      if (history == null) {
        history = new ParticipantHistory(instance);
      }
      if (history.getLastOfflineTime() == ParticipantHistory.ONLINE) {
        history.reportOffline();
        // persist history back to ZK.
        accessor.setProperty(propertyKey, history);
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
    Set<String> enabledLiveInstances = new HashSet<String>(getLiveInstances().keySet());
    enabledLiveInstances.removeAll(getDisabledInstances());

    return enabledLiveInstances;
  }

  /**
   * Return all nodes that are enabled.
   *
   * @return
   */
  public Set<String> getEnabledInstances() {
    Set<String> enabledNodes = new HashSet<String>(getInstanceConfigMap().keySet());
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
    Set<String> enabledLiveInstancesWithTag = new HashSet<String>(getLiveInstances().keySet());
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
    Set<String> taggedInstances = new HashSet<String>();
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
   * Returns the instance config map
   *
   * @return
   */
  public Map<String, ResourceConfig> getResourceConfigMap() {
    return _resourceConfigMap;
  }

  /**
   * Returns the instance config map
   *
   * @return
   */
  public ResourceConfig getResourceConfig(String resource) {
    return _resourceConfigMap.get(resource);
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
    Set<String> disabledInstancesSet = new HashSet<String>();
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

  public Integer getParticipantActiveTaskCount(String instance) {
    return _participantActiveTaskCount.get(instance);
  }

  public void setParticipantActiveTaskCount(String instance, int taskCount) {
    _participantActiveTaskCount.put(instance, taskCount);
  }

  /**
   * Reset RUNNING/INIT tasks count based on current state output
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

  private void fillActiveTaskCount(Map<String, Integer> additionPartitionMap, Map<String, Integer> partitionMap) {
    for (String participant : additionPartitionMap.keySet()) {
      partitionMap.put(participant, partitionMap.get(participant) + additionPartitionMap.get(participant));
    }
  }

  /**
   * Indicate that a full read should be done on the next refresh
   */
  public synchronized void requireFullRefresh() {
    _init = true;
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
}
