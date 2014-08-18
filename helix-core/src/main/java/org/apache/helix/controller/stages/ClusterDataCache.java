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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.context.ControllerContextHolder;
import org.apache.helix.model.ClusterConfiguration;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Reads the data from the cluster using data accessor. This output ClusterData which
 * provides useful methods to search/lookup properties
 */
public class ClusterDataCache {
  Map<String, LiveInstance> _liveInstanceMap;
  Map<String, LiveInstance> _liveInstanceCacheMap;
  Map<String, IdealState> _idealStateMap;
  Map<String, IdealState> _idealStateCacheMap;
  Map<String, StateModelDefinition> _stateModelDefMap;
  Map<String, InstanceConfig> _instanceConfigMap;
  Map<String, InstanceConfig> _instanceConfigCacheMap;
  Map<String, ClusterConstraints> _constraintMap;
  Map<String, ClusterConstraints> _constraintCacheMap;
  Map<String, Map<String, Map<String, CurrentState>>> _currentStateMap;
  Map<String, Map<String, Message>> _messageMap;
  Map<String, Map<String, String>> _idealStateRuleMap;
  Map<String, ResourceConfiguration> _resourceConfigMap;
  Map<String, ResourceConfiguration> _resourceConfigCacheMap;
  Map<String, ControllerContextHolder> _controllerContextMap;
  PauseSignal _pause;
  LiveInstance _leader;
  ClusterConfiguration _clusterConfig;
  boolean _writeAssignments;

  // maintain a cache of participant messages across pipeline runs
  Map<String, Map<String, Message>> _messageCache = Maps.newHashMap();

  boolean _init = true;

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
      _resourceConfigCacheMap = accessor.getChildValuesMap(keyBuilder.resourceConfigs());
      _constraintCacheMap = accessor.getChildValuesMap(keyBuilder.constraints());
    }
    _idealStateMap = Maps.newHashMap(_idealStateCacheMap);
    _liveInstanceMap = Maps.newHashMap(_liveInstanceCacheMap);
    _instanceConfigMap = Maps.newHashMap(_instanceConfigCacheMap);
    _resourceConfigMap = Maps.newHashMap(_resourceConfigCacheMap);
    _constraintMap = Maps.newHashMap(_constraintCacheMap);

    for (LiveInstance instance : _liveInstanceMap.values()) {
      LOG.trace("live instance: " + instance.getInstanceName() + " " + instance.getSessionId());
    }

    _stateModelDefMap = accessor.getChildValuesMap(keyBuilder.stateModelDefs());

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

    // New in 0.7: Read more information for the benefit of user-defined rebalancers
    _controllerContextMap = accessor.getChildValuesMap(keyBuilder.controllerContexts());

    // Read all single properties together
    List<HelixProperty> singleProperties =
        accessor.getProperty(ImmutableList.of(keyBuilder.clusterConfig(),
            keyBuilder.controllerLeader(), keyBuilder.pause()));
    _clusterConfig = (ClusterConfiguration) singleProperties.get(0);
    if (_clusterConfig != null) {
      _idealStateRuleMap = _clusterConfig.getIdealStateRules();
    } else {
      _idealStateRuleMap = Collections.emptyMap();
    }
    _leader = (LiveInstance) singleProperties.get(1);
    _pause = (PauseSignal) singleProperties.get(2);

    long endTime = System.currentTimeMillis();
    LOG.info("END: ClusterDataCache.refresh(), took " + (endTime - startTime) + " ms");

    if (LOG.isDebugEnabled()) {
      int numPaths =
          _liveInstanceMap.size() + _idealStateMap.size() + +_resourceConfigMap.size()
              + _stateModelDefMap.size() + _instanceConfigMap.size() + _constraintMap.size()
              + _controllerContextMap.size() + newMessageKeys.size() + currentStateKeys.size();
      LOG.debug("Paths read: " + numPaths);
    }

    _init = false;
    return true;
  }

  /**
   * Get the live instance associated with the controller leader
   * @return LiveInstance
   */
  public LiveInstance getLeader() {
    return _leader;
  }

  /**
   * Get the pause signal (if any)
   * @return PauseSignal
   */
  public PauseSignal getPauseSignal() {
    return _pause;
  }

  /**
   * Retrieves the configs for all resources
   * @return
   */
  public Map<String, ResourceConfiguration> getResourceConfigs() {
    return _resourceConfigMap;
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

  public synchronized void setLiveInstances(List<LiveInstance> liveInstances) {
    Map<String, LiveInstance> liveInstanceMap = Maps.newHashMap();
    for (LiveInstance liveInstance : liveInstances) {
      liveInstanceMap.put(liveInstance.getId(), liveInstance);
    }
    _liveInstanceCacheMap = liveInstanceMap;
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

  /**
   * Provides all outstanding messages
   * @return
   */
  public Map<String, Map<String, Message>> getMessageMap() {
    return _messageMap;
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

  public synchronized void setInstanceConfigs(List<InstanceConfig> instanceConfigs) {
    Map<String, InstanceConfig> instanceConfigMap = Maps.newHashMap();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      instanceConfigMap.put(instanceConfig.getId(), instanceConfig);
    }
    _instanceConfigCacheMap = instanceConfigMap;
  }

  public synchronized void setResourceConfigs(List<ResourceConfiguration> resourceConfigs) {
    Map<String, ResourceConfiguration> resourceConfigMap = Maps.newHashMap();
    for (ResourceConfiguration resourceConfig : resourceConfigs) {
      resourceConfigMap.put(resourceConfig.getId(), resourceConfig);
    }
    _resourceConfigCacheMap = resourceConfigMap;
  }

  public synchronized void setConstraints(List<ClusterConstraints> constraints) {
    Map<String, ClusterConstraints> constraintMap = Maps.newHashMap();
    for (ClusterConstraints constraint : constraints) {
      constraintMap.put(constraint.getId(), constraint);
    }
    _constraintCacheMap = constraintMap;
  }

  /**
   * Some partitions might be disabled on specific nodes.
   * This method allows one to fetch the set of nodes where a given partition is disabled
   * @param partition
   * @return
   */
  public Set<String> getDisabledInstancesForPartition(String partition) {
    Set<String> disabledInstancesSet = new HashSet<String>();
    for (String instance : _instanceConfigMap.keySet()) {
      InstanceConfig config = _instanceConfigMap.get(instance);
      if (config.getInstanceEnabled() == false
          || config.getInstanceEnabledForPartition(partition) == false) {
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
        if (replicasStr.equals(StateModelToken.ANY_LIVEINSTANCE.toString())) {
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

  public Map<String, ClusterConstraints> getConstraintMap() {
    return _constraintMap;
  }

  public Map<String, ControllerContextHolder> getContextMap() {
    return _controllerContextMap;
  }

  public ClusterConfiguration getClusterConfig() {
    return _clusterConfig;
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
   * Enable or disable writing resource assignments
   * @param enable true to enable, false to disable
   */
  public void setAssignmentWritePolicy(boolean enable) {
    _writeAssignments = enable;
  }

  /**
   * Check if writing resource assignments is enabled
   * @return true if enabled, false if disabled
   */
  public boolean assignmentWriteEnabled() {
    return _writeAssignments;
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
    sb.append("messageMap:" + _messageMap).append("\n");

    return sb.toString();
  }
}
