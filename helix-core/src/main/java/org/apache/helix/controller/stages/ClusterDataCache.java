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
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * Reads the data from the cluster using data accessor. This output ClusterData which
 * provides useful methods to search/lookup properties
 */
@Deprecated
public class ClusterDataCache {

  Map<String, LiveInstance> _liveInstanceMap;
  Map<String, IdealState> _idealStateMap;
  Map<String, StateModelDefinition> _stateModelDefMap;
  Map<String, InstanceConfig> _instanceConfigMap;
  Map<String, ClusterConstraints> _constraintMap;
  Map<String, Map<String, Map<String, CurrentState>>> _currentStateMap;
  Map<String, Map<String, Message>> _messageMap;

  // Map<String, Map<String, HealthStat>> _healthStatMap;
  // private HealthStat _globalStats; // DON'T THINK I WILL USE THIS ANYMORE
  // private PersistentStats _persistentStats;
  // private Alerts _alerts;
  // private AlertStatus _alertStatus;

  private static final Logger LOG = Logger.getLogger(ClusterDataCache.class.getName());

  /**
   * This refreshes the cluster data by re-fetching the data from zookeeper in
   * an efficient way
   * @param accessor
   * @return
   */
  public boolean refresh(HelixDataAccessor accessor) {
    Builder keyBuilder = accessor.keyBuilder();
    _idealStateMap = accessor.getChildValuesMap(keyBuilder.idealStates());
    _liveInstanceMap = accessor.getChildValuesMap(keyBuilder.liveInstances());

    for (LiveInstance instance : _liveInstanceMap.values()) {
      LOG.trace("live instance: " + instance.getParticipantId() + " " + instance.getTypedSessionId());
    }

    _stateModelDefMap = accessor.getChildValuesMap(keyBuilder.stateModelDefs());
    _instanceConfigMap = accessor.getChildValuesMap(keyBuilder.instanceConfigs());
    _constraintMap = accessor.getChildValuesMap(keyBuilder.constraints());

    Map<String, Map<String, Message>> msgMap = new HashMap<String, Map<String, Message>>();
    for (String instanceName : _liveInstanceMap.keySet()) {
      Map<String, Message> map = accessor.getChildValuesMap(keyBuilder.messages(instanceName));
      msgMap.put(instanceName, map);
    }
    _messageMap = Collections.unmodifiableMap(msgMap);

    Map<String, Map<String, Map<String, CurrentState>>> allCurStateMap =
        new HashMap<String, Map<String, Map<String, CurrentState>>>();
    for (String instanceName : _liveInstanceMap.keySet()) {
      LiveInstance liveInstance = _liveInstanceMap.get(instanceName);
      String sessionId = liveInstance.getTypedSessionId().stringify();
      if (!allCurStateMap.containsKey(instanceName)) {
        allCurStateMap.put(instanceName, new HashMap<String, Map<String, CurrentState>>());
      }
      Map<String, Map<String, CurrentState>> curStateMap = allCurStateMap.get(instanceName);
      Map<String, CurrentState> map =
          accessor.getChildValuesMap(keyBuilder.currentStates(instanceName, sessionId));
      curStateMap.put(sessionId, map);
    }

    for (String instance : allCurStateMap.keySet()) {
      allCurStateMap.put(instance, Collections.unmodifiableMap(allCurStateMap.get(instance)));
    }
    _currentStateMap = Collections.unmodifiableMap(allCurStateMap);

    return true;
  }

  /**
   * Retrieves the idealstates for all resources
   * @return
   */
  public Map<String, IdealState> getIdealStates() {
    return _idealStateMap;
  }

  /**
   * Returns the LiveInstances for each of the instances that are curretnly up and running
   * @return
   */
  public Map<String, LiveInstance> getLiveInstances() {
    return _liveInstanceMap;
  }

  /**
   * Provides the current state of the node for a given session id,
   * the sessionid can be got from LiveInstance
   * @param instanceName
   * @param clientSessionId
   * @return
   */
  public Map<String, CurrentState> getCurrentState(String instanceName, String clientSessionId) {
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

  // public HealthStat getGlobalStats()
  // {
  // return _globalStats;
  // }
  //
  // public PersistentStats getPersistentStats()
  // {
  // return _persistentStats;
  // }
  //
  // public Alerts getAlerts()
  // {
  // return _alerts;
  // }
  //
  // public AlertStatus getAlertStatus()
  // {
  // return _alertStatus;
  // }
  //
  // public Map<String, HealthStat> getHealthStats(String instanceName)
  // {
  // Map<String, HealthStat> map = _healthStatMap.get(instanceName);
  // if (map != null)
  // {
  // return map;
  // } else
  // {
  // return Collections.emptyMap();
  // }
  // }
  /**
   * Provides the state model definition for a given state model
   * @param stateModelDefRef
   * @return
   */
  public StateModelDefinition getStateModelDef(String stateModelDefRef) {

    return _stateModelDefMap.get(stateModelDefRef);
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
