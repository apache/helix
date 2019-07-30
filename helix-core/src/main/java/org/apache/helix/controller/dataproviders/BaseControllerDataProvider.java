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

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.caches.AbstractDataCache;
import org.apache.helix.common.caches.CurrentStateCache;
import org.apache.helix.common.caches.InstanceMessagesCache;
import org.apache.helix.common.caches.PropertyCache;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Common building block for controller to cache their data. This common building block contains
 * information about cluster config, instance config, resource config, ideal states, current state,
 * live instances, cluster constraint, state model definition.
 *
 * This class will be moved to helix-common module in the future
 */
public class BaseControllerDataProvider implements ControlContextProvider {
  private static final Logger logger =
      LoggerFactory.getLogger(BaseControllerDataProvider.class);

  // We only refresh EV and TEV the very first time the cluster data cache is initialized
  private static final List<HelixConstants.ChangeType> _noFullRefreshProperty = Arrays
      .asList(HelixConstants.ChangeType.EXTERNAL_VIEW,
          HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW);

  private String _clusterName = AbstractDataCache.UNKNOWN_CLUSTER;
  private String _pipelineName = AbstractDataCache.UNKNOWN_PIPELINE;
  private String _clusterEventId = AbstractDataCache.UNKNOWN_EVENT_ID;
  private ClusterConfig _clusterConfig;

  private boolean _updateInstanceOfflineTime = true;
  private MaintenanceSignal _maintenanceSignal;
  private boolean _isMaintenanceModeEnabled;
  private boolean _hasMaintenanceSignalChanged;
  private ExecutorService _asyncTasksThreadPool;

  // A map recording what data has changed
  protected Map<HelixConstants.ChangeType, AtomicBoolean> _propertyDataChangedMap;

  // Property caches
  private final PropertyCache<ResourceConfig> _resourceConfigCache;
  private final PropertyCache<InstanceConfig> _instanceConfigCache;
  private final PropertyCache<LiveInstance> _liveInstanceCache;
  private final PropertyCache<IdealState> _idealStateCache;
  private final PropertyCache<ClusterConstraints> _clusterConstraintsCache;
  private final PropertyCache<StateModelDefinition> _stateModelDefinitionCache;

  // Special caches
  private CurrentStateCache _currentStateCache;
  private InstanceMessagesCache _instanceMessagesCache;

  // Other miscellaneous caches
  private Map<String, Long> _instanceOfflineTimeMap;
  private Map<String, Map<String, String>> _idealStateRuleMap;
  private Map<String, Map<String, Set<String>>> _disabledInstanceForPartitionMap = new HashMap<>();
  private Set<String> _disabledInstanceSet = new HashSet<>();

  public BaseControllerDataProvider() {
    this(AbstractDataCache.UNKNOWN_CLUSTER, AbstractDataCache.UNKNOWN_PIPELINE);
  }

  public BaseControllerDataProvider(String clusterName, String pipelineName) {
    _clusterName = clusterName;
    _pipelineName = pipelineName;
    _propertyDataChangedMap = new ConcurrentHashMap<>();
    for (HelixConstants.ChangeType type : HelixConstants.ChangeType.values()) {
      // refresh every type when it is initialized
      _propertyDataChangedMap
          .put(type, new AtomicBoolean(true));
    }

    // initialize caches
    _resourceConfigCache = new PropertyCache<>(this, "ResourceConfig", new PropertyCache.PropertyCacheKeyFuncs<ResourceConfig>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().resourceConfigs();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().resourceConfig(objName);
      }

      @Override
      public String getObjName(ResourceConfig obj) {
        return obj.getResourceName();
      }
    }, true);
    _liveInstanceCache = new PropertyCache<>(this, "LiveInstance", new PropertyCache.PropertyCacheKeyFuncs<LiveInstance>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().liveInstances();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().liveInstance(objName);
      }

      @Override
      public String getObjName(LiveInstance obj) {
        return obj.getInstanceName();
      }
    }, true);
    _instanceConfigCache = new PropertyCache<>(this, "InstanceConfig", new PropertyCache.PropertyCacheKeyFuncs<InstanceConfig>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().instanceConfigs();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().instanceConfig(objName);
      }

      @Override
      public String getObjName(InstanceConfig obj) {
        return obj.getInstanceName();
      }
    }, true);
    _idealStateCache = new PropertyCache<>(this, "IdealState", new PropertyCache.PropertyCacheKeyFuncs<IdealState>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().idealStates();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().idealStates(objName);
      }

      @Override
      public String getObjName(IdealState obj) {
        return obj.getResourceName();
      }
    }, true);
    _clusterConstraintsCache = new PropertyCache<>(this, "ClusterConstraint", new PropertyCache.PropertyCacheKeyFuncs<ClusterConstraints>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().constraints();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().constraint(objName);
      }

      @Override
      public String getObjName(ClusterConstraints obj) {
        // We set constraint type to the HelixProperty id
        return obj.getId();
      }
    }, false);
    _stateModelDefinitionCache = new PropertyCache<>(this, "StateModelDefinition", new PropertyCache.PropertyCacheKeyFuncs<StateModelDefinition>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().stateModelDefs();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().stateModelDef(objName);
      }

      @Override
      public String getObjName(StateModelDefinition obj) {
        return obj.getId();
      }
    }, false);
    _currentStateCache = new CurrentStateCache(this);
    _instanceMessagesCache = new InstanceMessagesCache(_clusterName);
  }

  private void refreshIdealState(final HelixDataAccessor accessor,
      Set<HelixConstants.ChangeType> refreshedType) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.IDEAL_STATE).getAndSet(false)) {
      _idealStateCache.refresh(accessor);
      refreshedType.add(HelixConstants.ChangeType.IDEAL_STATE);
    } else {
      LogUtil.logInfo(logger, getClusterEventId(), String
          .format("No ideal state change for %s cluster, %s pipeline", _clusterName,
              getPipelineName()));
    }
  }

  private void refreshLiveInstances(final HelixDataAccessor accessor,
      Set<HelixConstants.ChangeType> refreshedType) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.LIVE_INSTANCE).getAndSet(false)) {
      _liveInstanceCache.refresh(accessor);
      _updateInstanceOfflineTime = true;
      refreshedType.add(HelixConstants.ChangeType.LIVE_INSTANCE);
    } else {
      LogUtil.logInfo(logger, getClusterEventId(), String
          .format("No live instance change for %s cluster, %s pipeline", _clusterName,
              getPipelineName()));
    }
  }

  private void refreshInstanceConfigs(final HelixDataAccessor accessor,
      Set<HelixConstants.ChangeType> refreshedType) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.INSTANCE_CONFIG).getAndSet(false)) {
      _instanceConfigCache.refresh(accessor);
      LogUtil.logInfo(logger, getClusterEventId(), String
          .format("Reloaded InstanceConfig for cluster %s, %s pipeline. Keys: %s", _clusterName,
              getPipelineName(), _instanceConfigCache.getPropertyMap().keySet()));
      refreshedType.add(HelixConstants.ChangeType.INSTANCE_CONFIG);
    } else {
      LogUtil.logInfo(logger, getClusterEventId(), String
          .format("No instance config change for %s cluster, %s pipeline", _clusterName,
              getPipelineName()));
    }
  }

  private void refreshResourceConfig(final HelixDataAccessor accessor,
      Set<HelixConstants.ChangeType> refreshedType) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.RESOURCE_CONFIG).getAndSet(false)) {
      _resourceConfigCache.refresh(accessor);
      LogUtil.logInfo(logger, getClusterEventId(), String
          .format("Reloaded ResourceConfig for cluster %s, %s pipeline. Cnt: %s", _clusterName,
              getPipelineName(), _resourceConfigCache.getPropertyMap().keySet().size()));
      refreshedType.add(HelixConstants.ChangeType.RESOURCE_CONFIG);
    } else {
      LogUtil.logInfo(logger, getClusterEventId(), String
          .format("No resource config change for %s cluster, %s pipeline", _clusterName,
              getPipelineName()));
    }
  }

  private void updateMaintenanceInfo(final HelixDataAccessor accessor) {
    _maintenanceSignal = accessor.getProperty(accessor.keyBuilder().maintenance());
    _isMaintenanceModeEnabled = _maintenanceSignal != null;
    // The following flag is to guarantee that there's only one update per pineline run because we
    // check for whether maintenance recovery could happen twice every pipeline
    _hasMaintenanceSignalChanged = false;
  }

  private void updateIdealRuleMap() {
    // Assumes cluster config is up-to-date
    if (_clusterConfig != null) {
      _idealStateRuleMap = _clusterConfig.getIdealStateRules();
    } else {
      _idealStateRuleMap = new HashMap<>();
      LogUtil.logWarn(logger, getClusterEventId(), String
          .format("Cluster config is null for %s cluster, %s pipeline", getClusterName(),
              getPipelineName()));
    }
  }

  public synchronized void refresh(HelixDataAccessor accessor) {
    doRefresh(accessor);
  }

  /**
   * @param accessor
   * @return The types that has been updated during the refresh.
   */
  protected synchronized Set<HelixConstants.ChangeType> doRefresh(HelixDataAccessor accessor) {
    Set<HelixConstants.ChangeType> refreshedTypes = new HashSet<>();

    // Refresh raw data
    _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    refreshIdealState(accessor, refreshedTypes);
    refreshLiveInstances(accessor, refreshedTypes);
    refreshInstanceConfigs(accessor, refreshedTypes);
    refreshResourceConfig(accessor, refreshedTypes);
    _stateModelDefinitionCache.refresh(accessor);
    _clusterConstraintsCache.refresh(accessor);
    updateMaintenanceInfo(accessor);

    // TODO: once controller gets split, only one controller should update offline instance history
    updateOfflineInstanceHistory(accessor);

    // Refresh derived data
    _instanceMessagesCache.refresh(accessor, _liveInstanceCache.getPropertyMap());
    _currentStateCache.refresh(accessor, _liveInstanceCache.getPropertyMap());

    // current state must be refreshed before refreshing relay messages
    // because we need to use current state to validate all relay messages.
    _instanceMessagesCache.updateRelayMessages(_liveInstanceCache.getPropertyMap(),
        _currentStateCache.getCurrentStatesMap());

    updateIdealRuleMap();
    updateDisabledInstances();

    return refreshedTypes;
  }

  protected void dumpDebugInfo() {
    if (logger.isDebugEnabled()) {
      LogUtil.logDebug(logger, getClusterEventId(),
          "# of StateModelDefinition read from zk: " + getStateModelDefMap().size());
      LogUtil.logDebug(logger, getClusterEventId(),
          "# of ConstraintMap read from zk: " + getConstraintMap().size());
      LogUtil
          .logDebug(logger, getClusterEventId(), "LiveInstances: " + getLiveInstances().keySet());
      for (LiveInstance instance : getLiveInstances().values()) {
        LogUtil.logDebug(logger, getClusterEventId(),
            "live instance: " + instance.getInstanceName() + " " + instance.getSessionId());
      }
      LogUtil.logDebug(logger, getClusterEventId(), "IdealStates: " + getIdealStates().keySet());
      LogUtil.logDebug(logger, getClusterEventId(),
          "ResourceConfigs: " + getResourceConfigMap().keySet());
      LogUtil.logDebug(logger, getClusterEventId(),
          "InstanceConfigs: " + getInstanceConfigMap().keySet());
      LogUtil.logDebug(logger, getClusterEventId(), "ClusterConfigs: " + getClusterConfig());
    }
  }

  public ClusterConfig getClusterConfig() {
    return _clusterConfig;
  }

  public void setClusterConfig(ClusterConfig clusterConfig) {
    _clusterConfig = clusterConfig;
  }

  @Override
  public String getClusterName() {
    // We have some assumption in code that we rely on cluster config to get cluster name
    return _clusterConfig == null ? _clusterName : _clusterConfig.getClusterName();
  }

  @Override
  public String getPipelineName() {
    return _pipelineName;
  }


  @Override
  public String getClusterEventId() {
    return _clusterEventId;
  }

  @Override
  public void setClusterEventId(String clusterEventId) {
    _clusterEventId = clusterEventId;
  }

  /**
   * Return the last offline time map for all offline instances.
   * @return
   */
  public Map<String, Long> getInstanceOfflineTimeMap() {
    return _instanceOfflineTimeMap;
  }

  /**
   * Retrieves the idealstates for all resources
   * @return
   */
  public Map<String, IdealState> getIdealStates() {
    return _idealStateCache.getPropertyMap();
  }

  public synchronized void setIdealStates(List<IdealState> idealStates) {
    _idealStateCache.setPropertyMap(HelixProperty.convertListToMap(idealStates));
  }

  public Map<String, Map<String, String>> getIdealStateRules() {
    return _idealStateRuleMap;
  }

  /**
   * Returns the LiveInstances for each of the instances that are curretnly up and running
   * @return
   */
  public Map<String, LiveInstance> getLiveInstances() {
    return _liveInstanceCache.getPropertyMap();
  }

  /**
   * Return the set of all instances names.
   */
  public Set<String> getAllInstances() {
    return _instanceConfigCache.getPropertyMap().keySet();
  }

  /**
   * Return all the live nodes that are enabled
   * @return A new set contains live instance name and that are marked enabled
   */
  public Set<String> getEnabledLiveInstances() {
    Set<String> enabledLiveInstances = new HashSet<>(getLiveInstances().keySet());
    enabledLiveInstances.removeAll(getDisabledInstances());

    return enabledLiveInstances;
  }

  /**
   * Return all nodes that are enabled.
   * @return
   */
  public Set<String> getEnabledInstances() {
    Set<String> enabledNodes = new HashSet<>(getAllInstances());
    enabledNodes.removeAll(getDisabledInstances());

    return enabledNodes;
  }

  /**
   * Return all the live nodes that are enabled and tagged with given instanceTag.
   * @param instanceTag The instance group tag.
   * @return A new set contains live instance name and that are marked enabled and have the
   *         specified tag.
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
   * @param instanceTag The instance group tag.
   */
  public Set<String> getInstancesWithTag(String instanceTag) {
    Set<String> taggedInstances = new HashSet<>();
    for (String instance : _instanceConfigCache.getPropertyMap().keySet()) {
      InstanceConfig instanceConfig = _instanceConfigCache.getPropertyByName(instance);
      if (instanceConfig != null && instanceConfig.containsTag(instanceTag)) {
        taggedInstances.add(instance);
      }
    }

    return taggedInstances;
  }

  /**
   * Some partitions might be disabled on specific nodes.
   * This method allows one to fetch the set of nodes where a given partition is disabled
   * @param partition
   * @return
   */
  public Set<String> getDisabledInstancesForPartition(String resource, String partition) {
    Set<String> disabledInstancesForPartition = new HashSet<>(_disabledInstanceSet);
    if (_disabledInstanceForPartitionMap.containsKey(resource)
        && _disabledInstanceForPartitionMap.get(resource).containsKey(partition)) {
      disabledInstancesForPartition
          .addAll(_disabledInstanceForPartitionMap.get(resource).get(partition));
    }

    return disabledInstancesForPartition;
  }

  /**
   * This method allows one to fetch the set of nodes that are disabled
   * @return
   */
  public Set<String> getDisabledInstances() {
    return Collections.unmodifiableSet(_disabledInstanceSet);
  }

  public synchronized void setLiveInstances(List<LiveInstance> liveInstances) {
    _liveInstanceCache.setPropertyMap(HelixProperty.convertListToMap(liveInstances));
    _updateInstanceOfflineTime = true;
  }

  /**
   * Provides the current state of the node for a given session id, the sessionid can be got from
   * LiveInstance
   * @param instanceName
   * @param clientSessionId
   * @return
   */
  public Map<String, CurrentState> getCurrentState(String instanceName, String clientSessionId) {
    return _currentStateCache.getCurrentState(instanceName, clientSessionId);
  }

  /**
   * Provides a list of current outstanding transitions on a given instance.
   * @param instanceName
   * @return
   */
  public Map<String, Message> getMessages(String instanceName) {
    return _instanceMessagesCache.getMessages(instanceName);
  }

  /**
   * Provides a list of current outstanding pending relay messages on a given instance.
   * @param instanceName
   * @return
   */
  public Map<String, Message> getRelayMessages(String instanceName) {
    return _instanceMessagesCache.getRelayMessages(instanceName);
  }

  public void cacheMessages(Collection<Message> messages) {
    _instanceMessagesCache.cacheMessages(messages);
  }

  /**
   * Provides the state model definition for a given state model
   * @param stateModelDefRef
   * @return
   */
  public StateModelDefinition getStateModelDef(String stateModelDefRef) {
    return _stateModelDefinitionCache.getPropertyByName(stateModelDefRef);
  }

  /**
   * Provides all state model definitions
   * @return state model definition map
   */
  public Map<String, StateModelDefinition> getStateModelDefMap() {
    return _stateModelDefinitionCache.getPropertyMap();
  }

  /**
   * Provides the idealstate for a given resource
   * @param resourceName
   * @return
   */
  public IdealState getIdealState(String resourceName) {
    return _idealStateCache.getPropertyByName(resourceName);
  }

  /**
   * Returns the instance config map
   * @return
   */
  public Map<String, InstanceConfig> getInstanceConfigMap() {
    return _instanceConfigCache.getPropertyMap();
  }

  /**
   * Set the instance config map
   * @param instanceConfigMap
   */
  public void setInstanceConfigMap(Map<String, InstanceConfig> instanceConfigMap) {
    _instanceConfigCache.setPropertyMap(instanceConfigMap);
  }

  /**
   * Returns the resource config map
   * @return
   */
  public Map<String, ResourceConfig> getResourceConfigMap() {
    return _resourceConfigCache.getPropertyMap();
  }

  public ResourceConfig getResourceConfig(String resource) {
    return _resourceConfigCache.getPropertyByName(resource);
  }

  /**
   * Returns the ClusterConstraints for a given constraintType
   * @param type
   * @return
   */
  public ClusterConstraints getConstraint(ClusterConstraints.ConstraintType type) {
    return _clusterConstraintsCache.getPropertyByName(type.name());
  }

  /**
   * Return a map of all constraints. Key is constraintType
   * @return
   */
  public Map<String, ClusterConstraints> getConstraintMap() {
    return _clusterConstraintsCache.getPropertyMap();
  }

  /**
   * Notify the cache that some part of the cluster data has been changed.
   *
   * Don't lock the propertyDataChangedMap here because the refresh process, which also read this map,
   * may take a long time to finish. If a lock is required, the notification might be blocked by refresh.
   * In this case, the delayed notification processing might cause performance issue.
   */
  public void notifyDataChange(HelixConstants.ChangeType changeType) {
    _propertyDataChangedMap.get(changeType).set(true);
  }

  /**
   * Notify the cache that some part of the cluster data has been changed.
   */
  public void notifyDataChange(HelixConstants.ChangeType changeType, String pathChanged) {
    notifyDataChange(changeType);
  }

  private void updateOfflineInstanceHistory(HelixDataAccessor accessor) {
    if (!_updateInstanceOfflineTime) {
      return;
    }
    List<String> offlineNodes =
        new ArrayList<>(_instanceConfigCache.getPropertyMap().keySet());
    offlineNodes.removeAll(_liveInstanceCache.getPropertyMap().keySet());
    _instanceOfflineTimeMap = new HashMap<>();

    for (String instance : offlineNodes) {
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      PropertyKey propertyKey = keyBuilder.participantHistory(instance);
      ParticipantHistory history = accessor.getProperty(propertyKey);
      if (history == null) {
        history = new ParticipantHistory(instance);
      }
      if (history.getLastOfflineTime() == ParticipantHistory.ONLINE) {
        history.reportOffline();
        // persist history back to ZK.
        if (!accessor.setProperty(propertyKey, history)) {
          LogUtil.logError(logger, getClusterEventId(),
              "Fails to persist participant online history back to ZK!");
        }
      }
      _instanceOfflineTimeMap.put(instance, history.getLastOfflineTime());
    }
    _updateInstanceOfflineTime = false;
  }

  private void updateDisabledInstances() {
    // Move the calculating disabled instances to refresh
    _disabledInstanceForPartitionMap.clear();
    _disabledInstanceSet.clear();
    for (InstanceConfig config : _instanceConfigCache.getPropertyMap().values()) {
      Map<String, List<String>> disabledPartitionMap = config.getDisabledPartitionsMap();
      if (!config.getInstanceEnabled()) {
        _disabledInstanceSet.add(config.getInstanceName());
      }
      for (String resource : disabledPartitionMap.keySet()) {
        if (!_disabledInstanceForPartitionMap.containsKey(resource)) {
          _disabledInstanceForPartitionMap.put(resource, new HashMap<String, Set<String>>());
        }
        for (String partition : disabledPartitionMap.get(resource)) {
          if (!_disabledInstanceForPartitionMap.get(resource).containsKey(partition)) {
            _disabledInstanceForPartitionMap.get(resource).put(partition, new HashSet<String>());
          }
          _disabledInstanceForPartitionMap.get(resource).get(partition)
              .add(config.getInstanceName());
        }
      }
    }
    if (_clusterConfig != null && _clusterConfig.getDisabledInstances() != null) {
      _disabledInstanceSet.addAll(_clusterConfig.getDisabledInstances().keySet());
    }
  }

  /**
   * Indicate that a full read should be done on the next refresh
   */
  public void requireFullRefresh() {
    for (HelixConstants.ChangeType type : HelixConstants.ChangeType.values()) {
      if (!_noFullRefreshProperty.contains(type)) {
        _propertyDataChangedMap.get(type).set(true);
      }
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

  public boolean isMaintenanceModeEnabled() {
    return _isMaintenanceModeEnabled;
  }

  public boolean hasMaintenanceSignalChanged() {
    return _hasMaintenanceSignalChanged;
  }

  public void setMaintenanceSignalChanged() {
    _hasMaintenanceSignalChanged = true;
  }

  public MaintenanceSignal getMaintenanceSignal() {
    return _maintenanceSignal;
  }

  protected StringBuilder genCacheContentStringBuilder() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("liveInstaceMap: %s", _liveInstanceCache.getPropertyMap())).append("\n");
    sb.append(String.format("idealStateMap: %s", _idealStateCache.getPropertyMap())).append("\n");
    sb.append(String.format("stateModelDefMap: %s",  _stateModelDefinitionCache.getPropertyMap())).append("\n");
    sb.append(String.format("instanceConfigMap: %s", _instanceConfigCache.getPropertyMap())).append("\n");
    sb.append(String.format("resourceConfigMap: %s", _resourceConfigCache.getPropertyMap())).append("\n");
    sb.append(String.format("messageCache: %s", _instanceMessagesCache)).append("\n");
    sb.append(String.format("currentStateCache: %s", _currentStateCache)).append("\n");
    sb.append(String.format("clusterConfig: %s", _clusterConfig)).append("\n");
    return sb;
  }

  @Override
  public String toString() {
    return genCacheContentStringBuilder().toString();
  }
}
