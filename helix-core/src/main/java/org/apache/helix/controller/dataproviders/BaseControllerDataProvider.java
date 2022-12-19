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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.rebalancer.constraint.AbnormalStateResolver;
import org.apache.helix.common.caches.AbstractDataCache;
import org.apache.helix.common.caches.CurrentStateCache;
import org.apache.helix.common.caches.InstanceMessagesCache;
import org.apache.helix.common.caches.PropertyCache;
import org.apache.helix.common.caches.TaskCurrentStateCache;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.InstanceValidationUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common building block for controller to cache their data. This common building block contains
 * information about cluster config, instance config, resource config, ideal states, current state,
 * live instances, cluster constraint, state model definition.
 *
 * This class will be moved to helix-common module in the future
 */
public class BaseControllerDataProvider implements ControlContextProvider {
  private static final Logger logger = LoggerFactory.getLogger(BaseControllerDataProvider.class);

  // We only refresh EV and TEV the very first time the cluster data cache is initialized
  private static final List<HelixConstants.ChangeType> _noFullRefreshProperty = Arrays
      .asList(HelixConstants.ChangeType.EXTERNAL_VIEW,
          HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW);

  private final String _clusterName;
  private final String _pipelineName;
  private String _clusterEventId = AbstractDataCache.UNKNOWN_EVENT_ID;
  private ClusterConfig _clusterConfig;

  private boolean _updateInstanceOfflineTime = true;
  private MaintenanceSignal _maintenanceSignal;
  private PauseSignal _pauseSignal;
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
  private final CurrentStateCache _currentStateCache;
  protected TaskCurrentStateCache _taskCurrentStateCache;
  private final InstanceMessagesCache _instanceMessagesCache;

  // Other miscellaneous caches
  private Map<String, Long> _instanceOfflineTimeMap;
  private Map<String, Map<String, String>> _idealStateRuleMap;
  private final Map<String, Map<String, Set<String>>> _disabledInstanceForPartitionMap = new HashMap<>();
  private final Set<String> _disabledInstanceSet = new HashSet<>();
  private final Map<String, MonitoredAbnormalResolver> _abnormalStateResolverMap = new HashMap<>();
  private final Set<String> _timedOutInstanceDuringMaintenance = new HashSet<>();
  private Map<String, LiveInstance> _liveInstanceExcludeTimedOutForMaintenance = new HashMap<>();

  public BaseControllerDataProvider() {
    this(AbstractDataCache.UNKNOWN_CLUSTER, AbstractDataCache.UNKNOWN_PIPELINE);
  }

  public BaseControllerDataProvider(String clusterName, String pipelineName) {
    _clusterName = clusterName;
    _pipelineName = pipelineName;
    _propertyDataChangedMap = new ConcurrentHashMap<>();
    for (HelixConstants.ChangeType type : HelixConstants.ChangeType.values()) {
      // refresh every type when it is initialized
      _propertyDataChangedMap.put(type, new AtomicBoolean(true));
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
    _taskCurrentStateCache = new TaskCurrentStateCache(this);
    _instanceMessagesCache = new InstanceMessagesCache(_clusterName);
  }

  private void refreshClusterConfig(final HelixDataAccessor accessor,
      Set<HelixConstants.ChangeType> refreshedType) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.CLUSTER_CONFIG).getAndSet(false)) {
      _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
      refreshedType.add(HelixConstants.ChangeType.CLUSTER_CONFIG);
      // TODO: This is a temp function to clean up incompatible batched disabled instances format.
      // Remove in later version.
      if (_clusterConfig!=null && needCleanUpBatchedDisabledInstance(_clusterConfig.getRecord())
          && cleanBatchDisableMapField(accessor)) {
        LogUtil.logInfo(logger, getClusterEventId(), String
            .format("Clean ClusterConfig mapField for cluster %s, pipeline %s", _clusterName,
                getPipelineName()));
      }
      refreshAbnormalStateResolverMap(_clusterConfig);
    } else {
      LogUtil.logDebug(logger, getClusterEventId(), String
          .format("No ClusterConfig change for cluster %s, pipeline %s", _clusterName,
              getPipelineName()));
    }
  }

  // TODO: This function is used to clean up batched disabled instances for
  // "DISABLED_INSTANCES" introduced in 1.0.3.0. This temp change should be reverted after 1.0.5.0 \
  // or later version.
  private boolean cleanBatchDisableMapField(final HelixDataAccessor accessor) {
    boolean successful =
        accessor.updateProperty(accessor.keyBuilder().clusterConfig(), new DataUpdater<ZNRecord>() {
          @Override
          public ZNRecord update(ZNRecord currentData) {
            if (currentData == null) {
              throw new HelixException(
                  "Cluster: " + _clusterConfig.getClusterName() + ": cluster config is null");
            }
            ZNRecord newRecord = new ZNRecord(currentData);
            String batchDisabledInstanceMapFieldKey =
                ClusterConfig.ClusterConfigProperty.DISABLED_INSTANCES.name();
            if (needCleanUpBatchedDisabledInstance(currentData)) {
              newRecord.getMapFields().remove(batchDisabledInstanceMapFieldKey);
            }
            return newRecord;
          }
        }, null);
    if (!successful) {
      LogUtil.logError(logger, getClusterEventId(), String
          .format("Failed to clean ClusterConfig change for cluster %s, pipeline %s", _clusterName,
              getPipelineName()));
    }
    return successful;
  }

  private boolean needCleanUpBatchedDisabledInstance(ZNRecord record) {
    return record!=null && record.getMapFields()!=null && record.getMapFields()
        .containsKey(ClusterConfig.ClusterConfigProperty.DISABLED_INSTANCES.name());
  }

  private void refreshIdealState(final HelixDataAccessor accessor,
      Set<HelixConstants.ChangeType> refreshedType) {
    if (_propertyDataChangedMap.get(HelixConstants.ChangeType.IDEAL_STATE).getAndSet(false)) {
      _idealStateCache.refresh(accessor);
      refreshedType.add(HelixConstants.ChangeType.IDEAL_STATE);
    } else {
      LogUtil.logInfo(logger, getClusterEventId(),
          String.format("No ideal state change for %s cluster, %s pipeline", _clusterName,
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

  private void refreshManagementSignals(final HelixDataAccessor accessor) {
    _maintenanceSignal = accessor.getProperty(accessor.keyBuilder().maintenance());
    _pauseSignal = accessor.getProperty(accessor.keyBuilder().pause());
    _isMaintenanceModeEnabled = _maintenanceSignal != null;
    // The following flag is to guarantee that there's only one update per pineline run because we
    // check for whether maintenance recovery could happen twice every pipeline
    _hasMaintenanceSignalChanged = false;

    // If maintenance mode has exited, clear cached timed-out nodes
    if (!_isMaintenanceModeEnabled) {
      _timedOutInstanceDuringMaintenance.clear();
      _liveInstanceExcludeTimedOutForMaintenance.clear();
    }
  }

  private void timeoutNodesDuringMaintenance(final HelixDataAccessor accessor, ClusterConfig clusterConfig, boolean isMaintenanceModeEnabled) {
    // If maintenance mode is enabled and timeout window is specified, filter 'new' live nodes
    // for timed-out nodes
    long timeOutWindow = -1;
    if (clusterConfig != null) {
      timeOutWindow = clusterConfig.getOfflineNodeTimeOutForMaintenanceMode();
    }
    if (timeOutWindow >= 0 && isMaintenanceModeEnabled) {
      for (String instance : _liveInstanceCache.getPropertyMap().keySet()) {
        // 1. Check timed-out cache and don't do repeated work;
        // 2. Check for nodes that didn't exist in the last iteration, because it has been checked;
        // 3. For all other nodes, check if it's timed-out.
        // When maintenance mode is first entered, all nodes will be checked as a result.
        if (!_timedOutInstanceDuringMaintenance.contains(instance)
            && !_liveInstanceExcludeTimedOutForMaintenance.containsKey(instance)
            && isInstanceTimedOutDuringMaintenance(accessor, instance, timeOutWindow)) {
          _timedOutInstanceDuringMaintenance.add(instance);
        }
      }
    }
    if (isMaintenanceModeEnabled) {
      _liveInstanceExcludeTimedOutForMaintenance =
          _liveInstanceCache.getPropertyMap().entrySet().stream()
              .filter(e -> !_timedOutInstanceDuringMaintenance.contains(e.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }

  private void updateIdealRuleMap(ClusterConfig clusterConfig) {
    // Assumes cluster config is up-to-date
    if (clusterConfig != null) {
      _idealStateRuleMap = clusterConfig.getIdealStateRules();
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
    refreshClusterConfig(accessor, refreshedTypes);
    refreshIdealState(accessor, refreshedTypes);
    refreshLiveInstances(accessor, refreshedTypes);
    refreshInstanceConfigs(accessor, refreshedTypes);
    refreshResourceConfig(accessor, refreshedTypes);
    _stateModelDefinitionCache.refresh(accessor);
    _clusterConstraintsCache.refresh(accessor);
    refreshManagementSignals(accessor);
    timeoutNodesDuringMaintenance(accessor, _clusterConfig, _isMaintenanceModeEnabled);

    // TODO: once controller gets split, only one controller should update offline instance history
    updateOfflineInstanceHistory(accessor);

    // Refresh derived data
    _instanceMessagesCache.refresh(accessor, _liveInstanceCache.getPropertyMap());
    _currentStateCache.refresh(accessor, _liveInstanceCache.getPropertyMap());

    // current state must be refreshed before refreshing relay messages
    // because we need to use current state to validate all relay messages.
    _instanceMessagesCache.updateRelayMessages(_liveInstanceCache.getPropertyMap(),
        _currentStateCache.getParticipantStatesMap());

    updateIdealRuleMap(getClusterConfig());
    updateDisabledInstances(getInstanceConfigMap().values(), getClusterConfig());

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
            "live instance: " + instance.getInstanceName() + " " + instance.getEphemeralOwner());
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
    refreshAbnormalStateResolverMap(_clusterConfig);
    updateIdealRuleMap(_clusterConfig);
    updateDisabledInstances(getInstanceConfigMap().values(), _clusterConfig);
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
   * Returns the LiveInstances for each of the instances that are currently up and running,
   * excluding the instances that are considered offline during maintenance mode. Instances
   * are timed-out if they have been offline for a while before going live during maintenance mode.
   */
  public Map<String, LiveInstance> getLiveInstances() {
    if (isMaintenanceModeEnabled()) {
      return _liveInstanceExcludeTimedOutForMaintenance;
    }

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
        && _disabledInstanceForPartitionMap
        .get(resource).containsKey(partition)) {
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
   * LiveInstance. This function is only called from the regular pipelines.
   * @param instanceName
   * @param clientSessionId
   * @return
   */
  public Map<String, CurrentState> getCurrentState(String instanceName, String clientSessionId) {
    return getCurrentState(instanceName, clientSessionId, false);
  }

  /**
   * Provides the current state of the node for a given session id, the sessionid can be got from
   * LiveInstance
   * @param instanceName
   * @param clientSessionId
   * @return
   */
  public Map<String, CurrentState> getCurrentState(String instanceName, String clientSessionId,
      boolean isTaskPipeline) {
    Map<String, CurrentState> regularCurrentStates =
        _currentStateCache.getParticipantState(instanceName, clientSessionId);
    if (isTaskPipeline) {
      // TODO: Targeted jobs still rely on regular resource current states, so need to include all
      // resource current states without filtering. For now, allow regular current states to
      // overwrite task current states in case of name conflicts, which are unlikely. Eventually,
      // it should be completely split.
      Map<String, CurrentState> mergedCurrentStates = new HashMap<>();
      mergedCurrentStates
          .putAll(_taskCurrentStateCache.getParticipantState(instanceName, clientSessionId));
      mergedCurrentStates.putAll(regularCurrentStates);
      return Collections.unmodifiableMap(mergedCurrentStates);
    }
    return regularCurrentStates.entrySet().stream().filter(
        entry -> !TaskConstants.STATE_MODEL_NAME.equals(entry.getValue().getStateModelDefRef()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
   * Gets all messages for each instance.
   *
   * @return Map of {instanceName -> Collection of Message}.
   */
  public Map<String, Collection<Message>> getAllInstancesMessages() {
    return getAllInstances().stream().collect(
        Collectors.toMap(instance -> instance, instance -> getMessages(instance).values()));
  }

  /**
   * This function is supposed to be only used by testing purpose for safety. For "get" usage,
   * please use getStaleMessagesByInstance.
   */
  @VisibleForTesting
  public Map<String, Map<String, Message>> getStaleMessages() {
    return _instanceMessagesCache.getStaleMessageCache();
  }

  public Set<Message> getStaleMessagesByInstance(String instanceName) {
    return _instanceMessagesCache.getStaleMessagesByInstance(instanceName);
  }

  public void addStaleMessage(String instanceName, Message staleMessage) {
    _instanceMessagesCache.addStaleMessage(instanceName, staleMessage);
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
    updateDisabledInstances(instanceConfigMap.values(), getClusterConfig());
  }

  /**
   * Returns the resource config map
   * @return
   */
  public Map<String, ResourceConfig> getResourceConfigMap() {
    return _resourceConfigCache.getPropertyMap();
  }

  /**
   * Sets the resource config map
   * @param resourceConfigMap
   */
  public void setResourceConfigMap(Map<String, ResourceConfig> resourceConfigMap) {
    _resourceConfigCache.setPropertyMap(resourceConfigMap);
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
    List<String> offlineNodes = new ArrayList<>(_instanceConfigCache.getPropertyMap().keySet());
    offlineNodes.removeAll(_liveInstanceCache.getPropertyMap().keySet());
    _instanceOfflineTimeMap = new HashMap<>();

    for (String instance : offlineNodes) {
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      PropertyKey propertyKey = keyBuilder.participantHistory(instance);
      ParticipantHistory history = accessor.getProperty(propertyKey);
      if (history == null) {
        // this means the instance has been removed, skip history update
        continue;
      }
      if (history.getLastOfflineTime() == ParticipantHistory.ONLINE) {
        history.reportOffline();
        // persist history back to ZK only if the node still exists
        boolean succeed = accessor.updateProperty(propertyKey,
            currentData -> (currentData == null) ? null : history.getRecord(), history);
        if (!succeed) {
          LogUtil.logError(logger, getClusterEventId(),
              "Fails to persist participant online history back to ZK!");
        }
      }
      _instanceOfflineTimeMap.put(instance, history.getLastOfflineTime());
    }
    _updateInstanceOfflineTime = false;
  }

  private void updateDisabledInstances(Collection<InstanceConfig> instanceConfigs,
      ClusterConfig clusterConfig) {
    // Move the calculating disabled instances to refresh
    _disabledInstanceForPartitionMap.clear();
    _disabledInstanceSet.clear();
    for (InstanceConfig config : instanceConfigs) {
      Map<String, List<String>> disabledPartitionMap = config.getDisabledPartitionsMap();
      if (!InstanceValidationUtil.isInstanceEnabled(config, clusterConfig)) {
        _disabledInstanceSet.add(config.getInstanceName());
      }
      for (String resource : disabledPartitionMap.keySet()) {
        _disabledInstanceForPartitionMap.putIfAbsent(resource, new HashMap<>());
        for (String partition : disabledPartitionMap.get(resource)) {
          _disabledInstanceForPartitionMap.get(resource)
              .computeIfAbsent(partition, key -> new HashSet<>()).add(config.getInstanceName());
        }
      }
    }
  }

  /*
   * Check if the instance is timed-out during maintenance mode. An instance is timed-out if it has
   * been offline for longer than the user defined timeout window.
   * @param timeOutWindow - the timeout window; guaranteed to be non-negative
   */
  private boolean isInstanceTimedOutDuringMaintenance(HelixDataAccessor accessor, String instance,
      long timeOutWindow) {
    ParticipantHistory history =
        accessor.getProperty(accessor.keyBuilder().participantHistory(instance));
    if (history == null) {
      LogUtil.logWarn(logger, getClusterEventId(), "Participant history is null for instance " + instance);
      return false;
    }
    List<Long> onlineTimestamps = history.getOnlineTimestampsAsMilliseconds();
    List<Long> offlineTimestamps = history.getOfflineTimestampsAsMilliseconds();

    onlineTimestamps.add(System.currentTimeMillis());
    // Hop between each pair of online timestamps and find the maximum offline window
    for (int onlineTimestampIndex = onlineTimestamps.size() - 1, offlineTimestampIndex =
        offlineTimestamps.size() - 1; onlineTimestampIndex >= 0 && offlineTimestampIndex >= 0;
        onlineTimestampIndex--) {
      long onlineWindowRight = onlineTimestamps.get(onlineTimestampIndex);
      // We don't care about offline windows before maintenance mode starts
      if (onlineWindowRight <= _maintenanceSignal.getTimestamp()) {
        break;
      }
      long onlineWindowLeft = 0L;
      if (onlineTimestampIndex > 0) {
        onlineWindowLeft = onlineTimestamps.get(onlineTimestampIndex - 1);
      }

      // If the offline timestamp isn't within this pair of online timestamp, continue
      if (offlineTimestamps.get(offlineTimestampIndex) <= onlineWindowLeft) {
        continue;
      }

      // Goes left until the next offline timestamp surpasses the window left
      while (offlineTimestampIndex >= 0
          && offlineTimestamps.get(offlineTimestampIndex) > onlineWindowLeft) {
        offlineTimestampIndex--;
      }

      long offlineTimeStamp = offlineTimestamps.get(offlineTimestampIndex + 1);
      // Check the largest offline window against the timeout window
      if (onlineWindowRight - offlineTimeStamp > timeOutWindow) {
        LogUtil.logWarn(logger, getClusterEventId(), String.format(
            "During maintenance mode, instance %s is timed-out due to its offline time. Online time: "
                + "%s, Offline time: %s, Timeout window: %s", instance, onlineWindowRight,
            offlineTimeStamp, timeOutWindow));
        return true;
      }
    }

    return false;
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

  public MonitoredAbnormalResolver getAbnormalStateResolver(String stateModel) {
    return _abnormalStateResolverMap
        .getOrDefault(stateModel, MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER);
  }

  private void refreshAbnormalStateResolverMap(ClusterConfig clusterConfig) {
    if (clusterConfig == null) {
      logger.debug("Skip refreshing abnormal state resolvers because the ClusterConfig is missing");
      return;
    }

    Map<String, String> resolverMap = clusterConfig.getAbnormalStateResolverMap();
    logger.info("Start loading the abnormal state resolvers with configuration {}", resolverMap);
    // Calculate all the resolvers to be removed.
    Map<String, MonitoredAbnormalResolver> removingResolverWraps =
        new HashMap<>(_abnormalStateResolverMap);
    removingResolverWraps.keySet().removeAll(resolverMap.keySet());
    for (MonitoredAbnormalResolver monitoredAbnormalResolver : removingResolverWraps.values()) {
      monitoredAbnormalResolver.close();
    }

    // Reload the resolver classes into cache based on the configuration.
    _abnormalStateResolverMap.keySet().retainAll(resolverMap.keySet());
    for (String stateModel : resolverMap.keySet()) {
      String resolverClassName = resolverMap.get(stateModel);
      if (resolverClassName == null || resolverClassName.isEmpty()) {
        // skip the empty definition.
        continue;
      }

      MonitoredAbnormalResolver currentMonitoredResolver =
          _abnormalStateResolverMap.get(stateModel);
      if (currentMonitoredResolver == null || !resolverClassName
          .equals(currentMonitoredResolver.getResolverClass().getName())) {

        if (currentMonitoredResolver != null) {
          // Clean up the existing monitored resolver.
          // We must close the existing object first to ensure the metric being removed before the
          // new one can be registered normally.
          currentMonitoredResolver.close();
        }

        try {
          AbnormalStateResolver newResolver = AbnormalStateResolver.class
              .cast(HelixUtil.loadClass(getClass(), resolverClassName).newInstance());
          _abnormalStateResolverMap.put(stateModel,
              new MonitoredAbnormalResolver(newResolver, _clusterName, stateModel));
        } catch (Exception e) {
          throw new HelixException(String
              .format("Failed to instantiate the abnormal state resolver %s for state model %s",
                  resolverClassName, stateModel));
        }
      } // else, nothing to update since the same resolver class has been loaded.
    }

    logger.info("Finish loading the abnormal state resolvers {}", _abnormalStateResolverMap);
  }

  public boolean isMaintenanceModeEnabled() {
    return _isMaintenanceModeEnabled;
  }

  /**
   * Mark the pipeline to enable maintenance mode. It is not supposed to use anywhere
   * except in the auto enter maintenance mode logic when offline instances exceeded.
   */
  // TODO: refactor it to writable cache once read-only/writable caches are separated.
  public void enableMaintenanceMode() {
    _isMaintenanceModeEnabled = true;
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

  public PauseSignal getPauseSignal() {
    return _pauseSignal;
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

  protected PropertyCache<LiveInstance> getLiveInstanceCache() {
    return _liveInstanceCache;
  }

  @Override
  public String toString() {
    return genCacheContentStringBuilder().toString();
  }
}
