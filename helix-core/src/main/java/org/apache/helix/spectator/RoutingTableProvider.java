package org.apache.helix.spectator;

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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.collect.ImmutableMap;
import javax.management.JMException;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.CustomizedViewChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.common.ClusterEventProcessor;
import org.apache.helix.common.caches.CurrentStateSnapshot;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.mbeans.RoutingTableProviderMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingTableProvider
    implements ExternalViewChangeListener, InstanceConfigChangeListener, ConfigChangeListener,
    LiveInstanceChangeListener, CurrentStateChangeListener, CustomizedViewChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(RoutingTableProvider.class);
  private static final long DEFAULT_PERIODIC_REFRESH_INTERVAL = 300000L; // 5 minutes
  private final Map<String, AtomicReference<RoutingTable>> _routingTableRefMap;
  private final HelixManager _helixManager;
  private final RouterUpdater _routerUpdater;
  private final Map<PropertyType, List<String>> _sourceDataTypeMap;
  private final Map<RoutingTableChangeListener, ListenerContext> _routingTableChangeListenerMap;
  private final Map<PropertyType, RoutingTableProviderMonitor> _monitorMap;

  // For periodic refresh
  private long _lastRefreshTimestamp;
  private boolean _isPeriodicRefreshEnabled = true; // Default is enabled
  private long _periodRefreshInterval;
  private ScheduledThreadPoolExecutor _periodicRefreshExecutor;
  // For computing intensive reporting logic
  private ExecutorService _reportExecutor;
  private Future _reportingTask = null;

  protected static final  String DEFAULT_PROPERTY = "HELIX_DEFAULT_PROPERTY";
  protected static final  String DEFAULT_TYPE = "HELIX_DEFAULT";


  public RoutingTableProvider() {
    this(null);
  }

  public RoutingTableProvider(HelixManager helixManager) throws HelixException {
    this(helixManager, ImmutableMap.of(PropertyType.EXTERNALVIEW, Collections.emptyList()), true,
        DEFAULT_PERIODIC_REFRESH_INTERVAL);
  }

  public RoutingTableProvider(HelixManager helixManager, PropertyType sourceDataType)
      throws HelixException {
    this(helixManager, ImmutableMap.of(sourceDataType, Collections.emptyList()), true,
        DEFAULT_PERIODIC_REFRESH_INTERVAL);
  }

  public RoutingTableProvider(HelixManager helixManager, Map<PropertyType, List<String>> sourceDataTypeMap) {
    this(helixManager, sourceDataTypeMap, true, DEFAULT_PERIODIC_REFRESH_INTERVAL);
  }

  /**
   * Initialize an instance of RoutingTableProvider
   * @param helixManager
   * @param sourceDataType
   * @param isPeriodicRefreshEnabled true if periodic refresh is enabled, false otherwise
   * @param periodRefreshInterval only effective if isPeriodRefreshEnabled is true
   * @throws HelixException
   */
  public RoutingTableProvider(HelixManager helixManager, PropertyType sourceDataType,
      boolean isPeriodicRefreshEnabled, long periodRefreshInterval) throws HelixException {
    this(helixManager, ImmutableMap.of(sourceDataType, Collections.emptyList()),
        isPeriodicRefreshEnabled, periodRefreshInterval);
  }

  /**
   * Initialize an instance of RoutingTableProvider
   * @param helixManager
   * @param sourceDataTypeMap
   * @param isPeriodicRefreshEnabled true if periodic refresh is enabled, false otherwise
   * @param periodRefreshInterval only effective if isPeriodRefreshEnabled is true
   * @throws HelixException
   */
  public RoutingTableProvider(HelixManager helixManager,
      Map<PropertyType, List<String>> sourceDataTypeMap, boolean isPeriodicRefreshEnabled,
      long periodRefreshInterval) throws HelixException {

    validateSourceDataTypeMap(sourceDataTypeMap);

    _routingTableRefMap = new HashMap<>();
    _helixManager = helixManager;
    _sourceDataTypeMap = sourceDataTypeMap;
    _routingTableChangeListenerMap = new ConcurrentHashMap<>();
    String clusterName = _helixManager != null ? _helixManager.getClusterName() : null;
    _routerUpdater = new RouterUpdater(clusterName, sourceDataTypeMap);
    _routerUpdater.start();
    _monitorMap = new HashMap<>();

    for (PropertyType propertyType : _sourceDataTypeMap.keySet()) {
      _monitorMap.put(propertyType, new RoutingTableProviderMonitor(propertyType, clusterName));
      try {
        _monitorMap.get(propertyType).register();
      } catch (JMException e) {
        logger.error("Failed to register RoutingTableProvider monitor MBean.", e);
      }
    }
    _reportExecutor = Executors.newSingleThreadExecutor();

    for (PropertyType propertyType : _sourceDataTypeMap.keySet()) {
      if (_sourceDataTypeMap.get(propertyType).size() == 0) {
        // Empty CustomizedStateType
        String customizedStateType = DEFAULT_TYPE;
        String key = propertyType.name() + "_" + customizedStateType;
        if (_routingTableRefMap.get(key) == null) {
          _routingTableRefMap.put(key, new AtomicReference<>(new RoutingTable(propertyType)));
        }
      } else {
        for (String customizedStateType : _sourceDataTypeMap.get(propertyType)) {
          String key = propertyType.name() + "_" + customizedStateType;
          if (_routingTableRefMap.get(key) == null) {
            _routingTableRefMap.put(key, new AtomicReference<>(
                new CustomizedViewRoutingTable(propertyType, customizedStateType)));
          }
        }
      }
    }

    if (_helixManager != null) {
      for (PropertyType propertyType : _sourceDataTypeMap.keySet()) {
        switch (propertyType) {
        case EXTERNALVIEW:
          try {
            _helixManager.addExternalViewChangeListener(this);
          } catch (Exception e) {
            shutdown();
            logger.error("Failed to attach ExternalView Listener to HelixManager!");
            throw new HelixException("Failed to attach ExternalView Listener to HelixManager!", e);
          }
          break;
        case CUSTOMIZEDVIEW:
          List<String> customizedStateTypes = _sourceDataTypeMap.get(propertyType);
          for (String customizedStateType : customizedStateTypes) {
            try {
              _helixManager.addCustomizedViewChangeListener(this, customizedStateType);
            } catch (Exception e) {
              shutdown();
              logger.error("Failed to attach CustomizedView Listener to HelixManager for type {}!",
                  customizedStateType);
              throw new HelixException(String.format(
                  "Failed to attach CustomizedView Listener to HelixManager for type %s!",
                  customizedStateType), e);
            }
          }
          break;
        case TARGETEXTERNALVIEW:
          // Check whether target external has been enabled or not
          if (!_helixManager.getHelixDataAccessor().getBaseDataAccessor().exists(
              _helixManager.getHelixDataAccessor().keyBuilder().targetExternalViews().getPath(), 0)) {
            shutdown();
            throw new HelixException("Target External View is not enabled!");
          }

          try {
            _helixManager.addTargetExternalViewChangeListener(this);
          } catch (Exception e) {
            shutdown();
            logger.error("Failed to attach TargetExternalView Listener to HelixManager!");
            throw new HelixException("Failed to attach TargetExternalView Listener to HelixManager!",
                e);
          }
          break;
        case CURRENTSTATES:
          // CurrentState change listeners will be added later in LiveInstanceChange call.
          break;
        default:
          throw new HelixException(String.format("Unsupported source data type: %s", propertyType));
        }
      }
      try {
        _helixManager.addInstanceConfigChangeListener(this);
        _helixManager.addLiveInstanceChangeListener(this);
      } catch (Exception e) {
        shutdown();
        logger.error(
            "Failed to attach InstanceConfig and LiveInstance Change listeners to HelixManager!");
        throw new HelixException(
            "Failed to attach InstanceConfig and LiveInstance Change listeners to HelixManager!",
            e);
      }
    }

    // For periodic refresh
    if (isPeriodicRefreshEnabled && _helixManager != null) {
      _lastRefreshTimestamp = System.currentTimeMillis(); // Initialize timestamp with current time
      _periodRefreshInterval = periodRefreshInterval;
      // Construct a periodic refresh context
      final NotificationContext periodicRefreshContext = new NotificationContext(_helixManager);
      periodicRefreshContext.setType(NotificationContext.Type.PERIODIC_REFRESH);
      // Create a thread that runs at specified interval
      _periodicRefreshExecutor = new ScheduledThreadPoolExecutor(1);
      _periodicRefreshExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          // If enough time has elapsed since last refresh, queue a refresh event
          if (_lastRefreshTimestamp + _periodRefreshInterval < System.currentTimeMillis()) {
            // changeType is irrelevant for NotificationContext.Type.PERIODIC_REFRESH
            _routerUpdater.queueEvent(periodicRefreshContext, ClusterEventType.PeriodicalRebalance,
                null);
          }
        }
      }, _periodRefreshInterval, _periodRefreshInterval, TimeUnit.MILLISECONDS);
    } else {
      _isPeriodicRefreshEnabled = false;
    }
  }

  /**
   * Check and validate the input of the sourceDataTypeMap parameter
   * @param sourceDataTypeMap
   */
  private void validateSourceDataTypeMap(Map<PropertyType, List<String>> sourceDataTypeMap) {
    for (PropertyType propertyType : sourceDataTypeMap.keySet()) {
      if (propertyType.equals(PropertyType.CUSTOMIZEDVIEW)
          && sourceDataTypeMap.get(propertyType).size() == 0) {
        logger.error("CustomizedView has been used without any aggregation type!");
        throw new HelixException("CustomizedView has been used without any aggregation type!");
      }
      if (!propertyType.equals(PropertyType.CUSTOMIZEDVIEW)
          && sourceDataTypeMap.get(propertyType).size() != 0) {
        logger.error("Type has been used in addition to the propertyType {} !",
            propertyType.name());
        throw new HelixException(
            String.format("Type %s has been used in addition to the propertyType %s !",
                sourceDataTypeMap.get(propertyType), propertyType.name()));
      }
    }
  }

  /**
   * Shutdown current RoutingTableProvider. Once it is shutdown, it should never be reused.
   */
  public void shutdown() {
    if (_periodicRefreshExecutor != null) {
      _periodicRefreshExecutor.purge();
      _periodicRefreshExecutor.shutdown();
    }
    _routerUpdater.shutdown();


    for (PropertyType propertyType : _monitorMap.keySet()) {
      _monitorMap.get(propertyType).unregister();
    }

    if (_helixManager != null) {
      PropertyKey.Builder keyBuilder = _helixManager.getHelixDataAccessor().keyBuilder();
      for (PropertyType propertyType : _sourceDataTypeMap.keySet()) {
        switch (propertyType) {
        case EXTERNALVIEW:
          _helixManager.removeListener(keyBuilder.externalViews(), this);
          break;
        case CUSTOMIZEDVIEW:
          List<String> customizedStateTypes = _sourceDataTypeMap.get(propertyType);
          // Remove listener on each individual customizedStateType
          for (String customizedStateType : customizedStateTypes) {
            _helixManager.removeListener(keyBuilder.customizedView(customizedStateType), this);
          }
          break;
        case TARGETEXTERNALVIEW:
          _helixManager.removeListener(keyBuilder.targetExternalViews(), this);
          break;
        case CURRENTSTATES:
          NotificationContext context = new NotificationContext(_helixManager);
          context.setType(NotificationContext.Type.FINALIZE);
          updateCurrentStatesListeners(Collections.<LiveInstance> emptyList(), context);
          break;
        default:
          break;
        }
      }
    }
  }

  /**
   * Get an snapshot of current RoutingTable information. The snapshot is immutable, it reflects the
   * routing table information at the time this method is called.
   * @return snapshot of current routing table.
   */
  public RoutingTableSnapshot getRoutingTableSnapshot() {
      String key = getRoutingTableKey(DEFAULT_PROPERTY, DEFAULT_TYPE);
      return new RoutingTableSnapshot(_routingTableRefMap.get(key).get());
  }

  /**
   * Get an snapshot of current RoutingTable information for specific PropertyType.
   * The snapshot is immutable, it reflects the routing table information at the time this method is
   * called.
   * @return snapshot of current routing table.
   */
  public RoutingTableSnapshot getRoutingTableSnapshot (PropertyType propertyType) {
    String key = getRoutingTableKey(propertyType.name(), DEFAULT_TYPE);
    return new RoutingTableSnapshot(_routingTableRefMap.get(key).get());
  }

  /**
   * Get an snapshot of all of the available RoutingTable information. The snapshot is immutable, it
   * reflects the routing table information at the time this method is called.
   * @return snapshot associated with specific propertyType and type.
   */
  public RoutingTableSnapshot getRoutingTableSnapshot(PropertyType propertyType, String type) {
    String key = getRoutingTableKey(propertyType.name(), type);
    return new RoutingTableSnapshot(_routingTableRefMap.get(key).get());
  }

  /**
   * Get an snapshot of all of the available RoutingTable information. The snapshot is immutable, it
   * reflects the routing table information at the time this method is called.
   * @return all of the available snapshots of current routing table.
   */
  public Map<String, Map<String, RoutingTableSnapshot>> getRoutingTableSnapshots() {
    Map<String, Map<String, RoutingTableSnapshot>> snapshots = new HashMap<>();
    for (String key : _routingTableRefMap.keySet()) {
      RoutingTable routingTable = _routingTableRefMap.get(key).get();
      String propertyTypeName = routingTable.getPropertyType().name();
      String customizedStateType = routingTable.getCustomizedStateType();
      if (!snapshots.containsKey(propertyTypeName)) {
        snapshots.put(propertyTypeName, new HashMap<>());
      }
      snapshots.get(propertyTypeName).put(customizedStateType, new RoutingTableSnapshot(routingTable));
    }
    return snapshots;
  }

  /**
   * Add RoutingTableChangeListener with user defined context
   * @param routingTableChangeListener
   * @param context user defined context
   */
  public void addRoutingTableChangeListener(
      final RoutingTableChangeListener routingTableChangeListener, Object context) {
    _routingTableChangeListenerMap.put(routingTableChangeListener, new ListenerContext(context));
    logger.info("Attach RoutingTableProviderChangeListener {}",
        routingTableChangeListener.getClass().getName());
  }

  /**
   * Remove RoutingTableChangeListener
   * @param routingTableChangeListener
   */
  public Object removeRoutingTableChangeListener(
      final RoutingTableChangeListener routingTableChangeListener) {
    logger.info("Detach RoutingTableProviderChangeListener {}",
        routingTableChangeListener.getClass().getName());
    return _routingTableChangeListenerMap.remove(routingTableChangeListener);
  }

  /**
   * returns the instances for {resource,partition} pair that are in a specific
   * {state}
   * This method will be deprecated, please use the
   * {@link #getInstancesForResource(String, String, String)} getInstancesForResource} method.
   * @param resourceName
   *          -
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstances(String resourceName, String partitionName,
      String state) {
    return getInstancesForResource(resourceName, partitionName, state);
  }

  public List<InstanceConfig> getInstances(String resourceName, String partitionName, String state,
      PropertyType propertyType, String type) {
    return getInstancesForResource(resourceName, partitionName, state, propertyType, type);
  }

  /**
   * returns the instances for {resource,partition} pair that are in a specific
   * {state}
   * @param resourceName
   *          -
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResource(String resourceName, String partitionName,
      String state) {
    String key = getRoutingTableKey(DEFAULT_PROPERTY, DEFAULT_TYPE);
    return _routingTableRefMap.get(key).get().getInstancesForResource(resourceName, partitionName,
        state);
  }

  public List<InstanceConfig> getInstancesForResource(String resourceName, String partitionName,
      String state, PropertyType propertyType, String type) {
    String key = getRoutingTableKey(propertyType.name(), type);
    return _routingTableRefMap.get(key).get().getInstancesForResource(resourceName, partitionName,
        state);
  }

  /**
   * returns the instances for {resource group,partition} pair in all resources belongs to the given
   * resource group that are in a specific {state}.
   * The return results aggregate all partition states from all the resources in the given resource
   * group.
   * @param resourceGroupName
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName,
      String partitionName, String state) {
    String key = getRoutingTableKey(DEFAULT_PROPERTY, DEFAULT_TYPE);
    return _routingTableRefMap.get(key).get().getInstancesForResourceGroup(resourceGroupName,
        partitionName, state);
  }

  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName,
      String partitionName, String state, PropertyType propertyType, String type) {
    String key = getRoutingTableKey(propertyType.name(), type);
    return _routingTableRefMap.get(key).get().getInstancesForResourceGroup(resourceGroupName,
        partitionName, state);
  }

  /**
   * returns the instances for {resource group,partition} pair contains any of the given tags
   * that are in a specific {state}.
   * Find all resources belongs to the given resource group that have any of the given resource tags
   * and return the aggregated partition states from all these resources.
   * @param resourceGroupName
   * @param partitionName
   * @param state
   * @param resourceTags
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName,
      String partitionName, String state, List<String> resourceTags) {
    String key = getRoutingTableKey(DEFAULT_PROPERTY, DEFAULT_TYPE);
    return _routingTableRefMap.get(key).get().getInstancesForResourceGroup(resourceGroupName,
        partitionName, state, resourceTags);
  }

  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName,
      String partitionName, String state, List<String> resourceTags, PropertyType propertyType,
      String type) {
    String key = getRoutingTableKey(propertyType.name(), type);
    return _routingTableRefMap.get(key).get().getInstancesForResourceGroup(resourceGroupName,
        partitionName, state, resourceTags);
  }

  /**
   * returns all instances for {resource} that are in a specific {state}
   * This method will be deprecated, please use the
   * {@link #getInstancesForResource(String, String) getInstancesForResource} method.
   * @param resourceName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstances(String resourceName, String state) {
    return getInstancesForResource(resourceName, state);
  }

  public Set<InstanceConfig> getInstances(String resourceName, String state,
      PropertyType propertyType, String type) {
    return getInstancesForResource(resourceName, state, propertyType, type);
  }

  /**
   * returns all instances for {resource} that are in a specific {state}.
   * @param resourceName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResource(String resourceName, String state) {
    String key = getRoutingTableKey(DEFAULT_PROPERTY, DEFAULT_TYPE);
    return _routingTableRefMap.get(key).get().getInstancesForResource(resourceName, state);
  }

  public Set<InstanceConfig> getInstancesForResource(String resourceName, String state,
      PropertyType propertyType, String type) {
    String key = getRoutingTableKey(propertyType.name(), type);
    return _routingTableRefMap.get(key).get().getInstancesForResource(resourceName, state);
  }

  /**
   * returns all instances for all resources in {resource group} that are in a specific {state}
   * @param resourceGroupName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state) {
    String key = getRoutingTableKey(DEFAULT_PROPERTY, DEFAULT_TYPE);
    return _routingTableRefMap.get(key).get().getInstancesForResourceGroup(resourceGroupName, state);
  }

  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state,
      PropertyType propertyType, String type) {
    String key = getRoutingTableKey(propertyType.name(), type);
    return _routingTableRefMap.get(key).get().getInstancesForResourceGroup(resourceGroupName, state);
  }

  /**
   * returns all instances for resources contains any given tags in {resource group} that are in a
   * specific {state}
   * @param resourceGroupName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state,
      List<String> resourceTags) {
    String key = getRoutingTableKey(DEFAULT_PROPERTY, DEFAULT_TYPE);
    return _routingTableRefMap.get(key).get().getInstancesForResourceGroup(resourceGroupName, state,
        resourceTags);
  }

  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state,
      List<String> resourceTags, PropertyType propertyType, String type) {
    String key = getRoutingTableKey(propertyType.name(), type);
    return _routingTableRefMap.get(key).get().getInstancesForResourceGroup(resourceGroupName, state,
        resourceTags);
  }

  /**
   * Return all liveInstances in the cluster now.
   * @return
   */
  public Collection<LiveInstance> getLiveInstances() {
    // Since line instances will be the same across all _routingTableRefMap, here one of the keys
    // will be used without considering PropertyType
    String key = null;
    Iterator<String> iter = _routingTableRefMap.keySet().iterator();
    if (iter.hasNext()) {
      key = iter.next();
    }
    if (key == null) {
      throw new HelixException("There is no key available in this RoutingTableProvider.");
    }
    return _routingTableRefMap.get(key).get().getLiveInstances();
  }

  /**
   * Return all instance's config in this cluster.
   * @return
   */
  public Collection<InstanceConfig> getInstanceConfigs() {
    // Since line instances will be the same across all _routingTableRefMap, here one of the keys
    // will be used without considering PropertyType
    String key = null;
    Iterator<String> iter = _routingTableRefMap.keySet().iterator();
    if (iter.hasNext()) {
      key = iter.next();
    }
    if (key == null) {
      throw new HelixException("There is no key available in this RoutingTableProvider.");
    }
    return _routingTableRefMap.get(key).get().getInstanceConfigs();
  }

  /**
   * Return names of all resources (shown in ExternalView or CustomizedView) in this cluster.
   */
  public Collection<String> getResources() {
    String key = getRoutingTableKey(DEFAULT_PROPERTY, DEFAULT_TYPE);
    return _routingTableRefMap.get(key).get().getResources();
  }

  public Collection<String> getResources(PropertyType propertyType, String type) {
    String key = getRoutingTableKey(propertyType.name(), type);
    return _routingTableRefMap.get(key).get().getResources();
  }


  private String getRoutingTableKey(String propertyTypeName, String customizedStateType) {
    if (propertyTypeName.equals(DEFAULT_PROPERTY) && customizedStateType.equals(DEFAULT_TYPE)) {
      // Check whether there exist only one snapshot (_routingTableRefMap)
      if (_routingTableRefMap.keySet().size() == 1) {
        return _routingTableRefMap.keySet().iterator().next();
      } else {
        throw new HelixException("There is none or more than one RoutingTableSnapshot");
      }
    }

    if (!propertyTypeName.equals(DEFAULT_PROPERTY) && customizedStateType.equals(DEFAULT_TYPE)) {
      if (propertyTypeName.equals(PropertyType.CUSTOMIZEDVIEW.name())) {
        throw new HelixException("Specific type needs to be used for CUSTOMIZEDVIEW PropertyType");
      }
    }

    String key = propertyTypeName + "_" + customizedStateType;
    if (!_routingTableRefMap.containsKey(key)) {
      throw new HelixException(
          String.format("Currently there is no snapshot available for PropertyType %s and type %s",
              propertyTypeName, customizedStateType));
    }
    return key;

  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    HelixConstants.ChangeType changeType = changeContext.getChangeType();
    if (changeType != null && !_sourceDataTypeMap.containsKey(changeType.getPropertyType())) {
      logger.warn(
          "onExternalViewChange called with mismatched change types. Source data types does not contain changed data type: {}",
          changeType);
      return;
    }
    // Refresh with full list of external view.
    if (externalViewList != null && externalViewList.size() > 0) {
      // keep this here for back-compatibility, application can call onExternalViewChange directly
      // with externalview list supplied.
      String keyReference = PropertyType.EXTERNALVIEW.name() + "_" + DEFAULT_TYPE;
      refresh(externalViewList, Collections.emptyList(), changeContext, keyReference);
    } else {
      ClusterEventType eventType;
      if (_sourceDataTypeMap.containsKey(PropertyType.EXTERNALVIEW)) {
        eventType = ClusterEventType.ExternalViewChange;
      } else if (_sourceDataTypeMap.containsKey(PropertyType.TARGETEXTERNALVIEW)) {
        eventType = ClusterEventType.TargetExternalViewChange;
      } else {
        logger.warn(
            "onExternalViewChange called with mismatched change types. Source data types does not contain changed data type: {}",
            changeType);
        return;
      }
      _routerUpdater.queueEvent(changeContext, eventType, changeType);
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onInstanceConfigChange(List<InstanceConfig> configs,
      NotificationContext changeContext) {
    _routerUpdater.queueEvent(changeContext, ClusterEventType.InstanceConfigChange,
        HelixConstants.ChangeType.INSTANCE_CONFIG);
  }

  @Override
  @PreFetch(enabled = false)
  public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    onInstanceConfigChange(configs, changeContext);
  }

  @Override
  @PreFetch(enabled = true)
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    if (_sourceDataTypeMap.containsKey(PropertyType.CURRENTSTATES)) {
      // Go though the live instance list and update CurrentState listeners
      updateCurrentStatesListeners(liveInstances, changeContext);
    }
    _routerUpdater.queueEvent(changeContext, ClusterEventType.LiveInstanceChange,
        HelixConstants.ChangeType.LIVE_INSTANCE);
  }

  @Override
  @PreFetch(enabled = false)
  public void onStateChange(String instanceName, List<CurrentState> statesInfo,
      NotificationContext changeContext) {
    if (_sourceDataTypeMap.containsKey(PropertyType.CURRENTSTATES)) {
      _routerUpdater.queueEvent(changeContext, ClusterEventType.CurrentStateChange,
          HelixConstants.ChangeType.CURRENT_STATE);
    } else {
      logger.warn(
          "RoutingTableProvider does not use CurrentStates as source, ignore CurrentState changes!");
    }
    _routerUpdater.queueEvent(changeContext, ClusterEventType.CurrentStateChange,
        HelixConstants.ChangeType.CURRENT_STATE);
  }

  @Override
  @PreFetch(enabled = false)
  public void onCustomizedViewChange(List<CustomizedView> customizedViewList,
      NotificationContext changeContext) {
    _routerUpdater.queueEvent(changeContext, ClusterEventType.CustomizedViewChange,
        HelixConstants.ChangeType.CUSTOMIZED_VIEW);
  }

  final AtomicReference<Map<String, LiveInstance>> _lastSeenSessions = new AtomicReference<>();

  /**
   * Go through all live instances in the cluster, add CurrentStateChange listener to
   * them if they are newly added, and remove CurrentStateChange listener if instance is offline.
   */
  private void updateCurrentStatesListeners(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    HelixManager manager = changeContext.getManager();
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(manager.getClusterName());

    if (changeContext.getType() == NotificationContext.Type.FINALIZE) {
      // on finalize, should remove all current-state listeners
      logger.info("remove current-state listeners. lastSeenSessions: {}", _lastSeenSessions);
      liveInstances = Collections.emptyList();
    }

    Map<String, LiveInstance> curSessions = new HashMap<>();
    for (LiveInstance liveInstance : liveInstances) {
      curSessions.put(liveInstance.getEphemeralOwner(), liveInstance);
    }

    // Go though the live instance list and update CurrentState listeners
    synchronized (_lastSeenSessions) {
      Map<String, LiveInstance> lastSessions = _lastSeenSessions.get();
      if (lastSessions == null) {
        lastSessions = Collections.emptyMap();
      }

      // add listeners to new live instances
      for (String session : curSessions.keySet()) {
        if (!lastSessions.containsKey(session)) {
          String instanceName = curSessions.get(session).getInstanceName();
          try {
            // add current-state listeners for new sessions
            manager.addCurrentStateChangeListener(this, instanceName, session);
            logger.info(
                "{} added current-state listener for instance: {}, session: {}, listener: {}",
                manager.getInstanceName(), instanceName, session, this);
          } catch (Exception e) {
            logger.error("Fail to add current state listener for instance: {} with session: {}",
                instanceName, session, e);
          }
        }
      }

      // remove current-state listener for expired session
      for (String session : lastSessions.keySet()) {
        if (!curSessions.containsKey(session)) {
          String instanceName = lastSessions.get(session).getInstanceName();
          manager.removeListener(keyBuilder.currentStates(instanceName, session), this);
          logger.info("remove current-state listener for instance: {}, session: {}", instanceName,
              session);
        }
      }

      // update last-seen
      _lastSeenSessions.set(curSessions);
    }
  }

  private void reset() {
    logger.info("Resetting the routing table.");
    for (String key: _routingTableRefMap.keySet()) {
      PropertyType propertyType = _routingTableRefMap.get(key).get().getPropertyType();
      if (propertyType == PropertyType.CUSTOMIZEDVIEW) {
        String type = _routingTableRefMap.get(key).get().getCustomizedStateType();
        RoutingTable newRoutingTable = new CustomizedViewRoutingTable(propertyType, type);
        _routingTableRefMap.get(key).set(newRoutingTable);
      } else {
        RoutingTable newRoutingTable = new RoutingTable(propertyType);
        _routingTableRefMap.get(key).set(newRoutingTable);
      }
    }
  }

  protected void refresh(List<ExternalView> externalViewList,
      List<CustomizedView> customizedViewList, NotificationContext changeContext,
      String referenceKey) {
    HelixDataAccessor accessor = changeContext.getManager().getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    List<InstanceConfig> configList = accessor.getChildValues(keyBuilder.instanceConfigs());
    List<LiveInstance> liveInstances = accessor.getChildValues(keyBuilder.liveInstances());
    refresh(externalViewList, customizedViewList, configList, liveInstances, referenceKey);
  }

  protected void refresh(Collection<ExternalView> externalViews,
      Collection<CustomizedView> customizedViews, Collection<InstanceConfig> instanceConfigs,
      Collection<LiveInstance> liveInstances, String referenceKey) {
    long startTime = System.currentTimeMillis();
    PropertyType propertyType = _routingTableRefMap.get(referenceKey).get().getPropertyType();
    if (propertyType == PropertyType.CUSTOMIZEDVIEW) {
      String customizedStateType = _routingTableRefMap.get(referenceKey).get().getCustomizedStateType();
      RoutingTable newRoutingTable = new CustomizedViewRoutingTable(customizedViews,
          instanceConfigs, liveInstances, propertyType, customizedStateType);
      resetRoutingTableAndNotify(startTime, newRoutingTable, referenceKey);
    } else {
      RoutingTable newRoutingTable = new RoutingTable(externalViews, instanceConfigs,
          liveInstances, propertyType);
      resetRoutingTableAndNotify(startTime, newRoutingTable, referenceKey);
    }
  }

  protected void refresh(Map<String, Map<String, Map<String, CurrentState>>> currentStateMap,
      Collection<InstanceConfig> instanceConfigs, Collection<LiveInstance> liveInstances,
      String referenceKey) {
    long startTime = System.currentTimeMillis();
    RoutingTable newRoutingTable =
        new RoutingTable(currentStateMap, instanceConfigs, liveInstances);
    resetRoutingTableAndNotify(startTime, newRoutingTable, referenceKey);
  }

  private void resetRoutingTableAndNotify(long startTime, RoutingTable newRoutingTable, String referenceKey) {
    _routingTableRefMap.get(referenceKey).set(newRoutingTable);
    String clusterName = _helixManager != null ? _helixManager.getClusterName() : null;
    logger.info("Refreshed the RoutingTable for cluster {}, took {} ms.", clusterName,
        (System.currentTimeMillis() - startTime));

    // TODO: move the callback user code logic to separate thread upon routing table statePropagation latency
    // integration test result. If the latency is more than 2 secs, we need to change this part.
    notifyRoutingTableChange(clusterName, referenceKey);

    // Update timestamp for last refresh
    if (_isPeriodicRefreshEnabled) {
      _lastRefreshTimestamp = System.currentTimeMillis();
    }
  }

  private void notifyRoutingTableChange(String clusterName, String referenceKey) {
    // This call back is called in the main event queue of RoutingTableProvider. We add log to
    // record time spent
    // here. Potentially, we should call this callback in a separate thread if this is a bottleneck.
    long startTime = System.currentTimeMillis();
    for (Map.Entry<RoutingTableChangeListener, ListenerContext> entry : _routingTableChangeListenerMap
        .entrySet()) {
      entry.getKey().onRoutingTableChange(
          new RoutingTableSnapshot(_routingTableRefMap.get(referenceKey).get()),
          entry.getValue().getContext());
    }
    logger.info("RoutingTableProvider user callback time for cluster {}, took {} ms.", clusterName,
        (System.currentTimeMillis() - startTime));
  }

  private class RouterUpdater extends ClusterEventProcessor {
    private final RoutingDataCache _dataCache;
    private final Map<PropertyType, List<String>> _sourceDataTypeMap;

    public RouterUpdater(String clusterName, Map<PropertyType, List<String>> sourceDataTypeMap) {
      super(clusterName, "Helix-RouterUpdater-event_process");
      _sourceDataTypeMap = sourceDataTypeMap;
      _dataCache = new RoutingDataCache(clusterName, _sourceDataTypeMap);
    }

    @Override
    protected void handleEvent(ClusterEvent event) {
      NotificationContext changeContext = event.getAttribute(AttributeName.changeContext.name());
      HelixConstants.ChangeType changeType = changeContext.getChangeType();

      // Set cluster event id for later processing methods and it also helps debug.
      _dataCache.setClusterEventId(event.getEventId());

      if (changeContext == null || changeContext.getType() != NotificationContext.Type.CALLBACK) {
        _dataCache.requireFullRefresh();
      } else {
        _dataCache.notifyDataChange(changeType, changeContext.getPathChanged());
      }

      // session has expired clean up the routing table
      if (changeContext.getType() == NotificationContext.Type.FINALIZE) {
        reset();
      } else {
        // refresh routing table.
        HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
        if (manager == null) {
          logger.error(String.format("HelixManager is null for router update event: %s", event));
          throw new HelixException("HelixManager is null for router update event.");
        }
        if (!manager.isConnected()) {
          logger.error(String.format("HelixManager is not connected for router update event: %s", event));
          throw new HelixException("HelixManager is not connected for router update event.");
        }

        long startTime = System.currentTimeMillis();

        _dataCache.refresh(manager.getHelixDataAccessor());
        for (PropertyType propertyType : _sourceDataTypeMap.keySet()) {
          switch (propertyType) {
            case EXTERNALVIEW: {
              String keyReference = propertyType.name() + "_" + DEFAULT_TYPE;
              refresh(_dataCache.getExternalViews().values(), Collections.emptyList(),
                  _dataCache.getInstanceConfigMap().values(), _dataCache.getLiveInstances().values(), keyReference);
            }
              break;
            case TARGETEXTERNALVIEW: {
              String keyReference = propertyType.name() + "_" + DEFAULT_TYPE;
              refresh(_dataCache.getTargetExternalViews().values(), Collections.emptyList(),
                  _dataCache.getInstanceConfigMap().values(), _dataCache.getLiveInstances().values(), keyReference);
            }
              break;
            case CUSTOMIZEDVIEW:
              for (String customizedStateType : _sourceDataTypeMap.getOrDefault(PropertyType.CUSTOMIZEDVIEW, Collections.emptyList())) {
                String keyReference = propertyType.name() + "_" + customizedStateType;
                refresh(Collections.emptyList(), _dataCache.getCustomizedView(customizedStateType).values(),
                    _dataCache.getInstanceConfigMap().values(), _dataCache.getLiveInstances().values(), keyReference);
              }
              break;
            case CURRENTSTATES: {
              String keyReference = propertyType.name() + "_" + DEFAULT_TYPE;
              refresh(_dataCache.getCurrentStatesMap(), _dataCache.getInstanceConfigMap().values(),
                  _dataCache.getLiveInstances().values(), keyReference);
              recordPropagationLatency(System.currentTimeMillis(), _dataCache.getCurrentStateSnapshot());
            }
              break;
            default:
              logger.warn("Unsupported source data type: {}, stop refreshing the routing table!", propertyType);
          }

          _monitorMap.get(propertyType).increaseDataRefreshCounters(startTime);
        }
      }
    }

    /**
     * Report current state to routing table propagation latency
     * This method is not threadsafe. Take care of _reportingTask atomicity if use in multi-threads.
     */
    private void recordPropagationLatency(final long currentTime,
        final CurrentStateSnapshot currentStateSnapshot) {
      // Note that due to the extra mem footprint introduced by currentStateSnapshot ref, we
      // restrict running report task count to be 1.
      // Any parallel tasks will be skipped. So the reporting metric data is sampled.
      if (_reportingTask == null || _reportingTask.isDone()) {
        _reportingTask = _reportExecutor.submit(new Callable<Object>() {
          @Override
          public Object call() {
            // getNewCurrentStateEndTimes() needs to iterate all current states. Make it async to
            // avoid performance impact.
            Map<PropertyKey, Map<String, Long>> currentStateEndTimeMap =
                currentStateSnapshot.getNewCurrentStateEndTimes();
            for (PropertyKey key : currentStateEndTimeMap.keySet()) {
              Map<String, Long> partitionStateEndTimes = currentStateEndTimeMap.get(key);
              for (String partition : partitionStateEndTimes.keySet()) {
                long endTime = partitionStateEndTimes.get(partition);
                if (currentTime >= endTime) {
                  for (PropertyType propertyType : _sourceDataTypeMap.keySet()) {
                    _monitorMap.get(propertyType).recordStatePropagationLatency(currentTime - endTime);
                    logger.debug(
                        "CurrentState updated in the routing table. Node Key {}, Partition {}, end time {}, Propagation latency {}",
                        key.toString(), partition, endTime, currentTime - endTime);
                  }
                } else {
                  // Verbose log in case currentTime < endTime. This could be the case that Router
                  // clock is slower than the participant clock.
                  logger.trace(
                      "CurrentState updated in the routing table. Node Key {}, Partition {}, end time {}, Propagation latency {}",
                      key.toString(), partition, endTime, currentTime - endTime);
                }
              }
            }
            return null;
          }
        });
      }
    }


    public void queueEvent(NotificationContext context, ClusterEventType eventType,
        HelixConstants.ChangeType changeType) {
      ClusterEvent event = new ClusterEvent(_clusterName, eventType);

      // Null check for manager in the following line is done in handleEvent()
      event.addAttribute(AttributeName.helixmanager.name(), context.getManager());
      event.addAttribute(AttributeName.changeContext.name(), context);
      queueEvent(event);
      for (PropertyType propertyType : _monitorMap.keySet()) {
        _monitorMap.get(propertyType).increaseCallbackCounters(_eventQueue.size());
      }
    }
  }

  protected class ListenerContext {
    private Object _context;

    public ListenerContext(Object context) {
      _context = context;
    }

    public Object getContext() {
      return _context;
    }
  }
}
