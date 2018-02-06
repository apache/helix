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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyType;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.common.ClusterEventProcessor;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingTableProvider implements ExternalViewChangeListener, InstanceConfigChangeListener,
    ConfigChangeListener, LiveInstanceChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(RoutingTableProvider.class);
  private final AtomicReference<RoutingTable> _routingTableRef;
  private final HelixManager _helixManager;
  private final RouterUpdater _routerUpdater;
  private final PropertyType _sourceDataType;

  public RoutingTableProvider() {
    this(null);
  }

  public RoutingTableProvider(HelixManager helixManager) throws HelixException {
    this(helixManager, PropertyType.EXTERNALVIEW);
  }

  public RoutingTableProvider(HelixManager helixManager, PropertyType sourceDataType) throws HelixException {
    _routingTableRef = new AtomicReference<>(new RoutingTable());
    _helixManager = helixManager;
    _sourceDataType = sourceDataType;
    String clusterName = _helixManager != null ? _helixManager.getClusterName() : null;
    _routerUpdater = new RouterUpdater(clusterName, _sourceDataType);
    _routerUpdater.start();
    if (_helixManager != null) {
      try {
        switch (_sourceDataType) {
        case EXTERNALVIEW:
          _helixManager.addExternalViewChangeListener(this);
          break;
        case TARGETEXTERNALVIEW:
          // Check whether target external has been enabled or not
          if (!_helixManager.getHelixDataAccessor().getBaseDataAccessor().exists(
              _helixManager.getHelixDataAccessor().keyBuilder().targetExternalViews().getPath(),
              0)) {
            throw new HelixException("Target External View is not enabled!");
          }
          _helixManager.addTargetExternalViewChangeListener(this);
          break;
        default:
          throw new HelixException("Unsupported source data type: " + sourceDataType);
        }
        _helixManager.addInstanceConfigChangeListener(this);
        _helixManager.addLiveInstanceChangeListener(this);
      } catch (Exception e) {
        logger.error("Failed to attach listeners to HelixManager!");
        throw new HelixException("Failed to attach listeners to HelixManager!", e);
      }
    }
  }

  /**
   * returns the instances for {resource,partition} pair that are in a specific
   * {state}
   *
   * This method will be deprecated, please use the
   * {@link #getInstancesForResource(String, String, String)} getInstancesForResource} method.
   * @param resourceName
   *          -
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstances(String resourceName, String partitionName, String state) {
    return getInstancesForResource(resourceName, partitionName, state);
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
  public List<InstanceConfig> getInstancesForResource(String resourceName, String partitionName, String state) {
    return _routingTableRef.get().getInstancesForResource(resourceName, partitionName, state);
  }

  /**
   * returns the instances for {resource group,partition} pair in all resources belongs to the given
   * resource group that are in a specific {state}.
   *
   * The return results aggregate all partition states from all the resources in the given resource
   * group.
   *
   * @param resourceGroupName
   * @param partitionName
   * @param state
   *
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName,
      String partitionName, String state) {
    return _routingTableRef.get().getInstancesForResourceGroup(resourceGroupName, partitionName, state);
  }

  /**
   * returns the instances for {resource group,partition} pair contains any of the given tags
   * that are in a specific {state}.
   *
   * Find all resources belongs to the given resource group that have any of the given resource tags
   * and return the aggregated partition states from all these resources.
   *
   * @param resourceGroupName
   * @param partitionName
   * @param state
   * @param resourceTags
   *
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName,
      String partitionName, String state, List<String> resourceTags) {
    return _routingTableRef.get()
        .getInstancesForResourceGroup(resourceGroupName, partitionName, state, resourceTags);
  }

  /**
   * returns all instances for {resource} that are in a specific {state}
   *
   * This method will be deprecated, please use the
   * {@link #getInstancesForResource(String, String) getInstancesForResource} method.
   * @param resourceName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstances(String resourceName, String state) {
    return getInstancesForResource(resourceName, state);
  }

  /**
   * returns all instances for {resource} that are in a specific {state}.
   * @param resourceName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResource(String resourceName, String state) {
    return _routingTableRef.get().getInstancesForResource(resourceName, state);
  }

  /**
   * returns all instances for all resources in {resource group} that are in a specific {state}
   *
   * @param resourceGroupName
   * @param state
   *
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state) {
    return _routingTableRef.get().getInstancesForResourceGroup(resourceGroupName, state);
  }

  /**
   * returns all instances for resources contains any given tags in {resource group} that are in a
   * specific {state}
   *
   * @param resourceGroupName
   * @param state
   *
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResourceGroup(String resourceGroupName, String state,
      List<String> resourceTags) {
    return _routingTableRef.get().getInstancesForResourceGroup(resourceGroupName, state,
        resourceTags);
  }

  /**
   * Return all liveInstances in the cluster now.
   * @return
   */
  public Collection<LiveInstance> getLiveInstances() {
    return _routingTableRef.get().getLiveInstances();
  }

  /**
   * Return all instance's config in this cluster.
   * @return
   */
  public Collection<InstanceConfig> getInstanceConfigs() {
    return _routingTableRef.get().getInstanceConfigs();
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    HelixConstants.ChangeType changeType = changeContext.getChangeType();
    if (changeType != null && !changeType.getPropertyType().equals(_sourceDataType)) {
      logger.warn("onExternalViewChange called with dis-matched change types. Source data type "
          + _sourceDataType + ", changed data type: " + changeType);
      return;
    }
    // Refresh with full list of external view.
    // keep this here for back-compatibility
    if (externalViewList != null && externalViewList.size() > 0) {
      refresh(externalViewList, changeContext);
    } else {
      _routerUpdater.queueEvent(changeContext, ClusterEventType.ExternalViewChange,
          HelixConstants.ChangeType.EXTERNAL_VIEW);
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
  public void onConfigChange(List<InstanceConfig> configs,
      NotificationContext changeContext) {
    onInstanceConfigChange(configs, changeContext);
  }

  @Override
  @PreFetch(enabled = false)
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    _routerUpdater.queueEvent(changeContext, ClusterEventType.LiveInstanceChange,
        HelixConstants.ChangeType.LIVE_INSTANCE);
  }

  private void reset() {
    logger.info("Resetting the routing table.");
    RoutingTable newRoutingTable = new RoutingTable();
    _routingTableRef.set(newRoutingTable);
  }

  public void refresh(List<ExternalView> externalViewList, NotificationContext changeContext) {
    HelixDataAccessor accessor = changeContext.getManager().getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    List<InstanceConfig> configList = accessor.getChildValues(keyBuilder.instanceConfigs());
    List<LiveInstance> liveInstances = accessor.getChildValues(keyBuilder.liveInstances());
    refresh(externalViewList, configList, liveInstances);
  }

  public void refresh(Collection<ExternalView> externalViews,
      Collection<InstanceConfig> instanceConfigs, Collection<LiveInstance> liveInstances) {
    RoutingTable newRoutingTable = new RoutingTable(externalViews, instanceConfigs, liveInstances);
    _routingTableRef.set(newRoutingTable);
  }

  private class RouterUpdater extends ClusterEventProcessor {
    private final RoutingDataCache _dataCache;

    public RouterUpdater(String clusterName, PropertyType sourceDataType) {
      super("Helix-RouterUpdater-event_process");
      _dataCache = new RoutingDataCache(clusterName, sourceDataType);
    }

    @Override
    protected void handleEvent(ClusterEvent event) {
      NotificationContext changeContext = event.getAttribute(AttributeName.changeContext.name());
      // session has expired clean up the routing table
      if (changeContext.getType() == NotificationContext.Type.FINALIZE) {
        reset();
      } else {
        // refresh routing table.
        HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
        if (manager == null) {
          logger.error("HelixManager is null for router update event : " + event);
          throw new HelixException("HelixManager is null for router update event.");
        }
        _dataCache.refresh(manager.getHelixDataAccessor());
        refresh(_dataCache.getExternalViews().values(), _dataCache.getInstanceConfigMap().values(),
            _dataCache.getLiveInstances().values());
      }
    }

    public void queueEvent(NotificationContext context, ClusterEventType eventType,
        HelixConstants.ChangeType changeType) {
      ClusterEvent event = new ClusterEvent(_clusterName, eventType);
      if (context == null || context.getType() != NotificationContext.Type.CALLBACK) {
        _dataCache.requireFullRefresh();
      } else {
        _dataCache.notifyDataChange(changeType, context.getPathChanged());
      }

      event.addAttribute(AttributeName.helixmanager.name(), context.getManager());
      event.addAttribute(AttributeName.changeContext.name(), context);
      queueEvent(event);
    }
  }
}
