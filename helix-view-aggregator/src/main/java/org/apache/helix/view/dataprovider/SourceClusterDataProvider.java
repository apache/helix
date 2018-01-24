package org.apache.helix.view.dataprovider;

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

import java.util.List;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.common.BasicClusterDataCache;
import org.apache.helix.common.ClusterEventProcessor;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;

/**
 * SourceClusterDataProvider listens to changes in 1 source cluster, notifies cluster data cache,
 * generates event for event processor and provide methods to read data cache
 */
public class SourceClusterDataProvider extends BasicClusterDataCache
    implements InstanceConfigChangeListener, LiveInstanceChangeListener,
    ExternalViewChangeListener {
  private HelixManager _helixManager;
  private HelixDataAccessor _dataAccessor;
  private PropertyKey.Builder _propertyKeyBuilder;
  private ViewClusterSourceConfig _sourceClusterConfig;
  private ClusterEventProcessor _eventProcessor;

  public SourceClusterDataProvider(ViewClusterSourceConfig config,
      ClusterEventProcessor eventProcessor) {
    super(config.getName());
    _eventProcessor = eventProcessor;
    _sourceClusterConfig = config;
    _helixManager = HelixManagerFactory
        .getZKHelixManager(config.getName(), generateHelixManagerInstanceName(config.getName()),
            InstanceType.SPECTATOR, config.getZkAddress());
    requireFullRefreshBasedOnConfig();
  }

  public String getName() {
    return _helixManager.getInstanceName();
  }

  public ViewClusterSourceConfig getSourceClusterConfig() {
    return _sourceClusterConfig;
  }

  /**
   * Set up ClusterDataProvider. After setting up, the class should start listening
   * on change events and perform corresponding reactions
   * @throws Exception
   */
  public void setup() throws Exception {
    try {
      LOG.info(String.format("%s setting up ...", _helixManager.getInstanceName()));
      _helixManager.connect();
      _helixManager.addInstanceConfigChangeListener(this);
      _helixManager.addLiveInstanceChangeListener(this);
      _helixManager.addExternalViewChangeListener(this);
      _dataAccessor = _helixManager.getHelixDataAccessor();
      _propertyKeyBuilder = _dataAccessor.keyBuilder();
      LOG.info(String.format("%s started.", _helixManager.getInstanceName()));
    } catch(Exception e) {
      shutdown();
      throw e;
    }
  }

  public void shutdown() {
    // Clear cache to make sure memory is released
    for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
      clearCache(changeType);
    }
    if (_helixManager != null && _helixManager.isConnected()) {
      try {
        _helixManager.disconnect();
      } catch (ZkInterruptedException e) {
        // OK
      }
    }
  }

  public void refreshCache() {
    refresh(_dataAccessor);
  }

  /**
   * Re-list current instance config names. ListName is a more reliable way to find
   * current instance config names. This is needed for ViewClusterRefresher when
   * it is generating diffs to push to ViewCluster
   * @return
   */
  public List<String> listInstanceConfigNames() {
    return _dataAccessor.getChildNames(_propertyKeyBuilder.instanceConfigs());
  }

  /**
   * Re-list current live instance names.
   * @return
   */
  public List<String> listLiveInstanceNames() {
    return _dataAccessor.getChildNames(_propertyKeyBuilder.liveInstances());
  }

  /**
   * re-list external view names
   * @return
   */
  public List<String> listExternalViewNames() {
    return _dataAccessor.getChildNames(_propertyKeyBuilder.externalViews());
  }

  /**
   * Based on current ViewClusterSourceConfig, decide whether caller should
   * read instance config. Used by ViewClusterRefresher
   * @return
   */
  public boolean shouldReadInstanceConfigs() {
    // TODO: implement logic
    return false;
  }

  /**
   * Based on current ViewClusterSourceConfig, decide whether caller should
   * read live instances. Used by ViewClusterRefresher
   * @return
   */
  public boolean shouldReadLiveInstances() {
    // TODO: implement logic
    return false;
  }

  /**
   * Based on current ViewClusterSourceConfig, decide whether caller should
   * read external view. Used by ViewClusterRefresher
   * @return
   */
  public boolean shouldReadExternalViews() {
    // TODO: implement logic
    return false;
  }

  @Override
  @PreFetch(enabled = false)
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
      NotificationContext context) {
    queueEventToProcessor(context, ClusterEventType.InstanceConfigChange,
        HelixConstants.ChangeType.INSTANCE_CONFIG);
  }

  @Override
  @PreFetch(enabled = false)
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    queueEventToProcessor(changeContext, ClusterEventType.LiveInstanceChange,
        HelixConstants.ChangeType.LIVE_INSTANCE);
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    queueEventToProcessor(changeContext, ClusterEventType.ExternalViewChange,
        HelixConstants.ChangeType.EXTERNAL_VIEW);
  }

  private void queueEventToProcessor(NotificationContext context,
      ClusterEventType clusterEventType, HelixConstants.ChangeType cacheChangeType)
      throws IllegalStateException {
    if (!shouldProcessEvent(cacheChangeType)) {
      LOG.info(String.format(
          "Skip processing event based on ViewClusterSourceConfig: ClusterName=%s; ClusterEventType=%s, ChangeType=%s",
          _clusterName, clusterEventType, cacheChangeType));
      return;
    }
    ClusterEvent event = new ClusterEvent(_clusterName, clusterEventType);
    if (context != null && context.getType() != NotificationContext.Type.FINALIZE) {
      notifyDataChange(cacheChangeType);
      _eventProcessor.queueEvent(event);
    } else {
      LOG.info(String.format("SourceClusterDataProvider: skip queuing event %s", event));
    }
  }

  /**
   * Replace current source cluster config properties (A list of PropertyType we should aggregate)
   * with the given ones, and update corresponding provider mechanisms
   * @param properties
   */
  public synchronized void setSourceClusterConfigProperty(List<PropertyType> properties) {
    // TODO: implement logic
  }

  /**
   * Based on current ViewClusterSourceConfig, notify cache accordingly.
   */
  private void requireFullRefreshBasedOnConfig() {
    // TODO: implement logic
  }

  /**
   * Check source cluster config and decide whether this event should be processed
   * @param sourceClusterChangeType
   * @return
   */
  private synchronized boolean shouldProcessEvent(
      HelixConstants.ChangeType sourceClusterChangeType) {
    // TODO: implement
    return false;
  }

  private static String generateHelixManagerInstanceName(String clusterName) {
    return String.format("SourceClusterSpectatorHelixManager-%s", clusterName);
  }
}
