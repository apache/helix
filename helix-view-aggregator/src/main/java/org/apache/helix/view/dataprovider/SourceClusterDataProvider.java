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
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.common.caches.BasicClusterDataCache;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.view.common.ClusterViewEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SourceClusterDataProvider listens to changes in 1 source cluster, notifies cluster data cache,
 * generates event for event processor and provide methods to read data cache
 */
public class SourceClusterDataProvider extends BasicClusterDataCache
    implements InstanceConfigChangeListener, LiveInstanceChangeListener,
    ExternalViewChangeListener {
  private static final Logger LOG = LoggerFactory.getLogger(SourceClusterDataProvider.class);

  private final HelixManager _helixManager;
  private final DedupEventProcessor<ClusterViewEvent.Type, ClusterViewEvent> _eventProcessor;

  protected ViewClusterSourceConfig _sourceClusterConfig;
  private HelixDataAccessor _dataAccessor;
  private PropertyKey.Builder _propertyKeyBuilder;

  public SourceClusterDataProvider(ViewClusterSourceConfig config,
      DedupEventProcessor<ClusterViewEvent.Type, ClusterViewEvent> eventProcessor) {
    super(config.getName());
    _eventProcessor = eventProcessor;
    _sourceClusterConfig = config;
    _helixManager = HelixManagerFactory.getZKHelixManager(config.getName(),
        generateHelixManagerInstanceName(config.getName()),
        InstanceType.SPECTATOR, config.getZkAddress());
  }

  public String getName() {
    return _helixManager.getInstanceName();
  }

  /**
   * Set up ClusterDataProvider. After setting up, the class should start listening
   * on change events and perform corresponding reactions
   * @throws Exception
   */
  public void setup() throws Exception {
    if (_helixManager != null && _helixManager.isConnected()) {
      LOG.info(String.format("Data provider %s is already setup", _helixManager.getInstanceName()));
      return;
    }
    try {
      _helixManager.connect();
      for (PropertyType property : _sourceClusterConfig.getProperties()) {
        HelixConstants.ChangeType changeType;
        switch (property) {
        case INSTANCES:
          _helixManager.addInstanceConfigChangeListener(this);
          changeType = HelixConstants.ChangeType.INSTANCE_CONFIG;
          break;
        case LIVEINSTANCES:
          _helixManager.addLiveInstanceChangeListener(this);
          changeType = HelixConstants.ChangeType.LIVE_INSTANCE;
          break;
        case EXTERNALVIEW:
          _helixManager.addExternalViewChangeListener(this);
          changeType = HelixConstants.ChangeType.EXTERNAL_VIEW;
          break;
        default:
          LOG.warn(String
              .format("Unsupported property type: %s. Skip adding listener", property.name()));
          continue;
        }
        notifyDataChange(changeType);
      }
      _dataAccessor = _helixManager.getHelixDataAccessor();
      _propertyKeyBuilder = _dataAccessor.keyBuilder();
      LOG.info(String.format("Data provider %s (%s) started. Source cluster detail: %s",
          _helixManager.getInstanceName(), hashCode(),
              _sourceClusterConfig.toString()));
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
        LOG.info(
            String.format("Data provider %s (%s) shutdown cleanly.", _helixManager.getInstanceName(), hashCode()));
      } catch (ZkInterruptedException e) {
        // OK
      }
    }
  }

  public void refreshCache() {
    refresh(_dataAccessor);
  }

  /**
   * Get current instance config names. ListName is a more reliable way to find
   * current instance config names. This is needed for ViewClusterRefresher when
   * it is generating diffs to push to ViewCluster
   * @return
   */
  public List<String> getInstanceConfigNames() {
    return _dataAccessor.getChildNames(_propertyKeyBuilder.instanceConfigs());
  }

  /**
   * Get current live instance names.
   * @return
   */
  public List<String> getLiveInstanceNames() {
    return _dataAccessor.getChildNames(_propertyKeyBuilder.liveInstances());
  }

  /**
   * Get external view names
   * @return
   */
  public List<String> getExternalViewNames() {
    return _dataAccessor.getChildNames(_propertyKeyBuilder.externalViews());
  }

  public List<PropertyType> getPropertiesToAggregate() {
    return _sourceClusterConfig.getProperties();
  }

  @Override
  @PreFetch(enabled = false)
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
      NotificationContext context) {
    queueEvent(context, ClusterViewEvent.Type.InstanceConfigChange,
        HelixConstants.ChangeType.INSTANCE_CONFIG);
  }

  @Override
  @PreFetch(enabled = false)
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    queueEvent(changeContext, ClusterViewEvent.Type.LiveInstanceChange,
        HelixConstants.ChangeType.LIVE_INSTANCE);
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    queueEvent(changeContext, ClusterViewEvent.Type.ExternalViewChange,
        HelixConstants.ChangeType.EXTERNAL_VIEW);
  }

  private void queueEvent(NotificationContext context, ClusterViewEvent.Type changeType,
      HelixConstants.ChangeType cacheChangeType)
      throws IllegalStateException {
    // TODO: in case of FINALIZE, if we are not shutdown, re-connect helix manager and report error
    if (context != null && context.getType() != NotificationContext.Type.FINALIZE) {
      notifyDataChange(cacheChangeType);
      _eventProcessor.queueEvent(changeType, new ClusterViewEvent(_clusterName, changeType));
    } else {
      LOG.info("Skip queuing event from source cluster {}. ChangeType: {}, ContextType: {}",
          _clusterName, cacheChangeType.name(),
          context == null ? "NoContext" : context.getType().name());
    }
  }

  private static String generateHelixManagerInstanceName(String clusterName) {
    return String.format("SourceClusterSpectatorHelixManager-%s", clusterName);
  }

  @Override
  public String toString() {
    return String.format("%s::%s", getClass().getSimpleName(), hashCode());
  }
}
