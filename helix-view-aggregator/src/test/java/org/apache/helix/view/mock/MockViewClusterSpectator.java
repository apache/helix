package org.apache.helix.view.mock;

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
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyType;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.view.common.ClusterViewEvent;

/**
 * MockViewClusterSpectator monitors change in view cluster. When event happens, it push event to
 * MockClusterEventProcessor which records event change count in view cluster
 */
public class MockViewClusterSpectator implements ExternalViewChangeListener,
    InstanceConfigChangeListener, LiveInstanceChangeListener{
  private final HelixManager _manager;
  private final MockClusterEventProcessor _eventProcessor;
  private final String _viewClusterName;
  private final HelixDataAccessor _dataAccessor;

  public MockViewClusterSpectator(String viewClusterName, String zkAddr) throws Exception {
    _viewClusterName = viewClusterName;
    _eventProcessor = new MockClusterEventProcessor(viewClusterName);
    _eventProcessor.start();
    _manager = HelixManagerFactory
        .getZKHelixManager(viewClusterName, "MockViewClusterSpectator-" + viewClusterName,
            InstanceType.SPECTATOR, zkAddr);
    _manager.connect();
    _manager.addExternalViewChangeListener(this);
    _manager.addLiveInstanceChangeListener(this);
    _manager.addInstanceConfigChangeListener(this);
    _dataAccessor = _manager.getHelixDataAccessor();
  }

  public void shutdown() throws Exception {
    _eventProcessor.interrupt();
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    _eventProcessor.queueEvent(ClusterViewEvent.Type.ExternalViewChange,
        new ClusterViewEvent(_viewClusterName, ClusterViewEvent.Type.ExternalViewChange));
  }

  @Override
  @PreFetch(enabled = false)
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
      NotificationContext context) {
    _eventProcessor.queueEvent(ClusterViewEvent.Type.InstanceConfigChange,
        new ClusterViewEvent(_viewClusterName, ClusterViewEvent.Type.InstanceConfigChange));
  }

  @Override
  @PreFetch(enabled = false)
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    _eventProcessor.queueEvent(ClusterViewEvent.Type.LiveInstanceChange,
        new ClusterViewEvent(_viewClusterName, ClusterViewEvent.Type.LiveInstanceChange));
  }

  public int getLiveInstanceChangeCount() {
    return _eventProcessor.getHandledLiveInstancesChangeCount();
  }

  public int getInstanceConfigChangeCount() {
    return _eventProcessor.getHandledInstanceConfigChangeCount();
  }

  public int getExternalViewChangeCount() {
    return _eventProcessor.getHandledExternalViewChangeCount();
  }

  public void reset() {
    _eventProcessor.resetHandledEventCount();
  }

  public List<String> getPropertyNamesFromViewCluster(PropertyType propertyType) {
    switch (propertyType) {
    case INSTANCES:
      return _dataAccessor.getChildNames(_dataAccessor.keyBuilder().instanceConfigs());
    case LIVEINSTANCES:
      return _dataAccessor.getChildNames(_dataAccessor.keyBuilder().liveInstances());
    case EXTERNALVIEW:
      return _dataAccessor.getChildNames(_dataAccessor.keyBuilder().externalViews());
    default:
      return null;
    }
  }

}
