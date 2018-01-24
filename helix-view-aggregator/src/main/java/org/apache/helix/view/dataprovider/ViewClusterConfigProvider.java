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
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.common.ClusterEventProcessor;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.model.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class keeps an updated version of cluster config of the view cluster, notifies the event
 * processor it is designated about view cluster config change, and provides methods to help adjust
 * cluster settings.
 */
public class ViewClusterConfigProvider implements ClusterConfigChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(ViewClusterConfigProvider.class);
  private String _viewClusterName;
  private HelixManager _helixManager;
  protected ClusterConfig _viewClusterConfig;
  private ClusterEventProcessor _eventProcessor;

  public ViewClusterConfigProvider(String clusterName, HelixManager manager,
      ClusterEventProcessor eventProcessor) {
    _viewClusterName = clusterName;
    _helixManager = manager;
    _eventProcessor = eventProcessor;
  }

  /**
   * Set up ViewClusterConfigProvider. After setting up, the class should start listening
   * on change events and perform corresponding reactions
   */
  public void setup() {
    logger.info("Setting up ViewClusterConfigProvider for view cluster %s ...", _viewClusterName);
    try {
      _helixManager.addClusterfigChangeListener(this);
    } catch (Exception e) {
      throw new HelixException(
          "ViewClusterConfigProvider: Failed to attach listeners to HelixManager!", e);
    }
  }

  /**
   * This class takes a existing list of source cluster configs and generate lists of source
   * cluster configs to add / delete / modify, and tell caller if ViewClusterRefreshTimer should
   * be reset or not
   */
  public class SourceClusterConfigChangeAction {
    private List<ViewClusterSourceConfig> _oldConfigList;
    private long _oldRefreshPeriodMs;

    private List<ViewClusterSourceConfig> _toAdd;
    private List<ViewClusterSourceConfig> _toDelete;
    private List<ViewClusterSourceConfig> _toModify;
    private boolean _refreshPeriodChanged;

    public SourceClusterConfigChangeAction(List<ViewClusterSourceConfig> oldConfigList,
        long oldRefreshPeriodMs) {
      _oldConfigList = oldConfigList;
      _oldRefreshPeriodMs = oldRefreshPeriodMs;
    }

    /**
     * Compute actions and generate toAdd, toDelete toModify, and refreshPeriodChanged.
     */
    public void computeAction() {
      // TODO: implement logic
    }

    public List<ViewClusterSourceConfig> getConfigsToAdd() {
      return _toAdd;
    }

    public List<ViewClusterSourceConfig> getConfigsToDelete() {
      return _toDelete;
    }

    public List<ViewClusterSourceConfig> getConfigsToModify() {
      return _toModify;
    }

    public boolean shouldResetTimer() {
      return _refreshPeriodChanged;
    }
  }

  public synchronized long getViewClusterRefreshPeriodMs() {
    return _viewClusterConfig.getViewClusterRefershPeriod() * 1000;
  }

  public synchronized SourceClusterConfigChangeAction getSourceConfigChangeAction(
      List<ViewClusterSourceConfig> oldConfigList, long oldRefreshPeriodMs) {
    return new SourceClusterConfigChangeAction(oldConfigList, oldRefreshPeriodMs);
  }

  @Override
  public void onClusterConfigChange(ClusterConfig clusterConfig,
      NotificationContext context) {
    // TODO: we assume its a view cluster config here. Error handling if not?
    refreshViewClusterConfig(clusterConfig);
    // Source cluster config will not be aggregated so here ClusterConfigChange infers view cluster
    ClusterEvent event = new ClusterEvent(_viewClusterName, ClusterEventType.ClusterConfigChange);
    _eventProcessor.queueEvent(event);
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("ViewClusterConfigProvider: queued event %s", event.toString()));
    }
  }

  protected synchronized void refreshViewClusterConfig(ClusterConfig clusterConfig) {
    _viewClusterConfig = clusterConfig;
  }
}
