package org.apache.helix.view.aggregator;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.common.ClusterEventProcessor;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.view.common.ViewAggregatorEventAttributes;
import org.apache.helix.view.dataprovider.SourceClusterDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main logic for Helix view aggregator
 */
public class HelixViewAggregator implements ClusterConfigChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(HelixViewAggregator.class);
  private static final int PROCESS_VIEW_CONFIG_CHANGE_BACKOFF_MS = 3 * 1000;
  private final String _viewClusterName;
  private final HelixManager _viewClusterManager;
  private HelixDataAccessor _dataAccessor;

  // Worker that processes source cluster events and refresh view cluster
  private ClusterEventProcessor _aggregator;
  private boolean _refreshViewCluster;

  // Worker that processes view cluster config change
  private ClusterEventProcessor _viewConfigProcessor;

  private Map<String, SourceClusterDataProvider> _dataProviderMap;
  private ClusterConfig _curViewClusterConfig;
  private Timer _viewClusterRefreshTimer;
  private ViewClusterRefresher _viewClusterRefresher;

  public HelixViewAggregator(String viewClusterName, String zkAddr) {
    _viewClusterName = viewClusterName;
    _dataProviderMap = new HashMap<>();
    _viewClusterManager = HelixManagerFactory
        .getZKHelixManager(_viewClusterName, generateHelixManagerInstanceName(_viewClusterName),
            InstanceType.SPECTATOR, zkAddr);
    _refreshViewCluster = false;
    _aggregator = new ClusterEventProcessor(_viewClusterName, "Aggregator") {
      @Override
      public void handleEvent(ClusterEvent event) {
        handleSourceClusterEvent(event);
      }
    };

    _viewConfigProcessor = new ClusterEventProcessor(_viewClusterName, "ViewConfigProcessor") {
      @Override
      public void handleEvent(ClusterEvent event) {
        handleViewClusterConfigChange(event);
      }
    };
  }

  /**
   * Start controller main logic
   * @throws Exception
   */
  public void start() throws Exception {
    // Start workers
    _aggregator.start();
    _viewConfigProcessor.start();

    // Setup manager
    try {
      _viewClusterManager.connect();
      _viewClusterManager.addClusterfigChangeListener(this);
      _dataAccessor = _viewClusterManager.getHelixDataAccessor();
    } catch (Exception e) {
      throw new HelixException("Failed to connect view cluster helix manager", e);
    }

    // Set up view cluster refresher
    _viewClusterRefresher =
        new ViewClusterRefresher(_viewClusterName, _viewClusterManager.getHelixDataAccessor(),
            _dataProviderMap);
  }

  public void shutdown() {
    // Stop all workers
    _aggregator.interrupt();
    _viewConfigProcessor.interrupt();

    // Stop timer
    if (_viewClusterRefreshTimer != null) {
      logger.info("Shutting down view cluster refresh timer");
      _viewClusterRefreshTimer.cancel();
    }

    // disconnect manager
    if (_viewClusterManager != null && _viewClusterManager.isConnected()) {
      logger.info("Shutting down view cluster helix manager");
      try {
        _viewClusterManager.disconnect();
      } catch (ZkInterruptedException zkintr) {
        logger.warn("ZK interrupted when disconnecting helix manager", zkintr);
      } catch (Exception e) {
        logger.error(String
            .format("Failed to disconnect helix manager for view cluster %s", _viewClusterName), e);
      }
    }

    // Clean up all data providers
    for (SourceClusterDataProvider provider : _dataProviderMap.values()) {
      logger
          .info(String.format("Shutting data provider for source cluster %s", provider.getName()));
      try {
        provider.shutdown();
      } catch (Exception e) {
        logger.error(String
            .format("Failed to shutdown data provider %s for view cluster %s", provider.getName(),
                _viewClusterName), e);
      }
    }
    logger.info("HelixViewAggregator shutdown cleanly");
  }

  @Override
  @PreFetch(enabled = false)
  public void onClusterConfigChange(ClusterConfig clusterConfig, NotificationContext context) {
    if (context != null && context.getType() != NotificationContext.Type.FINALIZE) {
      _viewConfigProcessor
          .queueEvent(new ClusterEvent(_viewClusterName, ClusterEventType.ClusterConfigChange));
    } else {
      logger.info(String
          .format("Skip processing view cluster config change with notification context type %s",
              context == null ? "NoContext" : context.getType().name()));
    }
  }

  private void handleSourceClusterEvent(ClusterEvent event) {
    logger.info("Processing event from source cluster " + event.getClusterName());
    switch (event.getEventType()) {
    case LiveInstanceChange:
    case InstanceConfigChange:
    case ExternalViewChange:
      _refreshViewCluster = true;
      break;
    case ViewClusterPeriodicRefresh:
      if (!_refreshViewCluster) {
        logger.info("Skip refresh: No event happened since last refresh, and no force refresh.");
        return;
      }
      // mark source cluster as changed to trigger next refresh as we failed to refresh at
      // least some of the elements in view cluster
      logger.info("Refreshing cluster based on event " + event.getEventType().name());
      _refreshViewCluster = refreshViewCluster();
      break;
    default:
      logger.error(String.format("Unrecognized event type: %s", event.getEventType()));
    }
  }

  private synchronized void handleViewClusterConfigChange(ClusterEvent event) {
    logger.info("Processing view cluster event " + event.getEventType().name());
    switch (event.getEventType()) {
    case ClusterConfigChange:
      // TODO: when clusterEventProcessor supports delayed scheduling,
      // we should not have this head-of-line blocking but to have ClusterEventProcessor do the work.
      // Currently it's acceptable as we can endure delay in processing view cluster config change
      if (event.getAttribute(ViewAggregatorEventAttributes.EventProcessBackoff.name()) != null) {
        try {
          Thread.sleep(PROCESS_VIEW_CONFIG_CHANGE_BACKOFF_MS);
        } catch (InterruptedException e) {
          logger.warn("Interrupted when backing off during process view config change retry", e);
        }
      }
      // We always compare current cluster config with most up-to-date cluster config
      ClusterConfig newClusterConfig =
          _dataAccessor.getProperty(_dataAccessor.keyBuilder().clusterConfig());
      SourceClusterConfigChangeAction action =
          new SourceClusterConfigChangeAction(_curViewClusterConfig, newClusterConfig);
      action.computeAction();

      // If we fail to process action and should retry, re-queue event to retry
      if (processViewClusterConfigUpdate(action)) {
        event.addAttribute(ViewAggregatorEventAttributes.EventProcessBackoff.name(), true);
        _viewConfigProcessor.queueEvent(event);
      } else {
        _curViewClusterConfig = newClusterConfig;
      }
      break;
    default:
      logger.error(String.format("Unrecognized event type: %s", event.getEventType()));
    }
  }

  private class RefreshViewClusterTask extends TimerTask {
    @Override
    public void run() {
      logger.info("Triggering view cluster refresh");
      _aggregator.queueEvent(
          new ClusterEvent(_viewClusterName, ClusterEventType.ViewClusterPeriodicRefresh));
    }
  }

  /**
   * Recreate timer that triggers RefreshViewClusterTask
   */
  private void resetTimer(long triggerIntervalMs) {
    if (_viewClusterRefreshTimer != null) {
      _viewClusterRefreshTimer.cancel();
    }
    RefreshViewClusterTask refreshTrigger = new RefreshViewClusterTask();
    _viewClusterRefreshTimer = new Timer(true);
    _viewClusterRefreshTimer.scheduleAtFixedRate(refreshTrigger, 0, triggerIntervalMs);
  }

  /**
   * Use SourceClusterConfigChangeAction to reset timer (RefreshViewClusterTask),
   * create/delete SourceClusterDataProvider in data provider map
   *
   * @return true if action failed and should retry, else false
   */
  private boolean processViewClusterConfigUpdate(SourceClusterConfigChangeAction action) {
    boolean shouldRetry = false;
    for (ViewClusterSourceConfig source : action.getConfigsToDelete()) {
      String key = generateDataProviderMapKey(source);
      logger.info("Deleting data provider " + key);
      if (_dataProviderMap.containsKey(key)) {
        try {
          _dataProviderMap.get(key).shutdown();
          _dataProviderMap.remove(key);
        } catch (Exception e) {
          shouldRetry = true;
          logger.warn(String.format("Failed to shutdown data provider %s, will retry", key));
        }
      }
    }

    for (ViewClusterSourceConfig source : action.getConfigsToAdd()) {
      String key = generateDataProviderMapKey(source);
      logger.info("Creating data provider " + key);
      if (_dataProviderMap.containsKey(key)) {
        // possibly due to a previous failure of shutting down, print warning and recreate for now
        logger.warn(String.format("Add data provider %s which already exists. Recreating", key));
        _dataProviderMap.remove(key);
      }

      try {
        SourceClusterDataProvider provider = new SourceClusterDataProvider(source, _aggregator);
        provider.setup();
        _dataProviderMap.put(key, provider);
      } catch (Exception e) {
        shouldRetry = true;
        logger.warn(String.format("Failed to create data provider %s, will retry", key));
      }
    }

    if (action.shouldResetTimer()) {
      logger.info(
          "Resetting view cluster refresh timer at interval " + action.getCurrentRefreshPeriodMs());
      resetTimer(action.getCurrentRefreshPeriodMs());
    }
    return shouldRetry;
  }

  /**
   * Use ViewClusterRefresher to refresh ViewCluster.
   * @return true if needs retry, else false
   */
  private synchronized boolean refreshViewCluster() {
    // TODO: Implement refresh logic
    return false;
  }

  private static String generateHelixManagerInstanceName(String viewClusterName) {
    return String.format("HelixViewAggregator-%s", viewClusterName);
  }

  private static String generateDataProviderMapKey(ViewClusterSourceConfig config) {
    return String.format("%s-%s", config.getName(), config.getZkAddress());
  }
}
