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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.view.common.ClusterViewEvent;
import org.apache.helix.view.dataprovider.SourceClusterDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main logic for Helix view aggregator
 */
public class HelixViewAggregator implements ClusterConfigChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(HelixViewAggregator.class);
  private static final long DEFAULT_INITIAL_EVENT_PROCESS_BACKOFF = 10;
  private static final long DEFAULT_MAX_EVENT_PROCESS_BACKOFF = 5 * 1000;
  private final String _viewClusterName;
  private final HelixManager _viewClusterManager;
  private final Map<String, SourceClusterDataProvider> _dataProviderMap;
  private HelixDataAccessor _dataAccessor;

  // Worker that processes source cluster events and refresh view cluster
  private DedupEventProcessor<ClusterViewEvent.Type, ClusterViewEvent> _aggregator;
  private AtomicBoolean _refreshViewCluster;

  // Worker that processes view cluster config change
  private DedupEventProcessor<ClusterViewEvent.Type, ClusterViewEvent> _viewConfigProcessor;

  private ClusterConfig _curViewClusterConfig;
  private Timer _viewClusterRefreshTimer;
  private ViewClusterRefresher _viewClusterRefresher;

  public HelixViewAggregator(String viewClusterName, String zkAddr) {
    _viewClusterName = viewClusterName;
    _dataProviderMap = new ConcurrentHashMap<>();
    _viewClusterManager = HelixManagerFactory
        .getZKHelixManager(_viewClusterName, generateHelixManagerInstanceName(_viewClusterName),
            InstanceType.SPECTATOR, zkAddr);
    _refreshViewCluster = new AtomicBoolean(false);
    _aggregator = new DedupEventProcessor<ClusterViewEvent.Type, ClusterViewEvent>(_viewClusterName,
        "Aggregator") {
      @Override
      public void handleEvent(ClusterViewEvent event) {
        handleSourceClusterEvent(event);
      }
    };

    _viewConfigProcessor = new DedupEventProcessor<ClusterViewEvent.Type, ClusterViewEvent>(_viewClusterName, "ViewConfigProcessor") {
      @Override
      public void handleEvent(ClusterViewEvent event) {
        handleViewClusterConfigChange(event);
      }
    };
  }

  public String getAggregatorInstanceName() {
    return String
        .format("%s::%s", _viewClusterManager.getInstanceName(), hashCode());
  }

  /**
   * Start controller main logic
   * @throws Exception when HelixViewAggregator fails to start. Will try to shut it down before
   *                   exception is thrown out
   */
  public void start() throws Exception {
    // Start workers
    _aggregator.start();
    _viewConfigProcessor.start();

    // Setup manager
    try {
      _viewClusterManager.connect();
      _dataAccessor = _viewClusterManager.getHelixDataAccessor();
      _viewClusterManager.addClusterfigChangeListener(this);
    } catch (Exception e) {
      shutdown();
      throw new HelixException("Failed to connect view cluster helix manager", e);
    }

    // Set up view cluster refresher
    _viewClusterRefresher =
        new ViewClusterRefresher(_viewClusterName, _viewClusterManager.getHelixDataAccessor());
  }

  public void shutdown() {
    boolean success = true;

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
        success = false;
        logger.error(String
            .format("Failed to disconnect helix manager for view cluster %s", _viewClusterName), e);
      }
    }

    // Clean up all data providers
    for (SourceClusterDataProvider provider : _dataProviderMap.values()) {
      logger
          .info(String.format("Shutting down data provider for source cluster %s", provider.getName()));
      try {
        provider.shutdown();
      } catch (Exception e) {
        success = false;
        logger.error(String
            .format("Failed to shutdown data provider %s for view cluster %s", provider.getName(),
                _viewClusterName), e);
      }
    }

    logger.info("HelixViewAggregator shutdown " + (success ? "cleanly" : "with error"));
  }

  @Override
  @PreFetch(enabled = false)
  public void onClusterConfigChange(ClusterConfig clusterConfig, NotificationContext context) {
    if (context != null && context.getType() != NotificationContext.Type.FINALIZE) {
      _viewConfigProcessor.queueEvent(ClusterViewEvent.Type.ConfigChange,
          new ClusterViewEvent(_viewClusterName, ClusterViewEvent.Type.ConfigChange));
    } else {
      logger.info(String
          .format("Skip processing view cluster config change with notification context type %s",
              context == null ? "NoContext" : context.getType().name()));
    }
  }

  private void handleSourceClusterEvent(ClusterViewEvent event) {
    logger.info(String
        .format("Processing event %s from source cluster %s.", event.getEventType().name(),
            event.getClusterName()));
    switch (event.getEventType()) {
    case ExternalViewChange:
    case InstanceConfigChange:
    case LiveInstanceChange:
      _refreshViewCluster.set(true);
      break;
    case PeriodicViewRefresh:
      if (!_refreshViewCluster.get()) {
        logger.info("Skip refresh: No event happened since last refresh, and no force refresh.");
        return;
      }
      // mark source cluster as changed to trigger next refresh as we failed to refresh at
      // least some of the elements in view cluster
      logger.info("Refreshing cluster based on event " + event.getEventType().name());
      refreshViewCluster();
      break;
    default:
      logger.error(String.format("Unrecognized event type: %s", event.getEventType()));
    }
  }

  private void handleViewClusterConfigChange(ClusterViewEvent event) {
    logger.info(String
        .format("Processing event %s for view cluster %s", event.getEventType().name(),
            _viewClusterName));
    switch (event.getEventType()) {
    case ConfigChange:
      // TODO: when DedupEventProcessor supports delayed scheduling,
      // we should not have this head-of-line blocking but to have DedupEventProcessor do the work.
      // Currently it's acceptable as we can endure delay in processing view cluster config change
      try {
        Thread.sleep(event.getEventProcessBackoff());
      } catch (InterruptedException e) {
        logger.warn("Interrupted when backing off during process view config change retry", e);
        Thread.currentThread().interrupt();
      }

      // We always compare current cluster config with most up-to-date cluster config
      boolean success;
      ClusterConfig newClusterConfig =
          _dataAccessor.getProperty(_dataAccessor.keyBuilder().clusterConfig());

      if (newClusterConfig == null) {
        logger.warn("Failed to read view cluster config");
        success = false;
      } else {
        SourceClusterConfigChangeAction action =
            new SourceClusterConfigChangeAction(_curViewClusterConfig, newClusterConfig);
        action.computeAction();
        success = processViewClusterConfigUpdate(action);
      }

      // If we fail to process action and should retry, re-queue event to retry
      if (!success) {
        long backoff = computeNextEventProcessBackoff(event.getEventProcessBackoff());
        logger.info("Failed to process view cluster config change. Will retry in {} ms", backoff);
        event.setEventProcessBackoff(backoff);
        _viewConfigProcessor.queueEvent(event.getEventType(), event);
      } else {
        _curViewClusterConfig = newClusterConfig;
      }
      break;
    default:
      logger.error(String.format("Unrecognized event type: %s", event.getEventType()));
    }
  }

  private long computeNextEventProcessBackoff(long currentBackoff) {
    if (currentBackoff <= 0) {
      return DEFAULT_INITIAL_EVENT_PROCESS_BACKOFF;
    }

    // Exponential backoff with ceiling
    return currentBackoff * 2 > DEFAULT_MAX_EVENT_PROCESS_BACKOFF
        ? DEFAULT_MAX_EVENT_PROCESS_BACKOFF
        : currentBackoff * 2;
  }

  private class RefreshViewClusterTask extends TimerTask {
    @Override
    public void run() {
      logger.info("Triggering view cluster refresh");
      _aggregator.queueEvent(ClusterViewEvent.Type.PeriodicViewRefresh,
          new ClusterViewEvent(_viewClusterName, ClusterViewEvent.Type.PeriodicViewRefresh));
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
   * @return true if success else false
   */
  private boolean processViewClusterConfigUpdate(SourceClusterConfigChangeAction action) {
    boolean success = true;
    for (ViewClusterSourceConfig source : action.getConfigsToDelete()) {
      String key = generateDataProviderMapKey(source);
      logger.info("Deleting data provider " + key);
      if (_dataProviderMap.containsKey(key)) {
        try {
          _dataProviderMap.get(key).shutdown();
          synchronized (_dataProviderMap) {
            _dataProviderMap.remove(key);
            // upon successful removal of data provider, set refresh view cluster to true
            // or if no event from source cluster happened before next refresh cycle, this
            // removal will be missed.
            _refreshViewCluster.set(true);
          }
        } catch (Exception e) {
          success = false;
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
        success = false;
        logger.warn(String.format("Failed to create data provider %s, will retry", key));
      }
    }


    if (action.shouldResetTimer()) {
      logger.info(
          "Resetting view cluster refresh timer at interval " + action.getCurrentRefreshPeriodMs());
      resetTimer(action.getCurrentRefreshPeriodMs());
    }
    return success;
  }

  /**
   * Use ViewClusterRefresher to refresh ViewCluster.
   */
  private void refreshViewCluster() {
    long startRefreshMs = System.currentTimeMillis();
    logger.info(String.format("START RefreshViewCluster: Refresh view cluster %s at timestamp %s",
        _viewClusterName, startRefreshMs));
    boolean dataProviderFailure = false;

    // Generate a view of providers so refresh won't block cluster config update
    // When a data provider is shutdown while we are reloading cache / generating diff,
    // Exception will be thrown out and we retry during next refresh cycle
    Set<SourceClusterDataProvider> providerView;
    synchronized (_dataProviderMap) {
      _refreshViewCluster.set(false);
      providerView = new HashSet<>(_dataProviderMap.values());
    }

    // Refresh data providers
    // TODO: the following steps can be parallelized
    for (SourceClusterDataProvider provider : providerView) {
      try {
        provider.refreshCache();
      } catch (Exception e) {
        logger.warn("Caught exception when refreshing source cluster cache. Abort refresh.", e);
        _refreshViewCluster.set(true);
        dataProviderFailure = true;

        // Skip refresh view cluster when we cannot successfully refresh
        // source cluster caches
        break;
      }
    }

    // Refresh properties in view cluster
    if (!dataProviderFailure) {
      _viewClusterRefresher.updateProviderView(providerView);
      for (PropertyType propertyType : ViewClusterSourceConfig.getValidPropertyTypes()) {
        logger.info(String
            .format("Refreshing property %s in view cluster %s", propertyType, _viewClusterName));
        try {
          // We try to refresh all properties with best effort, and don't break when
          // failed to refresh a particular property
          if (!_viewClusterRefresher.refreshPropertiesInViewCluster(propertyType)) {
            _refreshViewCluster.set(true);
          }
        } catch (IllegalArgumentException e) {
          // Invalid property... not expected! Something wrong with code, should not retry
          logger.error(String.format("Failed to refresh property in view cluster %s with exception",
              _viewClusterName), e);
        }
      }
    }
  }

  private static String generateHelixManagerInstanceName(String viewClusterName) {
    return String.format("HelixViewAggregator-%s", viewClusterName);
  }

  private static String generateDataProviderMapKey(ViewClusterSourceConfig config) {
    return String.format("%s-%s", config.getName(), config.getZkAddress());
  }
}
