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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.common.ClusterEventProcessor;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.view.common.ViewAggregatorEventAttributes;
import org.apache.helix.view.dataprovider.SourceClusterDataProvider;
import org.apache.helix.view.dataprovider.ViewClusterConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main logic for Helix view aggregator
 */
public class HelixViewAggregator {
  private static final Logger logger = LoggerFactory.getLogger(HelixViewAggregator.class);
  private final String _viewClusterName;
  private final HelixManager _viewClusterManager;
  private ViewAggregationWorker _aggregationWorker;
  private ViewConfigWorker _viewConfigWorker;
  private long _lastViewClusterRefreshTimestampMs;
  private Map<String, SourceClusterDataProvider> _dataProviderMap;
  private ViewClusterConfigProvider _viewClusterConfigProvider;
  private List<ViewClusterSourceConfig> _sourceConfigs;
  private long _refreshPeriodMs;
  private Timer _viewClusterRefreshTimer;
  private ViewClusterRefresher _viewClusterRefresher;

  public HelixViewAggregator(String viewClusterName, String zkAddr) {
    _viewClusterName = viewClusterName;
    _lastViewClusterRefreshTimestampMs = 0L;
    _refreshPeriodMs = -1L;
    _sourceConfigs = new ArrayList<>();
    _dataProviderMap = new HashMap<>();
    _viewClusterManager = HelixManagerFactory
        .getZKHelixManager(_viewClusterName, generateHelixManagerInstanceName(_viewClusterName),
            InstanceType.SPECTATOR, zkAddr);

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        shutdown();
      }
    }));
  }

  /**
   * Start controller main logic
   * @throws Exception
   */
  public void start() throws Exception {
    try {
      _viewClusterManager.connect();
    } catch (Exception e) {
      throw new HelixException("Failed to connect view cluster helix manager", e);
    }

    // set up view cluster refresher
    _viewClusterRefresher =
        new ViewClusterRefresher(_viewClusterName, _viewClusterManager.getHelixDataAccessor(),
            _dataProviderMap);

    // Start workers
    _aggregationWorker = new ViewAggregationWorker();
    _aggregationWorker.start();
    _viewConfigWorker = new ViewConfigWorker();
    _viewConfigWorker.start();

    // Start cluster config provider
    _viewClusterConfigProvider =
        new ViewClusterConfigProvider(_viewClusterName, _viewClusterManager, _viewConfigWorker);
    _viewClusterConfigProvider.setup();
  }

  /**
   * Process view cluster config change events
   */
  private class ViewConfigWorker extends ClusterEventProcessor {
    public ViewConfigWorker() {
      super(_viewClusterName, "ViewConfigWorker");
    }
    @Override
    public void handleEvent(ClusterEvent event) {
      logger.info("Processing event " + event.getEventType().name());
      switch (event.getEventType()) {
      case ClusterConfigChange:
        processViewClusterConfigUpdate();
        break;
      default:
        logger.error(String.format("Unrecognized event type: %s", event.getEventType()));
      }
    }
  }

  /**
   * Process source cluster data change events and view cluster periodic refresh events
   */
  private class ViewAggregationWorker extends ClusterEventProcessor {
    private boolean _shouldRefresh;

    public ViewAggregationWorker() {
      super(_viewClusterName, "ViewAggregationWorker");
      _shouldRefresh = false;
    }

    @Override
    public void handleEvent(ClusterEvent event) {
      logger.info("Processing event " + event.getEventType().name());

      switch (event.getEventType()) {
      case LiveInstanceChange:
      case InstanceConfigChange:
      case ExternalViewChange:
        _shouldRefresh = true;
        break;
      case ViewClusterPeriodicRefresh:
        Boolean forceRefresh =
            event.getAttribute(ViewAggregatorEventAttributes.ViewClusterForceRefresh.name());
        if (!forceRefresh && !_shouldRefresh) {
          logger.info("Skip refresh: No event happened since last refresh, and no force refresh.");
          return;
        }

        // mark source cluster as changed to trigger next refresh as we failed to refresh at
        // least some of the elements in view cluster
        logger.info("Refreshing cluster based on event " + event.getEventType().name());
        _shouldRefresh = refreshViewCluster();
        break;
      default:
        logger.error(String.format("Unrecognized event type: %s", event.getEventType()));
      }
    }
  }

  private class RefreshViewClusterTask extends TimerTask {
    @Override
    public void run() {
      triggerViewClusterRefresh(false);
    }
  }

  public void shutdown() {
    if (_viewClusterManager != null) {
      logger.info("Shutting down view cluster helix manager");
      _viewClusterManager.disconnect();
    }

    if (_viewClusterRefreshTimer != null) {
      logger.info("Shutting down view cluster refresh timer");
      _viewClusterRefreshTimer.cancel();
    }

    for (SourceClusterDataProvider provider : _dataProviderMap.values()) {
      logger
          .info(String.format("Shutting data provider for source cluster %s", provider.getName()));
      provider.shutdown();
    }
    logger.info("HelixViewAggregator shutdown cleanly");
  }

  /**
   * Recreate timer that triggers RefreshViewClusterTask
   */
  private void resetTimer() {
    // TODO: implement
  }

  /**
   * Use ViewClusterConfigProvider (assume its up-to-date) to compute
   * SourceClusterConfigChangeAction, based on _sourceConfigs. Use the action object to
   * reset timer (RefreshViewClusterTask), create/delete/update SourceClusterDataProvider in
   * data provider map and populate new _sourceConfigs
   */
  private synchronized void processViewClusterConfigUpdate() {
    // TODO: implement
  }

  /**
   * push event to worker queue to trigger refresh. Worker might not refresh view cluster
   * if there is no event happened since last refresh
   * @param forceRefresh
   */
  private void triggerViewClusterRefresh(boolean forceRefresh) {
    ClusterEvent event = new ClusterEvent(_viewClusterName, ClusterEventType.ViewClusterPeriodicRefresh);
    event.addAttribute(ViewAggregatorEventAttributes.ViewClusterForceRefresh.name(),
        Boolean.valueOf(forceRefresh));
    _aggregationWorker.queueEvent(event);
    logger.info("Triggering view cluster refresh, forceRefresh=" + forceRefresh);
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

  private static String generateSourceClusterDataProviderMapKey(ViewClusterSourceConfig config) {
    return String.format("%s-%s", config.getName(), config.getZkAddress());
  }
}
