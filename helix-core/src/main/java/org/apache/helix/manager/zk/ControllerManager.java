package org.apache.helix.manager.zk;

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
import java.util.Arrays;
import java.util.List;
import java.util.Timer;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.healthcheck.HealthStatsAggregationTask;
import org.apache.helix.healthcheck.HealthStatsAggregator;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.ZKPathDataDumpTask;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ControllerManager extends AbstractManager {
  private static Logger LOG = Logger.getLogger(ControllerManager.class);

  final GenericHelixController _controller = new GenericHelixController();

  // TODO merge into GenericHelixController
  private CallbackHandler _leaderElectionHandler = null;

  /**
   * status dump timer-task
   *
   */
  static class StatusDumpTask extends HelixTimerTask {
    Timer _timer = null;
    final ZkClient zkclient;
    final AbstractManager helixController;

    public StatusDumpTask(ZkClient zkclient, AbstractManager helixController) {
      this.zkclient = zkclient;
      this.helixController = helixController;
    }

    @Override
    public void start() {
      long initialDelay = 30 * 60 * 1000;
      long period = 120 * 60 * 1000;
      int timeThresholdNoChange = 180 * 60 * 1000;

      if (_timer == null)
      {
        LOG.info("Start StatusDumpTask");
        _timer = new Timer("StatusDumpTimerTask", true);
        _timer.scheduleAtFixedRate(new ZKPathDataDumpTask(helixController,
                                                          zkclient,
                                                          timeThresholdNoChange),
                                   initialDelay,
                                   period);
      }

    }

    @Override
    public void stop() {
      if (_timer != null)
      {
        LOG.info("Stop StatusDumpTask");
        _timer.cancel();
        _timer = null;
      }
    }
  }

  public ControllerManager(String zkAddress, String clusterName, String instanceName) {
    super(zkAddress, clusterName, instanceName, InstanceType.CONTROLLER);

    _timerTasks.add(new HealthStatsAggregationTask(new HealthStatsAggregator(this)));
    _timerTasks.add(new StatusDumpTask(_zkclient, this));
  }

  @Override
  protected List<HelixTimerTask> getControllerHelixTimerTasks() {
    return _timerTasks;
  }

  @Override
  public void handleNewSession() throws Exception {
    waitUntilConnected();

    /**
     * reset all handlers, make sure cleanup completed for previous session
     * disconnect if fail to cleanup
     */
    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.reset();
    }
    // TODO reset user defined handlers only
    resetHandlers();

    /**
     * from here on, we are dealing with new session
     */

    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.init();
    } else {
      _leaderElectionHandler = new CallbackHandler(this,
                                                   _zkclient,
                                                   _keyBuilder.controller(),
                                  new DistributedLeaderElection(this, _controller),
                                  new EventType[] { EventType.NodeChildrenChanged,
                                      EventType.NodeDeleted, EventType.NodeCreated },
                                  ChangeType.CONTROLLER);
    }

    /**
     * init handlers
     * ok to init message handler and controller handlers twice
     * the second init will be skipped (see CallbackHandler)
     */
    initHandlers(_handlers);
  }

  @Override
  void doDisconnect() {
    if (_leaderElectionHandler != null)
    {
      _leaderElectionHandler.reset();
    }
  }

  @Override
  public boolean isLeader() {
    if (!isConnected())
    {
      return false;
    }

    try {
      LiveInstance leader = _dataAccessor.getProperty(_keyBuilder.controllerLeader());
      if (leader != null)
      {
        String leaderName = leader.getInstanceName();
        String sessionId = leader.getSessionId();
        if (leaderName != null && leaderName.equals(_instanceName)
            && sessionId != null && sessionId.equals(_sessionId))
        {
          return true;
        }
      }
    } catch (Exception e) {
      // log
    }
    return false;
  }

  /**
   * helix-controller uses a write-through cache for external-view
   *
   */
  @Override
  BaseDataAccessor<ZNRecord> createBaseDataAccessor(ZkBaseDataAccessor<ZNRecord> baseDataAccessor) {
    String extViewPath = PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, _clusterName);
    return new ZkCacheBaseDataAccessor<ZNRecord>(baseDataAccessor,
                                                Arrays.asList(extViewPath));

  }

}
