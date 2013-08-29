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
import java.util.List;

import org.apache.helix.HelixException;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.healthcheck.HealthStatsAggregationTask;
import org.apache.helix.healthcheck.HealthStatsAggregator;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.healthcheck.ParticipantHealthReportCollectorImpl;
import org.apache.helix.healthcheck.ParticipantHealthReportTask;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.participant.HelixStateMachineEngine;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

public class DistributedControllerManager extends AbstractManager {
  private static Logger LOG = Logger.getLogger(DistributedControllerManager.class);

  final StateMachineEngine _stateMachineEngine;
  final ParticipantHealthReportCollectorImpl _participantHealthInfoCollector;

  CallbackHandler _leaderElectionHandler = null;
  final GenericHelixController _controller = new GenericHelixController();

  /**
   * hold timer tasks for controller only
   * we need to add/remove controller timer tasks during handle new session
   */
  final List<HelixTimerTask> _controllerTimerTasks = new ArrayList<HelixTimerTask>();

  public DistributedControllerManager(String zkAddress, String clusterName, String instanceName) {
    super(zkAddress, clusterName, instanceName, InstanceType.CONTROLLER_PARTICIPANT);

    _stateMachineEngine = new HelixStateMachineEngine(this);
    _participantHealthInfoCollector = new ParticipantHealthReportCollectorImpl(this, _instanceName);

    _timerTasks.add(new ParticipantHealthReportTask(_participantHealthInfoCollector));

    _controllerTimerTasks.add(new HealthStatsAggregationTask(new HealthStatsAggregator(this)));
    _controllerTimerTasks.add(new ControllerManager.StatusDumpTask(_zkclient, this));

  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector() {
    checkConnected();
    return _participantHealthInfoCollector;
  }

  @Override
  public StateMachineEngine getStateMachineEngine() {
    return _stateMachineEngine;
  }

  @Override
  protected List<HelixTimerTask> getControllerHelixTimerTasks() {
    return _controllerTimerTasks;
  }

  @Override
  public void handleNewSession() throws Exception {
    waitUntilConnected();

    ParticipantManagerHelper participantHelper =
        new ParticipantManagerHelper(this, _zkclient, _sessionTimeout);

    /**
     * stop all timer tasks, reset all handlers, make sure cleanup completed for previous session
     * disconnect if fail to cleanup
     */
    stopTimerTasks();
    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.reset();
    }
    resetHandlers();

    /**
     * clean up write-through cache
     */
    _baseDataAccessor.reset();

    /**
     * from here on, we are dealing with new session
     */
    if (!ZKUtil.isClusterSetup(_clusterName, _zkclient)) {
      throw new HelixException("Cluster structure is not set up for cluster: " + _clusterName);
    }

    /**
     * auto-join
     */
    participantHelper.joinCluster();

    /**
     * Invoke PreConnectCallbacks
     */
    for (PreConnectCallback callback : _preConnectCallbacks) {
      callback.onPreConnect();
    }

    participantHelper.createLiveInstance();

    participantHelper.carryOverPreviousCurrentState();

    participantHelper.setupMsgHandler();

    /**
     * leader election
     */
    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.init();
    } else {
      _leaderElectionHandler =
          new CallbackHandler(this, _zkclient, _keyBuilder.controller(),
              new DistributedLeaderElection(this, _controller), new EventType[] {
                  EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated
              }, ChangeType.CONTROLLER);
    }

    /**
     * start health-check timer task
     */
    participantHelper.createHealthCheckPath();
    startTimerTasks();

    /**
     * init handlers
     * ok to init message handler, data-accessor, and controller handlers twice
     * the second init will be skipped (see CallbackHandler)
     */
    initHandlers(_handlers);

  }

  @Override
  void doDisconnect() {
    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.reset();
    }
  }

  @Override
  public boolean isLeader() {
    if (!isConnected()) {
      return false;
    }

    try {
      LiveInstance leader = _dataAccessor.getProperty(_keyBuilder.controllerLeader());
      if (leader != null) {
        String leaderName = leader.getInstanceName();
        String sessionId = leader.getSessionIdString();
        if (leaderName != null && leaderName.equals(_instanceName) && sessionId != null
            && sessionId.equals(_sessionId)) {
          return true;
        }
      }
    } catch (Exception e) {
      // log
    }
    return false;
  }

}
