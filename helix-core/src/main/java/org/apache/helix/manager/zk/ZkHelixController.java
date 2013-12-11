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

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.Id;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.healthcheck.HealthStatsAggregationTask;
import org.apache.helix.healthcheck.HealthStatsAggregator;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.StatusDumpTask;
import org.apache.log4j.Logger;

public class ZkHelixController implements HelixController {
  private static Logger LOG = Logger.getLogger(ZkHelixController.class);

  final ZkHelixConnection _connection;
  final ClusterId _clusterId;
  final ControllerId _controllerId;
  final GenericHelixController _pipeline;
  final DefaultMessagingService _messagingService;
  final List<HelixTimerTask> _timerTasks;
  final ClusterAccessor _clusterAccessor;
  final HelixDataAccessor _accessor;
  final HelixManager _manager;
  final ZkHelixLeaderElection _leaderElection;

  public ZkHelixController(ZkHelixConnection connection, ClusterId clusterId,
      ControllerId controllerId) {
    _connection = connection;
    _clusterId = clusterId;
    _controllerId = controllerId;
    _pipeline = new GenericHelixController();
    _clusterAccessor = connection.createClusterAccessor(clusterId);
    _accessor = connection.createDataAccessor(clusterId);

    _messagingService = (DefaultMessagingService) connection.createMessagingService(this);
    _timerTasks = new ArrayList<HelixTimerTask>();

    _manager = new HelixConnectionAdaptor(this);
    _leaderElection = new ZkHelixLeaderElection(this, _pipeline);

    _timerTasks.add(new HealthStatsAggregationTask(new HealthStatsAggregator(_manager)));
    _timerTasks.add(new StatusDumpTask(clusterId, _manager.getHelixDataAccessor()));
  }

  void startTimerTasks() {
    for (HelixTimerTask task : _timerTasks) {
      task.start();
    }
  }

  void stopTimerTasks() {
    for (HelixTimerTask task : _timerTasks) {
      task.stop();
    }
  }

  @Override
  public HelixConnection getConnection() {
    return _connection;
  }

  @Override
  public void startAsync() {
    _connection.addConnectionStateListener(this);
    onConnected();
  }

  @Override
  public void stopAsync() {
    _connection.removeConnectionStateListener(this);
    onDisconnecting();
  }

  void reset() {
    /**
     * reset all handlers, make sure cleanup completed for previous session
     * disconnect if fail to cleanup
     */
    _connection.resetHandlers(this);

  }

  void init() {
    /**
     * from here on, we are dealing with new session
     * init handlers
     */
    if (!_clusterAccessor.isClusterStructureValid()) {
      throw new HelixException("Cluster structure is not set up for cluster: " + _clusterId);
    }

    /**
     * leader-election listener should be reset/init before all other controller listeners;
     * it's ok to add a listener multiple times, since we check existence in
     * ZkHelixConnection#addXXXListner()
     */
    _connection.addControllerListener(this, _leaderElection, _clusterId);

    /**
     * ok to init message handler and controller handlers twice
     * the second init will be skipped (see CallbackHandler)
     */
    _connection.initHandlers(this);
  }

  @Override
  public void onConnected() {
    reset();
    init();
  }

  @Override
  public void onDisconnecting() {
    LOG.info("disconnecting " + _controllerId + "(" + getType() + ") from " + _clusterId);

    reset();
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    return _messagingService;
  }

  @Override
  public ClusterId getClusterId() {
    return _clusterId;
  }

  @Override
  public ControllerId getControllerId() {
    return _controllerId;
  }

  @Override
  public Id getId() {
    return getControllerId();
  }

  @Override
  public InstanceType getType() {
    return InstanceType.CONTROLLER;
  }

  @Override
  public boolean isLeader() {
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
    try {
      LiveInstance leader = _accessor.getProperty(keyBuilder.controllerLeader());
      if (leader != null) {
        String leaderName = leader.getInstanceName();
        String sessionId = leader.getSessionId();
        if (leaderName != null && leaderName.equals(_controllerId.stringify()) && sessionId != null
            && sessionId.equals(_connection.getSessionId().stringify())) {
          return true;
        }
      }
    } catch (Exception e) {
      // log
    }
    return false;
  }

  void addListenersToController(GenericHelixController pipeline) {
    try {
      /**
       * setup controller message listener and register message handlers
       */
      _connection.addControllerMessageListener(this, _messagingService.getExecutor(), _clusterId);
      MessageHandlerFactory defaultControllerMsgHandlerFactory =
          new DefaultControllerMessageHandlerFactory();
      _messagingService.getExecutor().registerMessageHandlerFactory(
          defaultControllerMsgHandlerFactory.getMessageType(), defaultControllerMsgHandlerFactory);
      MessageHandlerFactory defaultSchedulerMsgHandlerFactory =
          new DefaultSchedulerMessageHandlerFactory(_manager);
      _messagingService.getExecutor().registerMessageHandlerFactory(
          defaultSchedulerMsgHandlerFactory.getMessageType(), defaultSchedulerMsgHandlerFactory);
      MessageHandlerFactory defaultParticipantErrorMessageHandlerFactory =
          new DefaultParticipantErrorMessageHandlerFactory(_manager);
      _messagingService.getExecutor().registerMessageHandlerFactory(
          defaultParticipantErrorMessageHandlerFactory.getMessageType(),
          defaultParticipantErrorMessageHandlerFactory);

      /**
       * setup generic-controller
       */
      _connection.addConfigChangeListener(this, pipeline, _clusterId);
      _connection.addLiveInstanceChangeListener(this, pipeline, _clusterId);
      _connection.addIdealStateChangeListener(this, pipeline, _clusterId);
      _connection.addControllerListener(this, pipeline, _clusterId);
    } catch (ZkInterruptedException e) {
      LOG.warn("zk connection is interrupted during addListenersToController()" + e);
    } catch (Exception e) {
      LOG.error("Error addListenersToController", e);
    }
  }

  void removeListenersFromController(GenericHelixController pipeline) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(_manager.getClusterName());
    /**
     * reset generic-controller
     */
    _connection.removeListener(this, pipeline, keyBuilder.instanceConfigs());
    _connection.removeListener(this, pipeline, keyBuilder.liveInstances());
    _connection.removeListener(this, pipeline, keyBuilder.idealStates());
    _connection.removeListener(this, pipeline, keyBuilder.controller());

    /**
     * reset controller message listener and unregister all message handlers
     */
    _connection.removeListener(this, _messagingService.getExecutor(),
        keyBuilder.controllerMessages());
  }

  HelixManager getManager() {
    return _manager;
  }

}
