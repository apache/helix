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

import java.lang.management.ManagementFactory;

import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.model.LeaderHistory;
import org.apache.helix.model.LiveInstance;
import org.apache.log4j.Logger;

// TODO GenericHelixController has a controller-listener, we can invoke leader-election from there
public class ZkHelixLeaderElection implements ControllerChangeListener {
  private static Logger LOG = Logger.getLogger(ZkHelixLeaderElection.class);

  final ZkHelixController _controller;
  final ClusterId _clusterId;
  final ControllerId _controllerId;
  final HelixManager _manager;
  final GenericHelixController _pipeline;

  public ZkHelixLeaderElection(ZkHelixController controller, GenericHelixController pipeline) {
    _controller = controller;
    _clusterId = controller.getClusterId();
    _controllerId = controller.getControllerId();
    _pipeline = pipeline;
    _manager = controller.getManager();
  }

  /**
   * may be accessed by multiple threads: zk-client thread and
   * ZkHelixManager.disconnect()->reset() TODO: Refactor accessing
   * HelixMangerMain class statically
   */
  @Override
  public synchronized void onControllerChange(NotificationContext changeContext) {
    HelixManager manager = changeContext.getManager();
    if (manager == null) {
      LOG.error("missing attributes in changeContext. requires HelixManager");
      return;
    }

    InstanceType type = _manager.getInstanceType();
    if (type != InstanceType.CONTROLLER && type != InstanceType.CONTROLLER_PARTICIPANT) {
      LOG.error("fail to become controller because incorrect instanceType (was " + type.toString()
          + ", requires CONTROLLER | CONTROLLER_PARTICIPANT)");
      return;
    }

    try {
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();

      if (changeContext.getType().equals(NotificationContext.Type.INIT)
          || changeContext.getType().equals(NotificationContext.Type.CALLBACK)) {
        LOG.info(_controllerId + " is trying to acquire leadership for cluster: " + _clusterId);

        while (accessor.getProperty(keyBuilder.controllerLeader()) == null) {
          boolean success = tryUpdateController(_manager);
          if (success) {
            LOG.info(_controllerId + " acquires leadership of cluster: " + _clusterId);

            updateHistory(_manager);
            _manager.getHelixDataAccessor().getBaseDataAccessor().reset();
            _controller.addListenersToController(_pipeline);
            _controller.startTimerTasks();
          }
        }
      } else if (changeContext.getType().equals(NotificationContext.Type.FINALIZE)) {
        LOG.info(_controllerId + " reqlinquishes leadership of cluster: " + _clusterId);
        _controller.stopTimerTasks();
        _controller.removeListenersFromController(_pipeline);
        _pipeline.shutdownClusterStatusMonitor(_manager.getClusterName());

        /**
         * clear write-through cache
         */
        accessor.getBaseDataAccessor().reset();

        /**
         * remove leader ephemeral znode if this is the leader
         * note that session expiry may happen during checking leader and remove leader
         * in this race condition, we may remove a leader node created by another controller
         * this is fine since it will just invoke another round of leader-election
         */
        if (_controller.isLeader()) {
          accessor.removeProperty(keyBuilder.controllerLeader());
        }
      }

    } catch (Exception e) {
      LOG.error("Exception when trying to become leader", e);
    }
  }

  /**
   * Try to become the leader controller
   * @param manager a live helix manager connection
   * @return true if this controller has been elected the leader, false otherwise
   */
  public static boolean tryUpdateController(HelixManager manager) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = new LiveInstance(manager.getInstanceName());
    try {
      leader.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
      leader.setSessionId(manager.getSessionId());
      leader.setHelixVersion(manager.getVersion());
      boolean success = accessor.createProperty(keyBuilder.controllerLeader(), leader);
      if (success) {
        return true;
      } else {
        LOG.info("Unable to become leader probably because some other controller becames the leader");
      }
    } catch (Exception e) {
      LOG.error(
          "Exception when trying to updating leader record in cluster:" + manager.getClusterName()
              + ". Need to check again whether leader node has been created or not", e);
    }

    leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader != null) {
      String leaderSessionId = leader.getSessionId();
      String leaderId = leader.getId();
      LOG.info("Leader exists for cluster: " + manager.getClusterName() + ", currentLeader: "
          + leader.getInstanceName() + ", leaderSessionId: " + leaderSessionId);
      if (leaderId != null && leaderId.equals(manager.getInstanceName()) && leaderSessionId != null
          && leaderSessionId.equals(manager.getSessionId())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Update the history with this controller as the most recent leader
   * @param manager active helix manager connection
   */
  public static void updateHistory(HelixManager manager) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    LeaderHistory history = accessor.getProperty(keyBuilder.controllerLeaderHistory());
    if (history == null) {
      history = new LeaderHistory(PropertyType.HISTORY.toString());
    }
    history.updateHistory(manager.getClusterName(), manager.getInstanceName());
    accessor.setProperty(keyBuilder.controllerLeaderHistory(), history);
  }
}
