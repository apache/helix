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
import java.util.List;

import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.model.LeaderHistory;
import org.apache.helix.model.LiveInstance;
import org.apache.log4j.Logger;


/**
 * do distributed leader election
 */
public class DistributedLeaderElection implements ControllerChangeListener {
  private static Logger LOG = Logger.getLogger(DistributedLeaderElection.class);

  final HelixManager _manager;
  final GenericHelixController _controller;
  final List<HelixTimerTask> _controllerTimerTasks;

  public DistributedLeaderElection(HelixManager manager, GenericHelixController controller,
      List<HelixTimerTask> controllerTimerTasks) {
    _manager = manager;
    _controller = controller;
    _controllerTimerTasks = controllerTimerTasks;
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

    InstanceType type = manager.getInstanceType();
    if (type != InstanceType.CONTROLLER && type != InstanceType.CONTROLLER_PARTICIPANT) {
      LOG.error("fail to become controller because incorrect instanceType (was " + type.toString()
          + ", requires CONTROLLER | CONTROLLER_PARTICIPANT)");
      return;
    }

    ControllerManagerHelper controllerHelper =
        new ControllerManagerHelper(_manager, _controllerTimerTasks);
    try {
      if (changeContext.getType().equals(NotificationContext.Type.INIT)
          || changeContext.getType().equals(NotificationContext.Type.CALLBACK)) {
        LOG.info(_manager.getInstanceName() + " is trying to acquire leadership for cluster: "
            + _manager.getClusterName());
        HelixDataAccessor accessor = manager.getHelixDataAccessor();
        Builder keyBuilder = accessor.keyBuilder();

        while (accessor.getProperty(keyBuilder.controllerLeader()) == null) {
          boolean success = tryUpdateController(manager);
          if (success) {
            LOG.info(_manager.getInstanceName() + " acquired leadership for cluster: "
                + _manager.getClusterName());

            updateHistory(manager);
            _manager.getHelixDataAccessor().getBaseDataAccessor().reset();
            controllerHelper.addListenersToController(_controller);
            controllerHelper.startControllerTimerTasks();
          }
        }
      } else if (changeContext.getType().equals(NotificationContext.Type.FINALIZE)) {
        LOG.info(_manager.getInstanceName() + " reqlinquish leadership for cluster: "
            + _manager.getClusterName());
        controllerHelper.stopControllerTimerTasks();
        controllerHelper.removeListenersFromController(_controller);
        _controller.shutdownClusterStatusMonitor(_manager.getClusterName());

        /**
         * clear write-through cache
         */
        _manager.getHelixDataAccessor().getBaseDataAccessor().reset();
      }

    } catch (Exception e) {
      LOG.error("Exception when trying to become leader", e);
    }
  }

  private boolean tryUpdateController(HelixManager manager) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = new LiveInstance(manager.getInstanceName());
    try {
      leader.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
      leader.setSessionId(manager.getSessionId());
      leader.setHelixVersion(manager.getVersion());
      boolean success = accessor.createControllerLeader(leader);
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
      LOG.info("Leader exists for cluster: " + manager.getClusterName() + ", currentLeader: "
          + leader.getInstanceName() + ", leaderSessionId: " + leaderSessionId);

      if (leaderSessionId != null && leaderSessionId.equals(manager.getSessionId())) {
        return true;
      }
    }
    return false;
  }

  private void updateHistory(HelixManager manager) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    LeaderHistory history = accessor.getProperty(keyBuilder.controllerLeaderHistory());
    if (history == null) {
      history = new LeaderHistory(PropertyType.HISTORY.toString());
    }
    history.updateHistory(manager.getClusterName(), manager.getInstanceName());
    accessor.setControllerHistory(history);
  }
}
