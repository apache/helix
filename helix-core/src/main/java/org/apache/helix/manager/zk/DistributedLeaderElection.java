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

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * do distributed leader election
 */
public class DistributedLeaderElection implements ControllerChangeListener {
  private static Logger LOG = LoggerFactory.getLogger(DistributedLeaderElection.class);

  private final HelixManager _manager;
  private final GenericHelixController _controller;
  private final List<HelixTimerTask> _controllerTimerTasks;

  public DistributedLeaderElection(HelixManager manager, GenericHelixController controller,
      List<HelixTimerTask> controllerTimerTasks) {
    _manager = manager;
    _controller = controller;
    _controllerTimerTasks = controllerTimerTasks;

    InstanceType type = _manager.getInstanceType();
    if (type != InstanceType.CONTROLLER && type != InstanceType.CONTROLLER_PARTICIPANT) {
      throw new HelixException(
          "fail to become controller because incorrect instanceType (was " + type.toString()
              + ", requires CONTROLLER | CONTROLLER_PARTICIPANT)");
    }
  }

  @Override
  public synchronized void onControllerChange(NotificationContext changeContext) {
    ControllerManagerHelper controllerHelper =
        new ControllerManagerHelper(_manager, _controllerTimerTasks);
    try {
      switch (changeContext.getType()) {
      case INIT:
      case CALLBACK:
        acquireLeadership(_manager, controllerHelper);
        break;
      case FINALIZE:
        relinquishLeadership(_manager, controllerHelper);
        break;
      default:
        LOG.info("Ignore controller change event {}. Type {}.", changeContext.getEventName(),
            changeContext.getType().name());
      }
    } catch (Exception e) {
      LOG.error("Exception when trying to become leader", e);
    }
  }

  private void relinquishLeadership(HelixManager manager,
      ControllerManagerHelper controllerHelper) {
    long start = System.currentTimeMillis();
    LOG.info(manager.getInstanceName() + " tries to relinquish leadership for cluster: " + manager
        .getClusterName());
    controllerHelper.stopControllerTimerTasks();
    controllerHelper.removeListenersFromController(_controller);
    // clear write-through cache
    manager.getHelixDataAccessor().getBaseDataAccessor().reset();
    LOG.info("{} relinquishes leadership for cluster: {}, took: {}ms", manager.getInstanceName(),
        manager.getClusterName(), System.currentTimeMillis() - start);
  }

  private void acquireLeadership(final HelixManager manager,
      ControllerManagerHelper controllerHelper) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey leaderNodePropertyKey = accessor.keyBuilder().controllerLeader();

    LOG.info(manager.getInstanceName() + " tries to acquire leadership for cluster: " + manager
        .getClusterName());
    // Try to acquire leader and init the manager in any case.
    // Even when a leader node already exists, the election process shall still try to init the manager
    // in case it is the current leader.
    do {
      // Due to the possible carried over ZK events from the previous ZK session, the following
      // initialization might be triggered multiple times. So the operation must be idempotent.
      long start = System.currentTimeMillis();
      if (tryCreateController(manager)) {
        manager.getHelixDataAccessor().getBaseDataAccessor().reset();
        controllerHelper.addListenersToController(_controller);
        controllerHelper.startControllerTimerTasks();
        LOG.info("{} with session {} acquired leadership for cluster: {}, took: {}ms",
            manager.getInstanceName(), manager.getSessionId(), manager.getClusterName(),
            System.currentTimeMillis() - start);
      }
    } while (accessor.getProperty(leaderNodePropertyKey) == null);
  }

  /**
   * @return true if the current manager created a new controller node successfully.
   */
  private boolean tryCreateController(HelixManager manager) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance newLeader = new LiveInstance(manager.getInstanceName());
    newLeader.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
    newLeader.setSessionId(manager.getSessionId());
    newLeader.setHelixVersion(manager.getVersion());
    try {
      if (accessor.createControllerLeader(newLeader)) {
        updateHistory(manager);
        return true;
      } else {
        LOG.info(
            "Unable to become leader probably because some other controller became the leader.");
      }
    } catch (Exception e) {
      LOG.error(
          "Exception when trying to updating leader record in cluster:" + manager.getClusterName()
              + ". Need to check again whether leader node has been created or not.", e);
    }

    LiveInstance currentLeader = accessor.getProperty(keyBuilder.controllerLeader());
    if (currentLeader != null) {
      String currentSession = currentLeader.getEphemeralOwner();
      LOG.info("Leader exists for cluster: " + manager.getClusterName() + ", currentLeader: "
          + currentLeader.getInstanceName() + ", leaderSessionId: " + currentSession);
      if (currentSession != null && currentSession.equals(newLeader.getEphemeralOwner())) {
        return true;
      } else {
        LOG.warn("The existing leader has a different session. Expected session Id: " + newLeader
            .getEphemeralOwner());
      }
    }
    return false;
  }

  private void updateHistory(HelixManager manager) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    final String clusterName = manager.getClusterName();
    final String instanceName = manager.getInstanceName();
    final String version = manager.getVersion();

    // Record a MaintenanceSignal history
    if (!accessor.getBaseDataAccessor().update(keyBuilder.controllerLeaderHistory().getPath(),
        oldRecord -> {
          if (oldRecord == null) {
            oldRecord = new ZNRecord(PropertyType.HISTORY.toString());
          }
          return new ControllerHistory(oldRecord).updateHistory(clusterName, instanceName,
              version);
        }, AccessOption.PERSISTENT)) {
      LOG.error("Failed to persist leader history to ZK!");
    }
  }
}
