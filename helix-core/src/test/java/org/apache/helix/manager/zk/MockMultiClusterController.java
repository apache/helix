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
import java.util.concurrent.CountDownLatch;

import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.participant.MultiClusterControllerTransitionHandlerFactory;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;

public class MockMultiClusterController extends ZKHelixManager implements Runnable {
  private static Logger LOG = Logger.getLogger(MockMultiClusterController.class);

  private final CountDownLatch _startCountDown = new CountDownLatch(1);
  private final CountDownLatch _stopCountDown = new CountDownLatch(1);
  private final CountDownLatch _waitStopFinishCountDown = new CountDownLatch(1);

  public MockMultiClusterController(String zkAddr, String clusterName, String controllerName) {
    super(clusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT, zkAddr);
  }

  public void syncStop() {
    _stopCountDown.countDown();
    try {
      _waitStopFinishCountDown.await();
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for finish", e);
    }
  }

  public void syncStart() {
    // TODO: prevent start multiple times
    new Thread(this).start();
    try {
      _startCountDown.await();
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for start", e);
    }
  }

  @Override
  public void run() {
    try {
      StateMachineEngine stateMach = getStateMachineEngine();
      MultiClusterControllerTransitionHandlerFactory lsModelFactory =
          new MultiClusterControllerTransitionHandlerFactory(_zkAddress);
      stateMach.registerStateModelFactory(StateModelDefId.LeaderStandby, lsModelFactory);

      connect();
      _startCountDown.countDown();
      _stopCountDown.await();
    } catch (Exception e) {
      LOG.error("exception running controller-manager", e);
    } finally {
      _startCountDown.countDown();
      disconnect();
      _waitStopFinishCountDown.countDown();
    }
  }

  public ZkHelixConnection getConn() {
    return (ZkHelixConnection)_role.getConnection();
  }

  public ZkClient getZkClient() {
    ZkHelixConnection conn = (ZkHelixConnection)getConn();
    return conn._zkclient;
  }

  public List<ZkCallbackHandler> getHandlers() {
    ZkHelixConnection conn = (ZkHelixConnection)getConn();
    List<ZkCallbackHandler> handlers = new ArrayList<ZkCallbackHandler>();
    for (List<ZkCallbackHandler> handlerList : conn._handlers.values()) {
      handlers.addAll(handlerList);
    }

    return handlers;
  }

  public ZkHelixMultiClusterController getRole() {
    return (ZkHelixMultiClusterController) _role;
  }

  public ZkHelixController getController() {
    return getRole()._controller;
  }
}
