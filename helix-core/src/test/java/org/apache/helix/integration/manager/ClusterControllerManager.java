package org.apache.helix.integration.manager;

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

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;

public class ClusterControllerManager extends ZKHelixManager implements Runnable, ZkTestManager {
  private static Logger LOG = Logger.getLogger(ClusterControllerManager.class);

  private final CountDownLatch _startCountDown = new CountDownLatch(1);
  private final CountDownLatch _stopCountDown = new CountDownLatch(1);
  private final CountDownLatch _waitStopFinishCountDown = new CountDownLatch(1);

  private boolean _started = false;

  public ClusterControllerManager(String zkAddr, String clusterName) {
    this(zkAddr, clusterName, "controller");
  }

  public ClusterControllerManager(String zkAddr, String clusterName, String controllerName) {
    super(clusterName, controllerName, InstanceType.CONTROLLER, zkAddr);
  }

  public void syncStop() {
    _stopCountDown.countDown();
    try {
      _waitStopFinishCountDown.await();
      _started = false;
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for finish", e);
    }
  }

  // This should not be called more than once because HelixManager.connect() should not be called more than once.
  public void syncStart() {
    if (_started) {
      throw new RuntimeException("Helix Controller already started. Do not call syncStart() more than once.");
    } else {
      _started = true;
    }

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

  @Override
  public ZkClient getZkClient() {
    return _zkclient;
  }

  @Override
  public List<CallbackHandler> getHandlers() {
    return _handlers;
  }

  public List<HelixTimerTask> getControllerTimerTasks() {
    return _controllerTimerTasks;
  }
}
