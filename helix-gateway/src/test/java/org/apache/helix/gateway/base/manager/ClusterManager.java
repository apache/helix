package org.apache.helix.gateway.base.manager;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterManager extends ZKHelixManager implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
  private static final int DISCONNECT_WAIT_TIME_MS = 3000;

  private static AtomicLong UID = new AtomicLong(10000);
  private long _uid;

  private final String _clusterName;
  private final String _instanceName;
  private final InstanceType _type;

  protected CountDownLatch _startCountDown = new CountDownLatch(1);
  protected CountDownLatch _stopCountDown = new CountDownLatch(1);
  protected CountDownLatch _waitStopFinishCountDown = new CountDownLatch(1);

  protected boolean _started = false;

  protected Thread _watcher;

  protected ClusterManager(String zkAddr, String clusterName, String instanceName,
      InstanceType type) {
    super(clusterName, instanceName, type, zkAddr);
    _clusterName = clusterName;
    _instanceName = instanceName;
    _type = type;
    _uid = UID.getAndIncrement();
  }
  protected ClusterManager(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddress, HelixManagerStateListener stateListener,
      HelixManagerProperty helixManagerProperty) {
    super(clusterName, instanceName, instanceType, zkAddress, stateListener, helixManagerProperty);
    _clusterName = clusterName;
    _instanceName = instanceName;
    _type = instanceType;
    _uid = UID.getAndIncrement();
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
      throw new RuntimeException(
          "Helix Controller already started. Do not call syncStart() more than once.");
    } else {
      _started = true;
    }

    _watcher = new Thread(this);
    _watcher.setName(String
        .format("ClusterManager_Watcher_%s_%s_%s_%d", _clusterName, _instanceName, _type.name(), _uid));
    LOG.debug("ClusterManager_watcher_{}_{}_{}_{} started, stacktrace {}", _clusterName, _instanceName, _type.name(), _uid, Thread.currentThread().getStackTrace());
    _watcher.start();

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
  public void finalize() {
    _watcher.interrupt();
    try {
      _watcher.join(DISCONNECT_WAIT_TIME_MS);
    } catch (InterruptedException e) {
      LOG.error("ClusterManager watcher cleanup in the finalize method was interrupted.", e);
    } finally {
      if (isConnected()) {
        LOG.warn(
            "The HelixManager ({}-{}-{}) is still connected after {} ms wait. This is a potential resource leakage!",
            _clusterName, _instanceName, _type.name(), DISCONNECT_WAIT_TIME_MS);
      }
    }
  }
}

