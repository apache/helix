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

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.HelixConnection;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.mock.participant.DummyProcess.DummyLeaderStandbyStateModelFactory;
import org.apache.helix.mock.participant.DummyProcess.DummyOnlineOfflineStateModelFactory;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.mock.participant.MockSchemataModelFactory;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;

public class MockParticipant extends ZKHelixManager implements Runnable {
  private static Logger LOG = Logger.getLogger(MockParticipant.class);

  private final CountDownLatch _startCountDown = new CountDownLatch(1);
  private final CountDownLatch _stopCountDown = new CountDownLatch(1);
  private final CountDownLatch _waitStopCompleteCountDown = new CountDownLatch(1);

  private final MockMSModelFactory _msModelFactory = new MockMSModelFactory(null);


  public MockParticipant(String zkAddress, String clusterName, String instanceName) {
    super(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddress);
  }

  public void setTransition(MockTransition transition) {
    _msModelFactory.setTrasition(transition);
  }

  public void syncStop() {
    _stopCountDown.countDown();
    try {
      _waitStopCompleteCountDown.await();
    } catch (InterruptedException e) {
      LOG.error("exception in syncStop participant-manager", e);
    }
  }

  public void syncStart() {
    try {
      new Thread(this).start();
      _startCountDown.await();
    } catch (InterruptedException e) {
      LOG.error("exception in syncStart participant-manager", e);
    }
  }

  @Override
  public void run() {
    try {
      StateMachineEngine stateMach = getStateMachineEngine();
      stateMach.registerStateModelFactory(StateModelDefId.MasterSlave, _msModelFactory);

      DummyLeaderStandbyStateModelFactory lsModelFactory =
          new DummyLeaderStandbyStateModelFactory(10);
      DummyOnlineOfflineStateModelFactory ofModelFactory =
          new DummyOnlineOfflineStateModelFactory(10);
      stateMach.registerStateModelFactory(StateModelDefId.LeaderStandby, lsModelFactory);
      stateMach.registerStateModelFactory(StateModelDefId.OnlineOffline, ofModelFactory);

      MockSchemataModelFactory schemataFactory = new MockSchemataModelFactory();
      stateMach.registerStateModelFactory(StateModelDefId.from("STORAGE_DEFAULT_SM_SCHEMATA"), schemataFactory);

      connect();
      _startCountDown.countDown();

      _stopCountDown.await();
    } catch (InterruptedException e) {
      String msg =
          "participant: " + getInstanceName() + ", " + Thread.currentThread().getName()
              + " is interrupted";
      LOG.info(msg);
    } catch (Exception e) {
      LOG.error("exception running participant-manager", e);
    } finally {
      _startCountDown.countDown();

      disconnect();
      _waitStopCompleteCountDown.countDown();
    }
  }

  public HelixConnection getConn() {
    return _role.getConnection();
  }

  public ZkClient getZkClient() {
    ZkHelixConnection conn = (ZkHelixConnection)getConn();
    return conn._zkclient;
  }

  public List<ZkCallbackHandler> getHandlers() {
    ZkHelixConnection conn = (ZkHelixConnection)getConn();
    return conn._handlers.get(_role);
  }

  public ZkHelixParticipant getParticipant() {
    return (ZkHelixParticipant) _role;
  }
}
