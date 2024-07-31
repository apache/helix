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
import org.apache.helix.HelixCloudProperty;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.HelixPropertyFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.gateway.base.statemodel.MockOFModelFactory;
import org.apache.helix.gateway.base.statemodel.MockTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockParticipantManager extends ClusterManager {
  private static final Logger LOG = LoggerFactory.getLogger(MockParticipantManager.class);

  protected int _transDelay = 10;

  protected MockOFModelFactory _ofModelFactory;
  protected HelixCloudProperty _helixCloudProperty;

  public MockParticipantManager(String zkAddr, String clusterName, String instanceName) {
    this(zkAddr, clusterName, instanceName, 10);
  }

  public MockParticipantManager(String zkAddr, String clusterName, String instanceName,
      int transDelay) {
    this(zkAddr, clusterName, instanceName, transDelay, null);
  }

  public MockParticipantManager(String zkAddr, String clusterName, String instanceName,
      int transDelay, HelixCloudProperty helixCloudProperty) {
    this(zkAddr, clusterName, instanceName, transDelay, helixCloudProperty,
        HelixPropertyFactory.getInstance().getHelixManagerProperty(zkAddr, clusterName));
  }

  public MockParticipantManager(String zkAddr, String clusterName, String instanceName,
      int transDelay, HelixCloudProperty helixCloudProperty,
      HelixManagerProperty helixManagerProperty) {
    super(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddr, null, helixManagerProperty);
    _transDelay = transDelay;
    _ofModelFactory = new MockOFModelFactory(null);
    _helixCloudProperty = helixCloudProperty;
  }

  public MockParticipantManager(String clusterName, String instanceName,
      HelixManagerProperty helixManagerProperty, int transDelay,
      HelixCloudProperty helixCloudProperty) {
    super(clusterName, instanceName, InstanceType.PARTICIPANT, null, null, helixManagerProperty);
    _transDelay = transDelay;
    _ofModelFactory = new MockOFModelFactory();
    _helixCloudProperty = helixCloudProperty;
  }

  public void setTransition(MockTransition transition) {
    _ofModelFactory.setTrasition(transition);
  }

  /**
   * This method should be called before syncStart() called after syncStop()
   */
  public void reset() {
    syncStop();
    _startCountDown = new CountDownLatch(1);
    _stopCountDown = new CountDownLatch(1);
    _waitStopFinishCountDown = new CountDownLatch(1);
  }

  @Override
  public void run() {
    try {
      StateMachineEngine stateMach = getStateMachineEngine();
      stateMach.registerStateModelFactory(BuiltInStateModelDefinitions.OnlineOffline.name(),
          _ofModelFactory);
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
      _waitStopFinishCountDown.countDown();
    }
  }
  @Override
  public void finalize() {
    super.finalize();
  }
}
