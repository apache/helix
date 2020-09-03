package org.apache.helix;

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

import org.apache.helix.mock.participant.DummyProcess.DummyLeaderStandbyStateModelFactory;
import org.apache.helix.mock.participant.DummyProcess.DummyMasterSlaveStateModelFactory;
import org.apache.helix.mock.participant.DummyProcess.DummyOnlineOfflineStateModelFactory;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyProcessThread implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DummyProcessThread.class);
  private final HelixManager _manager;
  private final String _instanceName;

  public DummyProcessThread(HelixManager manager, String instanceName) {
    _manager = manager;
    _instanceName = instanceName;
  }

  @Override
  public void run() {
    try {
      DummyMasterSlaveStateModelFactory stateModelFactory = new DummyMasterSlaveStateModelFactory(0);
      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      stateMach.registerStateModelFactory("MasterSlave", stateModelFactory);

      DummyLeaderStandbyStateModelFactory stateModelFactory1 =
          new DummyLeaderStandbyStateModelFactory(10);
      DummyOnlineOfflineStateModelFactory stateModelFactory2 =
          new DummyOnlineOfflineStateModelFactory(10);
      stateMach.registerStateModelFactory("LeaderStandby", stateModelFactory1);
      stateMach.registerStateModelFactory("OnlineOffline", stateModelFactory2);

      _manager.connect();
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      String msg =
          "participant:" + _instanceName + ", " + Thread.currentThread().getName() + " interrupted";
      LOG.info(msg);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
