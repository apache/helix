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

import org.apache.helix.integration.ZkStandAloneCMTestBase;
import org.testng.Assert;

public class TestZkStateChangeListener extends ZkStandAloneCMTestBase {
  // TODO this test has been covered by TestZkFlapping. check if still needed
  // @Test
  public void testDisconnectHistory() throws Exception {
    // String controllerName = CONTROLLER_PREFIX + "_0";
    // StartCMResult controllerResult = _startCMResultMap.get(controllerName);
    // ZKHelixManager controller = (ZKHelixManager) controllerResult._manager;
    // ZkStateChangeListener listener1 = new ZkStateChangeListener(controller, 5000, 10);
    // ZkStateChangeListener listener1 = new ZkStateChangeListener(_controller, 5000, 10);

    // 11 disconnects in 5 sec
    for (int i = 0; i < 11; i++) {
      Thread.sleep(200);
      // _controller.handleStateChanged(KeeperState.Disconnected);
      if (i < 10) {
        Assert.assertTrue(_controller.isConnected());
      } else {
        Assert.assertFalse(_controller.isConnected());
      }
    }

    // If maxDisconnectThreshold is 0 it should be set to 1
    // String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + 0);
    // ZKHelixManager manager = (ZKHelixManager) _startCMResultMap.get(instanceName)._manager;

    // ZkStateChangeListener listener2 = new ZkStateChangeListener(_participants[0], 5000, 0);
    for (int i = 0; i < 2; i++) {
      Thread.sleep(200);
      // _participants[0].handleStateChanged(KeeperState.Disconnected);
      if (i < 1) {
        Assert.assertTrue(_participants[0].isConnected());
      } else {
        Assert.assertFalse(_participants[0].isConnected());
      }
    }

    // If there are long time after disconnect, older history should be cleanup
    // instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + 1);
    // manager = (ZKHelixManager) _startCMResultMap.get(instanceName)._manager;

    // ZkStateChangeListener listener3 = new ZkStateChangeListener(_participants[1], 5000, 5);
    for (int i = 0; i < 3; i++) {
      Thread.sleep(200);
      // _participants[1].handleStateChanged(KeeperState.Disconnected);
      Assert.assertTrue(_participants[1].isConnected());
    }
    Thread.sleep(5000);
    // Old entries should be cleaned up
    for (int i = 0; i < 3; i++) {
      Thread.sleep(200);
      // _participants[1].handleStateChanged(KeeperState.Disconnected);
      Assert.assertTrue(_participants[1].isConnected());
    }
    for (int i = 0; i < 2; i++) {
      Thread.sleep(200);
      // _participants[1].handleStateChanged(KeeperState.Disconnected);
      Assert.assertTrue(_participants[1].isConnected());
    }
    // _participants[1].handleStateChanged(KeeperState.Disconnected);
    Assert.assertFalse(_participants[1].isConnected());
  }
}
