package org.apache.helix.integration;

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

import java.util.Date;

import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.CustomCodeCallbackHandler;
import org.apache.helix.participant.HelixCustomCodeRunner;
import org.apache.helix.tools.ClusterStateVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixCustomCodeRunner extends ZkTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestHelixCustomCodeRunner.class);

  private final String _clusterName = "CLUSTER_" + getShortClassName();
  private final MockCallback _callback = new MockCallback();

  class MockCallback implements CustomCodeCallbackHandler {
    boolean _isCallbackInvoked;

    @Override
    public void onCallback(NotificationContext context) {
      _isCallbackInvoked = true;
      // System.out.println(type + ": TestCallback invoked on " + manager.getInstanceName());
    }

  }

  private void registerCustomCodeRunner(HelixManager manager) {
    try {
      // delay the start of the 1st participant
      // so there will be a leadership transfer from localhost_12919 to 12918
      if (manager.getInstanceName().equals("localhost_12918")) {
        Thread.sleep(2000);
      }

      HelixCustomCodeRunner customCodeRunner = new HelixCustomCodeRunner(manager, ZK_ADDR);
      customCodeRunner.invoke(_callback).on(ChangeType.LIVE_INSTANCE)
          .usingLeaderStandbyModel("TestParticLeader").start();
    } catch (Exception e) {
      LOG.error("Exception do pre-connect job", e);
    }
  }

  @Test
  public void testCustomCodeRunner() throws Exception {
    System.out.println("START " + _clusterName + " at " + new Date(System.currentTimeMillis()));

    int nodeNb = 5;
    int startPort = 12918;
    TestHelper.setupCluster(_clusterName, ZK_ADDR, startPort, "localhost", // participant name
                                                                           // prefix
        "TestDB", // resource name prefix
        1, // resourceNb
        5, // partitionNb
        nodeNb, // nodesNb
        nodeNb, // replica
        "MasterSlave", true);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    controller.syncStart();

    MockParticipantManager[] participants = new MockParticipantManager[5];
    for (int i = 0; i < nodeNb; i++) {
      String instanceName = "localhost_" + (startPort + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);

      registerCustomCodeRunner(participants[i]);
      participants[i].syncStart();
    }
    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName));
    Assert.assertTrue(result);

    Thread.sleep(1000); // wait for the INIT type callback to finish
    Assert.assertTrue(_callback._isCallbackInvoked);
    _callback._isCallbackInvoked = false;

    // add a new live instance and its instance config.
    // instance name: localhost_1000
    int[] newLiveInstance = new int[]{1000};
    setupInstances(_clusterName, newLiveInstance);
    setupLiveInstances(_clusterName, newLiveInstance);

    Thread.sleep(1000); // wait for the CALLBACK type callback to finish
    Assert.assertTrue(_callback._isCallbackInvoked);

    // clean up
    controller.syncStop();
    for (int i = 0; i < nodeNb; i++) {
      participants[i].syncStop();
    }
    deleteLiveInstances(_clusterName);
    deleteCluster(_clusterName);

    System.out.println("END " + _clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
