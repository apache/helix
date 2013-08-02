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


import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.PreConnectCallback;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConsecutiveZkSessionExpiry extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestConsecutiveZkSessionExpiry.class);

  /**
   * make use of PreConnectCallback to insert session expiry during HelixManager#handleNewSession()
   */
  class PreConnectTestCallback implements PreConnectCallback {
    final String instanceName;
    final CountDownLatch startCountDown;
    final CountDownLatch endCountDown;
    int count = 0;

    public PreConnectTestCallback(String instanceName, CountDownLatch startCountdown,
                                  CountDownLatch endCountdown) {
      this.instanceName = instanceName;
      this.startCountDown = startCountdown;
      this.endCountDown = endCountdown;
    }

    @Override
    public void onPreConnect()
    {
      // TODO Auto-generated method stub
      LOG.info("handleNewSession for instance: " + instanceName + ", count: " + count);
      if (count++ == 1) {
        startCountDown.countDown();
        LOG.info("wait session expiry to happen");
        try
        {
          endCountDown.await();
        }
        catch (Exception e)
        {
          LOG.error("interrupted in waiting", e);
        }
      }
    }

  }

  @Test
  public void test() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName,
                            ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            32, // partitions per resource
                            n, // number of nodes
                            2, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // start controller
    final ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.connect();

    // start participants
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++)
    {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, instanceName);

      if (i == 0) {
        participants[i].addPreConnectCallback(new PreConnectTestCallback(instanceName,
                                                                         startCountdown,
                                                                         endCountdown));
      }
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                      clusterName));
    Assert.assertTrue(result);

    // expire the session of participant
    LOG.info("1st Expiring participant session...");
    String oldSessionId = participants[0].getSessionId();

    ZkTestHelper.asyncExpireSession(participants[0].getZkClient());
    String newSessionId = participants[0].getSessionId();
    LOG.info("Expried participant session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    // expire zk session again during HelixManager#handleNewSession()
    startCountdown.await();
    LOG.info("2nd Expiring participant session...");
    oldSessionId = participants[0].getSessionId();

    ZkTestHelper.asyncExpireSession(participants[0].getZkClient());
    newSessionId = participants[0].getSessionId();
    LOG.info("Expried participant session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    endCountdown.countDown();

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                   clusterName));
    Assert.assertTrue(result);

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }

}
