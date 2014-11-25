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
import java.util.Map;
import java.util.Random;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper.Verifier;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * This is for testing Helix controller livelock @see Helix-541
 * The test has a high probability to reproduce the problem
 */
public class TestControllerLiveLock extends ZkUnitTestBase {
  private static final Logger LOG = Logger.getLogger(TestControllerLiveLock.class);

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 12;
    final int p = 256;
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    final HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        p, // partitions per resource
        n, // number of nodes
        1, // replicas
        "LeaderStandby", RebalanceMode.FULL_AUTO, true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    Random random = new Random();
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
      Thread.sleep(Math.abs(random.nextInt()) % 500 + 500);
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // make sure all partitions are assigned and no partitions is assigned to STANDBY state
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        ExternalView extView = accessor.getProperty(keyBuilder.externalView("TestDB0"));
        for (int i = 0; i < p; i++) {
          String partition = "TestDB0_" + i;
          Map<String, String> map = extView.getRecord().getMapField(partition);
          if (map == null || map.size() != 1) {
            return false;
          }
        }
        return true;
      }
    }, 10 * 1000);

    if (!result) {
      ExternalView extView = accessor.getProperty(keyBuilder.externalView("TestDB0"));
      for (int i = 0; i < p; i++) {
        String partition = "TestDB0_" + i;
        Map<String, String> map = extView.getRecord().getMapField(partition);
        if (map == null || map.size() != 1) {
          LOG.error(partition + ": " + map);
        }
      }
    }
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
