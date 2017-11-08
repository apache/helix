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


import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkManagerFlappingDetection extends ZkIntegrationTestBase {
  @Test
  public void testDisconnectHistory() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    String instanceName = "localhost_" + (12918 + 0);
    MockParticipantManager manager = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);

    manager.connect();
    ZkClient zkClient = manager.getZkClient();
    ZkTestHelper.expireSession(zkClient);
    for (int i = 0; i < 4; i++) {
      ZkTestHelper.expireSession(zkClient);
      Thread.sleep(1000);
      if (i < 5) {
        // System.out.println(i);
        Assert.assertTrue(manager.isConnected());
      }
    }
    ZkTestHelper.disconnectSession(zkClient);
    for (int i = 0; i < 20; i++) {
      Thread.sleep(500);
      System.out.println(i);
      if (!manager.isConnected())
        break;
    }
    Assert.assertFalse(manager.isConnected());
  }

  // TODO test was disabled. check if it is still needed
  // @Test(enabled = false)
  // public void testDisconnectFlappingWindow() throws Exception {
  // String className = TestHelper.getTestClassName();
  // String methodName = TestHelper.getTestMethodName();
  // String instanceName = "localhost_" + (12918 + 1);
  // final String clusterName = className + "_" + methodName + UUID.randomUUID();
  //
  // testDisconnectFlappingWindow2(instanceName, InstanceType.PARTICIPANT);
  // testDisconnectFlappingWindow2("admin", InstanceType.ADMINISTRATOR);
  // }
  //
  // public void testDisconnectFlappingWindow2(String instanceName, InstanceType type)
  // throws Exception {
  // String className = TestHelper.getTestClassName();
  // String methodName = TestHelper.getTestMethodName();
  // final String clusterName = className + "_" + methodName + UUID.randomUUID();
  //
  // TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
  // "localhost", // participant name prefix
  // "TestDB", // resource name prefix
  // 1, // resources
  // 10, // partitions per resource
  // 5, // number of nodes
  // 3, // replicas
  // "MasterSlave", true); // do rebalance
  //
  // // flapping time window to 5 sec
  // System.setProperty("helixmanager.flappingTimeWindow", "15000");
  // System.setProperty("helixmanager.maxDisconnectThreshold", "7");
  // ZkHelixTestManager manager2 = new ZkHelixTestManager(clusterName, instanceName, type, ZK_ADDR);
  // manager2.connect();
  // ZkClient zkClient = manager2.getZkClient();
  // for (int i = 0; i < 3; i++) {
  // ZkTestHelper.expireSession(zkClient);
  // Thread.sleep(500);
  // Assert.assertTrue(manager2.isConnected());
  // }
  // Thread.sleep(15000);
  // // Old entries should be cleaned up
  // for (int i = 0; i < 7; i++) {
  // ZkTestHelper.expireSession(zkClient);
  // Thread.sleep(1000);
  // Assert.assertTrue(manager2.isConnected());
  // }
  // ZkTestHelper.disconnectSession(zkClient);
  // for (int i = 0; i < 20; i++) {
  // Thread.sleep(500);
  // if (!manager2.isConnected())
  // break;
  // }
  // Assert.assertFalse(manager2.isConnected());
  // }

  // @Test
  public void testDisconnectFlappingWindowController() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // flapping time window to 5 sec
    System.setProperty("helixmanager.flappingTimeWindow", "5000");
    System.setProperty("helixmanager.maxDisconnectThreshold", "3");
    ClusterControllerManager manager2 = new ClusterControllerManager(ZK_ADDR, clusterName, null);
    manager2.connect();
    Thread.sleep(100);
    ZkClient zkClient = manager2.getZkClient();
    for (int i = 0; i < 2; i++) {
      ZkTestHelper.expireSession(zkClient);
      Thread.sleep(500);
      Assert.assertTrue(manager2.isConnected());
    }
    Thread.sleep(5000);
    // Old entries should be cleaned up
    for (int i = 0; i < 3; i++) {
      ZkTestHelper.expireSession(zkClient);
      Thread.sleep(500);
      Assert.assertTrue(manager2.isConnected());
    }
    ZkTestHelper.disconnectSession(zkClient);
    for (int i = 0; i < 20; i++) {
      Thread.sleep(500);
      if (!manager2.isConnected())
        break;
    }
    Assert.assertFalse(manager2.isConnected());
  }
}
