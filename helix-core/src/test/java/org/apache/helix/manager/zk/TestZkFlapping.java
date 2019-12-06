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

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.zookeeper.IZkStateListener;
import org.apache.helix.model.LiveInstance;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkFlapping extends ZkUnitTestBase {
  private final int _disconnectThreshold = 5;

  class ZkStateCountListener implements IZkStateListener {
    int count = 0;

    @Override
    public void handleStateChanged(KeeperState state) {
      if (state == KeeperState.Disconnected) {
        count++;
      }
    }

    @Override
    public void handleNewSession(final String sessionId) {
    }

    @Override
    public void handleSessionEstablishmentError(Throwable var1) {
    }
  }

  @Test
  public void testParticipantFlapping() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    System.setProperty(SystemPropertyKeys.MAX_DISCONNECT_THRESHOLD,
        Integer.toString(_disconnectThreshold));

    try {
      TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
          "localhost", // participant name prefix
          "TestDB", // resource name prefix
          1, // resources
          32, // partitions per resource
          1, // number of nodes
          1, // replicas
          "MasterSlave", false);

      final String instanceName = "localhost_12918";
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participant.syncStart();

      final ZkClient client = (ZkClient) participant.getZkClient();
      final ZkStateCountListener listener = new ZkStateCountListener();
      client.subscribeStateChanges(listener);

      final AtomicInteger expectDisconnectCnt = new AtomicInteger(0);
      final int n = _disconnectThreshold;
      for (int i = 0; i < _disconnectThreshold; i++) {
        String oldSessionId = ZkTestHelper.getSessionId(client);
        ZkTestHelper.simulateZkStateReconnected(client);
        expectDisconnectCnt.incrementAndGet();
        // wait until we get invoked by zk state change to disconnected
        TestHelper.verify(() -> listener.count == expectDisconnectCnt.get(), 30 * 1000);

        String newSessionId = ZkTestHelper.getSessionId(client);
        Assert.assertEquals(newSessionId, oldSessionId);
      }
      client.unsubscribeStateChanges(listener);
      // make sure participant is NOT disconnected
      LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
      Assert.assertNotNull(liveInstance, "Live-instance should exist after " + n + " disconnects");

      // trigger flapping
      ZkTestHelper.simulateZkStateReconnected(client);
      // wait until we get invoked by zk state change to disconnected
      boolean success = TestHelper.verify(client::getShutdownTrigger, 30 * 1000);

      Assert.assertTrue(success,
          "The " + (n + 1) + "th disconnect event should trigger ZkHelixManager#disonnect");

      // make sure participant is disconnected
      success = TestHelper.verify(() -> {
        LiveInstance liveInstance1 = accessor.getProperty(keyBuilder.liveInstance(instanceName));
        return liveInstance1 == null;
      }, 3 * 1000);
      Assert.assertTrue(success, "Live-instance should be gone after " + (n + 1) + " disconnects");
      participant.syncStop();
    } finally {
      System.clearProperty(SystemPropertyKeys.MAX_DISCONNECT_THRESHOLD);
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testControllerFlapping() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    System.setProperty(SystemPropertyKeys.MAX_DISCONNECT_THRESHOLD,
        Integer.toString(_disconnectThreshold));

    try {
      TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
          "localhost", // participant name prefix
          "TestDB", // resource name prefix
          1, // resources
          32, // partitions per resource
          1, // number of nodes
          1, // replicas
          "MasterSlave", false);

      ClusterControllerManager controller =
          new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
      controller.syncStart();

      final ZkClient client = (ZkClient) controller.getZkClient();
      final ZkStateCountListener listener = new ZkStateCountListener();
      client.subscribeStateChanges(listener);

      final AtomicInteger expectDisconnectCnt = new AtomicInteger(0);
      final int n = _disconnectThreshold;
      for (int i = 0; i < n; i++) {
        String oldSessionId = ZkTestHelper.getSessionId(client);
        ZkTestHelper.simulateZkStateReconnected(client);
        expectDisconnectCnt.incrementAndGet();
        // wait until we get invoked by zk state change to disconnected
        TestHelper.verify(() -> listener.count == expectDisconnectCnt.get(), 30 * 1000);

        String newSessionId = ZkTestHelper.getSessionId(client);
        Assert.assertEquals(newSessionId, oldSessionId);
      }

      // make sure controller is NOT disconnected
      LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
      Assert.assertNotNull(leader, "Leader should exist after " + n + " disconnects");

      // trigger flapping
      ZkTestHelper.simulateZkStateReconnected(client);
      // wait until we get invoked by zk state change to disconnected
      boolean success = TestHelper.verify(client::getShutdownTrigger, 30 * 1000);

      Assert.assertTrue(success,
          "The " + (n + 1) + "th disconnect event should trigger ZkHelixManager#disonnect");

      // make sure controller is disconnected
      success = TestHelper.verify(() -> {
        LiveInstance leader1 = accessor.getProperty(keyBuilder.controllerLeader());
        return leader1 == null;
      }, 5 * 1000);
      Assert.assertTrue(success, "Leader should be gone after " + (n + 1) + " disconnects");

      controller.syncStop();
    } finally {
      System.clearProperty(SystemPropertyKeys.MAX_DISCONNECT_THRESHOLD);
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
