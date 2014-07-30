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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.TestHelper.Verifier;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkFlapping extends ZkTestBase {

  @Test
  public void testZkSessionExpiry() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ZkClient client =
        new ZkClient(_zkaddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());

    String path = String.format("/%s", clusterName);
    client.createEphemeral(path);
    String oldSessionId = ZkTestHelper.getSessionId(client);
    ZkTestHelper.expireSession(client);
    String newSessionId = ZkTestHelper.getSessionId(client);
    Assert.assertNotSame(newSessionId, oldSessionId);
    Assert.assertFalse(client.exists(path), "Ephemeral znode should be gone after session expiry");
    client.close();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCloseZkClient() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ZkClient client =
        new ZkClient(_zkaddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    String path = String.format("/%s", clusterName);
    client.createEphemeral(path);

    client.close();
    Assert.assertFalse(_zkclient.exists(path), "Ephemeral node: " + path
        + " should be removed after ZkClient#close()");
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCloseZkClientInZkClientEventThread() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    final CountDownLatch waitCallback = new CountDownLatch(1);
    final ZkClient client =
        new ZkClient(_zkaddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    String path = String.format("/%s", clusterName);
    client.createEphemeral(path);
    client.subscribeDataChanges(path, new IZkDataListener() {

      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
      }

      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
        client.close();
        waitCallback.countDown();
      }
    });

    client.writeData(path, new ZNRecord("test"));
    waitCallback.await();
    Assert.assertFalse(_zkclient.exists(path), "Ephemeral node: " + path
        + " should be removed after ZkClient#close() in its own event-thread");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  class ZkStateCountListener implements IZkStateListener {
    int count = 0;

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
      if (state == KeeperState.Disconnected) {
        count++;
      }
    }

    @Override
    public void handleNewSession() throws Exception {
    }
  }

  @Test
  public void testParticipantFlapping() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        1, // number of nodes
        1, // replicas
        "MasterSlave", false);

    final String instanceName = "localhost_12918";
    MockParticipant participant =
        new MockParticipant(_zkaddr, clusterName, instanceName);
    participant.syncStart();

    final ZkClient client = participant.getZkClient();
    final ZkStateCountListener listener = new ZkStateCountListener();
    client.subscribeStateChanges(listener);

    final AtomicInteger expectDisconnectCnt = new AtomicInteger(0);
    final int n = ZKHelixManager.MAX_DISCONNECT_THRESHOLD;
    for (int i = 0; i < n; i++) {
      String oldSessionId = ZkTestHelper.getSessionId(client);
      ZkTestHelper.simulateZkStateDisconnected(client);
      expectDisconnectCnt.incrementAndGet();
      // wait until we get invoked by zk state change to disconnected
      TestHelper.verify(new Verifier() {

        @Override
        public boolean verify() throws Exception {
          return listener.count == expectDisconnectCnt.get();
        }
      }, 30 * 1000);

      String newSessionId = ZkTestHelper.getSessionId(client);
      Assert.assertEquals(newSessionId, oldSessionId);
    }
    client.unsubscribeStateChanges(listener);
    // make sure participant is NOT disconnected
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    Assert.assertNotNull(liveInstance, "Live-instance should exist after " + n + " disconnects");

    // trigger flapping
    ZkTestHelper.simulateZkStateDisconnected(client);
    // wait until we get invoked by zk state change to disconnected
    boolean success = TestHelper.verify(new Verifier() {

      @Override
      public boolean verify() throws Exception {
        return client.getShutdownTrigger();
      }
    }, 30 * 1000);

    Assert.assertTrue(success, "The " + (n + 1)
        + "th disconnect event should trigger ZkHelixManager#disonnect");

    // make sure participant is disconnected
    success = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
        return liveInstance == null;
      }
    }, 3 * 1000);
    Assert.assertTrue(success, "Live-instance should be gone after " + (n + 1) + " disconnects");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testControllerFlapping() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        1, // number of nodes
        1, // replicas
        "MasterSlave", false);

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    final ZkClient client = controller.getZkClient();
    final ZkStateCountListener listener = new ZkStateCountListener();
    client.subscribeStateChanges(listener);

    final AtomicInteger expectDisconnectCnt = new AtomicInteger(0);
    final int n = ZKHelixManager.MAX_DISCONNECT_THRESHOLD;
    for (int i = 0; i < n; i++) {
      String oldSessionId = ZkTestHelper.getSessionId(client);
      ZkTestHelper.simulateZkStateDisconnected(client);
      expectDisconnectCnt.incrementAndGet();
      // wait until we get invoked by zk state change to disconnected
      TestHelper.verify(new Verifier() {

        @Override
        public boolean verify() throws Exception {
          return listener.count == expectDisconnectCnt.get();
        }
      }, 30 * 1000);

      String newSessionId = ZkTestHelper.getSessionId(client);
      Assert.assertEquals(newSessionId, oldSessionId);
    }

    // make sure controller is NOT disconnected
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader, "Leader should exist after " + n + " disconnects");

    // trigger flapping
    ZkTestHelper.simulateZkStateDisconnected(client);
    // wait until we get invoked by zk state change to disconnected
    boolean success = TestHelper.verify(new Verifier() {

      @Override
      public boolean verify() throws Exception {
        return client.getShutdownTrigger();
      }
    }, 30 * 1000);

    Assert.assertTrue(success, "The " + (n + 1)
        + "th disconnect event should trigger ZkHelixManager#disonnect");

    // make sure controller is disconnected
    success = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
        return leader == null;
      }
    }, 5 * 1000);
    Assert.assertTrue(success, "Leader should be gone after " + (n + 1) + " disconnects");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
