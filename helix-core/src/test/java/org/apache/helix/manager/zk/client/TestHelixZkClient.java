package org.apache.helix.manager.zk.client;

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

import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixZkClient extends ZkUnitTestBase {
  final String TEST_NODE = "/test_helix_zkclient";

  @Test public void testZkConnectionManager() {
    final String TEST_ROOT = "/testZkConnectionManager/IDEALSTATES";
    final String TEST_PATH = TEST_ROOT + TEST_NODE;

    ZkConnectionManager zkConnectionManager =
        new ZkConnectionManager(new ZkConnection(ZK_ADDR), HelixZkClient.DEFAULT_CONNECTION_TIMEOUT,
            null);
    Assert.assertTrue(zkConnectionManager.waitUntilConnected(1, TimeUnit.SECONDS));

    // This client can write/read from ZK
    zkConnectionManager.createPersistent(TEST_PATH, true);
    zkConnectionManager.writeData(TEST_PATH, "Test");
    Assert.assertTrue(zkConnectionManager.readData(TEST_PATH) != null);
    zkConnectionManager.deleteRecursively(TEST_ROOT);

    // This client can be shared, and cannot close when sharing
    SharedZkClient sharedZkClient =
        new SharedZkClient(zkConnectionManager, new HelixZkClient.ZkClientConfig(), null);
    try {
      zkConnectionManager.close();
      Assert.fail("Dedicated ZkClient cannot be closed while sharing!");
    } catch (HelixException hex) {
      // expected
    }

    // This client can be closed normally when sharing ends
    sharedZkClient.close();
    Assert.assertTrue(sharedZkClient.isClosed());
    Assert.assertFalse(sharedZkClient.waitUntilConnected(100, TimeUnit.MILLISECONDS));

    zkConnectionManager.close();
    Assert.assertTrue(zkConnectionManager.isClosed());
    Assert.assertFalse(zkConnectionManager.waitUntilConnected(100, TimeUnit.MILLISECONDS));

    // Sharing a closed dedicated ZkClient shall fail
    try {
      new SharedZkClient(zkConnectionManager, new HelixZkClient.ZkClientConfig(), null);
      Assert.fail("Sharing a closed dedicated ZkClient shall fail.");
    } catch (HelixException hex) {
      // expected
    }
  }

  @Test(dependsOnMethods = "testZkConnectionManager") public void testSharingZkClient()
      throws Exception {
    final String TEST_ROOT = "/testSharedZkClient/IDEALSTATES";
    final String TEST_PATH = TEST_ROOT + TEST_NODE;

    // A factory just for this tests, this for avoiding the impact from other tests running in parallel.
    final SharedZkClientFactory testFactory = new SharedZkClientFactory();

    HelixZkClient.ZkConnectionConfig connectionConfig =
        new HelixZkClient.ZkConnectionConfig(ZK_ADDR);
    HelixZkClient sharedZkClientA =
        testFactory.buildZkClient(connectionConfig, new HelixZkClient.ZkClientConfig());
    Assert.assertTrue(sharedZkClientA.waitUntilConnected(1, TimeUnit.SECONDS));

    HelixZkClient sharedZkClientB =
        testFactory.buildZkClient(connectionConfig, new HelixZkClient.ZkClientConfig());
    Assert.assertTrue(sharedZkClientB.waitUntilConnected(1, TimeUnit.SECONDS));

    Assert.assertEquals(testFactory.getActiveConnectionCount(), 1);

    // client A and B is sharing the same session.
    Assert.assertEquals(sharedZkClientA.getSessionId(), sharedZkClientB.getSessionId());
    long sessionId = sharedZkClientA.getSessionId();

    final int[] notificationCountA = { 0, 0 };
    sharedZkClientA.subscribeDataChanges(TEST_PATH, new IZkDataListener() {
      @Override public void handleDataChange(String s, Object o) {
        notificationCountA[0]++;
      }

      @Override public void handleDataDeleted(String s) {
        notificationCountA[1]++;
      }
    });
    final int[] notificationCountB = { 0, 0 };
    sharedZkClientB.subscribeDataChanges(TEST_PATH, new IZkDataListener() {
      @Override public void handleDataChange(String s, Object o) {
        notificationCountB[0]++;
      }

      @Override public void handleDataDeleted(String s) {
        notificationCountB[1]++;
      }
    });

    // Modify using client A and client B will get notification.
    sharedZkClientA.createPersistent(TEST_PATH, true);
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() {
        return notificationCountB[0] == 1;
      }
    }, 1000));
    Assert.assertEquals(notificationCountB[1], 0);

    sharedZkClientA.deleteRecursively(TEST_ROOT);
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() {
        return notificationCountB[1] == 1;
      }
    }, 1000));
    Assert.assertEquals(notificationCountB[0], 1);

    try {
      sharedZkClientA.createEphemeral(TEST_PATH, true);
      Assert.fail("Create Ephemeral nodes using shared client should fail.");
    } catch (HelixException he) {
      // expected.
    }

    sharedZkClientA.close();
    // Shared client A closed.
    Assert.assertTrue(sharedZkClientA.isClosed());
    Assert.assertFalse(sharedZkClientA.waitUntilConnected(100, TimeUnit.MILLISECONDS));
    // Shared client B still open.
    Assert.assertFalse(sharedZkClientB.isClosed());
    Assert.assertTrue(sharedZkClientB.waitUntilConnected(100, TimeUnit.MILLISECONDS));

    // client A cannot do any modify once closed.
    try {
      sharedZkClientA.createPersistent(TEST_PATH, true);
      Assert.fail("Should not be able to create node with a closed client.");
    } catch (Exception e) {
      // expected to be here.
    }

    // Now modify using client B, and client A won't get notification.
    sharedZkClientB.createPersistent(TEST_PATH, true);
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() {
        return notificationCountB[0] == 2;
      }
    }, 1000));
    Assert.assertFalse(TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() {
        return notificationCountA[0] == 2;
      }
    }, 1000));
    sharedZkClientB.deleteRecursively(TEST_ROOT);

    Assert.assertEquals(testFactory.getActiveConnectionCount(), 1);

    sharedZkClientB.close();
    // Shared client B closed.
    Assert.assertTrue(sharedZkClientB.isClosed());
    Assert.assertFalse(sharedZkClientB.waitUntilConnected(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(testFactory.getActiveConnectionCount(), 0);

    // Try to create new shared ZkClient, will get a different session
    HelixZkClient sharedZkClientC =
        testFactory.buildZkClient(connectionConfig, new HelixZkClient.ZkClientConfig());
    Assert.assertFalse(sessionId == sharedZkClientC.getSessionId());
    Assert.assertEquals(testFactory.getActiveConnectionCount(), 1);

    sharedZkClientC.close();
    // Shared client C closed.
    Assert.assertTrue(sharedZkClientC.isClosed());
    Assert.assertFalse(sharedZkClientC.waitUntilConnected(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(testFactory.getActiveConnectionCount(), 0);
  }
}
