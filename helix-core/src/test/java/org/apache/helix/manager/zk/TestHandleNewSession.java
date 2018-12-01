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

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Semaphore;

import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.zookeeper.ZkClient;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHandleNewSession extends ZkTestBase {
  @Test
  public void testHandleNewSession() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, clusterName, "localhost_12918");
    participant.syncStart();

    // Logger.getRootLogger().setLevel(Level.INFO);
    String lastSessionId = participant.getSessionId();
    for (int i = 0; i < 3; i++) {
      // System.err.println("curSessionId: " + lastSessionId);
      ZkTestHelper.expireSession(participant.getZkClient());

      String sessionId = participant.getSessionId();
      Assert.assertTrue(sessionId.compareTo(lastSessionId) > 0,
          "Session id should be increased after expiry");
      lastSessionId = sessionId;

      // make sure session id is not 0
      Assert.assertFalse(sessionId.equals("0"),
          "Hit race condition in zhclient.handleNewSession(). sessionId is not returned yet.");

      // TODO: need to test session expiry during handleNewSession()
    }

    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("Disconnecting ...");
    participant.syncStop();
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testHandleNewSession")
  public void testAcquireLeadershipOnNewSession() throws Exception {
    String className = getShortClassName();
    final String clusterName =
        CLUSTER_PREFIX + "_" + className + "_" + "testAcquireLeadershipOnNewSession";
    final ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    // Create controller leader
    final String controllerName = "controller_0";
    final BlockingZkHelixManager manager =
        new BlockingZkHelixManager(clusterName, controllerName, InstanceType.CONTROLLER, ZK_ADDR);
    GenericHelixController controller0 = new GenericHelixController();
    DistributedLeaderElection election =
        new DistributedLeaderElection(manager, controller0, Collections.EMPTY_LIST);
    manager.connect();

    // Ensure the controller successfully acquired leadership.
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
        return liveInstance != null && controllerName.equals(liveInstance.getInstanceName())
            && manager.getSessionId().equals(liveInstance.getSessionId());
      }
    }, 1000));
    // Record the original connection info.
    final String originalSessionId = manager.getSessionId();
    final long originalCreationTime =
        accessor.getProperty(keyBuilder.controllerLeader()).getStat().getCreationTime();

    // 1. lock the zk event processing to simulate long backlog queue.
    ((ZkClient) manager._zkclient).getEventLock().lockInterruptibly();
    // 2. add a controller leader node change event to the queue, that will not be processed.
    accessor.removeProperty(keyBuilder.controllerLeader());
    // 3. expire the session and create a new session
    ZkTestHelper.asyncExpireSession(manager._zkclient);
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        return !((ZkClient) manager._zkclient).getConnection().getZookeeperState().isAlive();
      }
    }, 3000));
    // 4. start processing event again
    ((ZkClient) manager._zkclient).getEventLock().unlock();

    // Wait until the ZkClient has got a new session, and the original leader node gone
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        try {
          return !Long.toHexString(manager._zkclient.getSessionId()).equals(originalSessionId);
        } catch (HelixException hex) {
          return false;
        }
      }
    }, 2000));
    // ensure that the manager has not process the new session event yet
    Assert.assertEquals(manager.getSessionId(), originalSessionId);

    // Wait until an invalid leader node created again.
    // Note that this is the expected behavior but NOT desired behavior. Ideally, the new node should
    // be created with the right session directly. We will need to improve this.
    // TODO We should recording session Id in the zk event so the stale events are discarded instead of processed. After this is done, there won't be invalid node.
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        // Newly created node should have a new creating time but with old session.
        LiveInstance invalidLeaderNode = accessor.getProperty(keyBuilder.controllerLeader());
        // node exist
        if (invalidLeaderNode == null)
          return false;
        // node is newly created
        if (invalidLeaderNode.getStat().getCreationTime() == originalCreationTime)
          return false;
        // node has the same session as the old one, so it's invalid
        if (!invalidLeaderNode.getSessionId().equals(originalSessionId))
          return false;
        return true;
      }
    }, 2000));
    Assert.assertFalse(manager.isLeader());

    // 5. proceed the new session handling, so the manager will get the new session.
    manager.proceedNewSessionHandling();
    // Since the new session handling will re-create the leader node, a new valid node shall be created.
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        return manager.isLeader();
      }
    }, 1000));

    manager.disconnect();
    TestHelper.dropCluster(clusterName, _gZkClient);
  }

  class BlockingZkHelixManager extends ZKHelixManager {
    private final Semaphore newSessionHandlingCount = new Semaphore(1);

    public BlockingZkHelixManager(String clusterName, String instanceName,
        InstanceType instanceType, String zkAddress) {
      super(clusterName, instanceName, instanceType, zkAddress);
    }

    @Override
    public void handleNewSession() throws Exception {
      newSessionHandlingCount.acquire();
      super.handleNewSession();
    }

    void proceedNewSessionHandling() {
      newSessionHandlingCount.release();
    }
  }
}
