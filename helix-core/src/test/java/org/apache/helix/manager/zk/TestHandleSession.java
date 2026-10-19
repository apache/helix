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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHandleSession extends ZkTestBase {
  // Generous polling window for session re-establishment, handler reset and live instance
  // creation. These are fast normally but take seconds on slow CI machines.
  private static final long SESSION_WAIT_DURATION = 30 * 1000L;

  private static final String _className = TestHelper.getTestClassName();

  @Test
  public void testHandleNewSession() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    String clusterName = _className + "_" + methodName;

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
    final BlockingHandleNewSessionZkHelixManager manager =
        new BlockingHandleNewSessionZkHelixManager(clusterName, controllerName,
            InstanceType.CONTROLLER, ZK_ADDR);
    GenericHelixController controller0 = new GenericHelixController();
    DistributedLeaderElection election =
        new DistributedLeaderElection(manager, controller0, Collections.EMPTY_LIST);
    manager.connect();

    // Ensure the controller successfully acquired leadership.
    Assert.assertTrue(TestHelper.verify(() -> {
      LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
      return liveInstance != null && controllerName.equals(liveInstance.getInstanceName())
          && manager.getSessionId().equals(liveInstance.getEphemeralOwner());
    }, 1000));
    // Record the original connection info.
    final String originalSessionId = manager.getSessionId();
    final long originalCreationTime =
        accessor.getProperty(keyBuilder.controllerLeader()).getStat().getCreationTime();

    int handlerCount = manager.getHandlers().size();

    // 1. lock the zk event processing to simulate long backlog queue.
    ((ZkClient) manager._zkclient).getEventLock().lockInterruptibly();
    // 2. add a controller leader node change event to the queue, that will not be processed.
    accessor.removeProperty(keyBuilder.controllerLeader());
    // 3. expire the session and create a new session
    ZkTestHelper.asyncExpireSession(manager._zkclient);
    Assert.assertTrue(TestHelper
        .verify(() -> !((ZkClient) manager._zkclient).getConnection().getZookeeperState().isAlive(),
            3000));
    // 4. start processing event again
    ((ZkClient) manager._zkclient).getEventLock().unlock();

    // Wait until the ZkClient has got a new session, and the original leader node gone
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        return !Long.toHexString(manager._zkclient.getSessionId()).equals(originalSessionId);
      } catch (HelixException hex) {
        return false;
      }
    }, 2000));
    // ensure that the manager has not process the new session event yet
    Assert.assertEquals(manager.getSessionId(), originalSessionId);

    // Wait until an invalid leader node created again.
    // Note that this is the expected behavior but NOT desired behavior. Ideally, the new node should
    // be created with the right session directly. We will need to improve this.
    // TODO We should recording session Id in the zk event so the stale events are discarded instead of processed. After this is done, there won't be invalid node.
    Assert.assertTrue(TestHelper.verify(() -> {
      // Newly created node should have a new creating time but with old session.
      LiveInstance invalidLeaderNode = accessor.getProperty(keyBuilder.controllerLeader());
      // node exist
      if (invalidLeaderNode == null) {
        return false;
      }
      // node is newly created
      if (invalidLeaderNode.getStat().getCreationTime() == originalCreationTime) {
        return false;
      }
      // node has the same session as the old one, so it's invalid
      if (!invalidLeaderNode.getSessionId().equals(originalSessionId)) {
        return false;
      }
      return true;
    }, 2000));
    Assert.assertFalse(manager.isLeader());

    // 5. proceed the new session handling, so the manager will get the new session.
    manager.proceedNewSessionHandling();
    // Since the new session handling will re-create the leader node, a new valid node shall be created.
    Assert.assertTrue(TestHelper.verify(() -> manager.isLeader(), 1000));
    // All the callback handlers shall be recovered.
    Assert.assertTrue(TestHelper.verify(() -> manager.getHandlers().size() == handlerCount, 3000));
    Assert.assertTrue(manager.getHandlers().stream().allMatch(handler -> handler.isReady()));

    manager.disconnect();
    TestHelper.dropCluster(clusterName, _gZkClient);
  }

  @Test (dependsOnMethods = "testAcquireLeadershipOnNewSession")
  public void testRemoveOldSession() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    String clusterName = _className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    String testInstanceName = "localhost_12918";
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, clusterName, testInstanceName);
    participant.syncStart();

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    String testCurrentStateSessionId = "testCurrentStateSessionId";
    _baseAccessor
        .create(keyBuilder.sessions(testInstanceName).toString() + "/" + testCurrentStateSessionId,
            new ZNRecord(testCurrentStateSessionId), AccessOption.PERSISTENT);
    String testTaskCurrentStateSessionId = "testTaskCurrentStateSessionId";
    _baseAccessor.create(keyBuilder.taskCurrentStateSessions(testInstanceName).toString() + "/"
            + testTaskCurrentStateSessionId, new ZNRecord(testTaskCurrentStateSessionId),
        AccessOption.PERSISTENT);

    ZkTestHelper.expireSession(participant.getZkClient());
    // Ensure that the test sessions are removed
    Assert.assertEquals(_gZkClient.getChildren(keyBuilder.sessions(testInstanceName).toString()),
        Collections.emptyList());
    Assert.assertEquals(
        _gZkClient.getChildren(keyBuilder.taskCurrentStateSessions(testInstanceName).toString()),
        Collections.emptyList());

    participant.syncStop();
    deleteCluster(clusterName);
  }

  /*
   * Tests session expiry before calling ZkHelixManager.handleNewSession(sessionId).
   * This test checks to see if the expired sessions would be discarded and the operation would
   * be returned in handleNewSession. The live instance is only created by the latest session.
   * This test does not handle new sessions until creating 2 expired session events, which simulates
   * a long backlog in the event queue. At that time, the first new session is already expired and
   * should be discarded. The live instance is only created by the second new session.
   * Set test timeout to 5 minutes, just in case zk server is dead and the test is hung.
   */
  @Test(timeOut = 5 * 60 * 1000L)
  public void testDiscardExpiredSessions() throws Exception {
    final String methodName = TestHelper.getTestMethodName();
    final String clusterName = _className + "_" + methodName;

    final ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(ZK_ADDR));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    final String instanceName = "localhost_12918";
    final BlockingHandleNewSessionZkHelixManager manager =
        new BlockingHandleNewSessionZkHelixManager(clusterName, instanceName,
            InstanceType.PARTICIPANT, ZK_ADDR);

    manager.connect();

    final String originalSessionId = manager.getSessionId();
    final LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    final long originalLiveInstanceCreationTime = liveInstance.getStat().getCreationTime();

    // Verify current live instance.
    Assert.assertNotNull(liveInstance);
    Assert.assertEquals(liveInstance.getEphemeralOwner(), originalSessionId);

    final int handlerCount = manager.getHandlers().size();
    final long originalNewSessionStartTime = manager.getHandleNewSessionStartTime();

    /*
     * Create 2 expired session events. Followed by the expired sessions, there will be 2 new
     * sessions(S1, S2) created: S0(original) expired -> S1 created -> S1 expired -> S2 created.
     * Session S1 would not create a live instance. Instead, only S2 creates a live instance.
     */
    for (int i = 0; i < 2; i++) {
      final String lastSessionId = ZKUtil.toHexSessionId(manager.getZkClient().getSessionId());
      try {
        // Lock zk event processing to simulate a long backlog queue.
        ((ZkClient) manager.getZkClient()).getEventLock().lockInterruptibly();

        // Async expire the session and create a new session.
        ZkTestHelper.asyncExpireSession(manager.getZkClient());

        // Wait and verify the zookeeper is alive.
        Assert.assertTrue(TestHelper.verify(
            () -> !((ZkClient) manager.getZkClient()).getConnection().getZookeeperState().isAlive(),
            3000L));
      } finally {
        // Unlock to start processing event again.
        ((ZkClient) manager.getZkClient()).getEventLock().unlock();
      }

      // Wait until the ZkClient has got a new session.
      Assert.assertTrue(TestHelper.verify(() -> {
        try {
          final String sessionId = ZKUtil.toHexSessionId(manager.getZkClient().getSessionId());
          return !"0".equals(sessionId) && !sessionId.equals(lastSessionId);
        } catch (HelixException ex) {
          return false;
        }
      }, 2000L));

      // Ensure that the manager has not processed the new session event yet.
      Assert.assertEquals(manager.getHandleNewSessionStartTime(), originalNewSessionStartTime);
    }

    // Start to handle all new sessions.
    for (int i = 0; i < 2; i++) {
      // The live instance is gone and should NOT be created by the expired session.
      Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance(instanceName)));

      final long lastEndTime = manager.getHandleNewSessionEndTime();

      // Proceed the new session handling, so the manager will
      // get the second new session and process it.
      manager.proceedNewSessionHandling();

      // Wait for handling new session to complete.
      Assert.assertTrue(
          TestHelper.verify(() -> manager.getHandleNewSessionEndTime() > lastEndTime, 2000L));
    }

    // From now on, the live instance is created.
    // The latest(the final new one) session id that is valid.
    final String latestSessionId = ZKUtil.toHexSessionId(manager.getZkClient().getSessionId());

    Assert.assertTrue(TestHelper.verify(() -> {
      // Newly created live instance should be created by the latest session
      // and have a new creation time.
      LiveInstance newLiveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
      return newLiveInstance != null
          && newLiveInstance.getStat().getCreationTime() != originalLiveInstanceCreationTime
          && newLiveInstance.getEphemeralOwner().equals(latestSessionId);
    }, 2000L));

    // All the callback handlers shall be recovered.
    Assert.assertTrue(TestHelper.verify(() -> manager.getHandlers().size() == handlerCount, 1000L));
    Assert.assertTrue(manager.getHandlers().stream().allMatch(CallbackHandler::isReady));

    // Clean up.
    manager.disconnect();
    deleteCluster(clusterName);
  }

  /*
   * This test simulates that long time cost in resetting handlers causes zk session expiry, and
   * ephemeral node should not be created by this expired zk session.
   * This test follows belows steps:
   * 1. Original session S0 initialized
   * 2. S0 expired, new session S1 created
   * 3. S1 spends a long time resetting handlers
   * 4. S1 expired, new session S2 created
   * 5. S1 completes resetting handlers, live instance should not be created by the expired S1
   * 6. S2 is valid and creates live instance.
   */
  @Test
  public void testSessionExpiredWhenResetHandlers() throws Exception {
    final String methodName = TestHelper.getTestMethodName();
    final String clusterName = _className + "_" + methodName;

    final ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(ZK_ADDR));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // 1. Original session S0 initialized
    final String instanceName = "localhost_12918";
    final BlockingResetHandlersZkHelixManager manager =
        new BlockingResetHandlersZkHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
            ZK_ADDR);

    manager.connect();

    final String originalSessionId = manager.getSessionId();
    final long initResetHandlersStartTime = manager.getResetHandlersStartTime();
    final LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));

    // Verify current live instance.
    Assert.assertNotNull(liveInstance);
    Assert.assertEquals(liveInstance.getEphemeralOwner(), originalSessionId);

    final int handlerCount = manager.getHandlers().size();
    final long originalCreationTime = liveInstance.getStat().getCreationTime();
    // Released by the helper thread once expired S1 has completed resetting handlers and S2 has
    // started handling its new session, so the main thread can verify S1 did not create the live
    // instance.
    final CountDownLatch s1ResetDoneBlocker = new CountDownLatch(1);
    // Released by the main thread to let the helper thread proceed S2.
    final CountDownLatch helperThreadBlocker = new CountDownLatch(1);
    // Released by the helper thread once it has let S2 proceed.
    final CountDownLatch s2ProceededBlocker = new CountDownLatch(1);
    // Failures in the helper thread are re-thrown on the main thread. They used to be swallowed,
    // so a slow session establishment silently turned into a confusing failure of the final
    // assertions.
    final AtomicReference<Throwable> helperFailure = new AtomicReference<>();

    // Helper thread to help verify zk session states, async expire S1, proceed S1 to reset
    // handlers and release main thread to verify results.
    new Thread(() -> {
      try {
        // Wait for new session S1 is established and starting to reset handlers.
        Assert.assertTrue(TestHelper.verify(() -> !(manager.getSessionId().equals(originalSessionId))
            && manager.getResetHandlersStartTime() > initResetHandlersStartTime,
            SESSION_WAIT_DURATION), "S1 did not start resetting handlers in time");

        // S1's info.
        final String lastSessionId = manager.getSessionId();
        final long lastResetHandlersStartTime = manager.getResetHandlersStartTime();

        ((ZkClient) manager.getZkClient()).getEventLock().lockInterruptibly();
        try {
          // 4. S1 expired, new session S2 created
          ZkTestHelper.asyncExpireSession(manager.getZkClient());

          // Wait and verify the zookeeper is closed. The zk client cannot reconnect while the
          // event lock is held, so S2 only gets established after the unlock below.
          Assert.assertTrue(TestHelper.verify(
              () -> !((ZkClient) manager.getZkClient()).getConnection().getZookeeperState()
                  .isAlive(), SESSION_WAIT_DURATION), "S1 was not expired in time");
        } finally {
          // Unlock to start processing event again.
          ((ZkClient) manager.getZkClient()).getEventLock().unlock();
        }

        // Wait and verify the new session S2 is established.
        Assert.assertTrue(TestHelper.verify(() -> {
          try {
            final String sessionId = ZKUtil.toHexSessionId(manager.getZkClient().getSessionId());
            return !"0".equals(sessionId) && !sessionId.equals(lastSessionId);
          } catch (HelixException ex) {
            // The zk client may still be re-connecting.
            return false;
          }
        }, SESSION_WAIT_DURATION), "S2 was not established in time");

        // Proceed S1 to complete reset handlers and try to create live instance.
        manager.proceedResetHandlers();

        // Wait for S2 to handle new session.
        Assert.assertTrue(TestHelper.verify(() -> !(manager.getSessionId().equals(lastSessionId))
            && manager.getResetHandlersStartTime() > lastResetHandlersStartTime,
            SESSION_WAIT_DURATION), "S2 did not start resetting handlers in time");
      } catch (Throwable t) {
        helperFailure.compareAndSet(null, t);
      } finally {
        // Notify main thread to verify result: expired S1 should not create live instance.
        s1ResetDoneBlocker.countDown();
      }

      try {
        // Wait for notification from main thread to proceed S2.
        helperThreadBlocker.await();

        // Proceed S2.
        // 6. S2 is valid and creates live instance.
        manager.proceedResetHandlers();
      } catch (Throwable t) {
        helperFailure.compareAndSet(null, t);
      } finally {
        s2ProceededBlocker.countDown();
      }
    }).start();

    // Lock zk event processing to simulate a long backlog queue.
    ((ZkClient) manager.getZkClient()).getEventLock().lockInterruptibly();
    try {
      // 2. S0 expired, new session S1 created
      ZkTestHelper.asyncExpireSession(manager.getZkClient());
      // 3. S1 spends a long time resetting handlers during this period.

      // Wait and verify the zookeeper is closed.
      Assert.assertTrue(TestHelper.verify(
          () -> !((ZkClient) manager.getZkClient()).getConnection().getZookeeperState().isAlive(),
          SESSION_WAIT_DURATION));
    } finally {
      // Unlock to start processing event again.
      ((ZkClient) manager.getZkClient()).getEventLock().unlock();
    }

    // Wait for S1 completing resetting handlers.
    s1ResetDoneBlocker.await();
    Assert.assertNull(helperFailure.get(), "Helper thread failed: " + helperFailure.get());

    // 5. S1 completes resetting handlers, live instance should not be created by the expired S1
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance(instanceName)));

    // Notify helper thread to proceed S2.
    helperThreadBlocker.countDown();

    // Wait for the helper thread to let S2 proceed. (The previous version re-used an already
    // released latch here, so it did not wait at all and then only gave S2 a 2s window.)
    s2ProceededBlocker.await();
    Assert.assertNull(helperFailure.get(), "Helper thread failed: " + helperFailure.get());

    // From now on, the live instance is going to be created by S2.
    // The latest(the final new one S2) session id that is valid.
    final String latestSessionId = ZKUtil.toHexSessionId(manager.getZkClient().getSessionId());

    // Finishing the handler reset and writing the live instance to ZK can take seconds on a slow
    // machine, so poll generously.
    Assert.assertTrue(TestHelper.verify(() -> {
      // Newly created live instance should be created by the latest session
      // and have a new creation time.
      LiveInstance newLiveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
      return newLiveInstance != null
          && newLiveInstance.getStat().getCreationTime() != originalCreationTime && newLiveInstance
          .getEphemeralOwner().equals(latestSessionId);
    }, SESSION_WAIT_DURATION));

    // All the callback handlers shall be recovered.
    Assert.assertTrue(TestHelper.verify(() -> manager.getHandlers().size() == handlerCount,
        SESSION_WAIT_DURATION));
    Assert.assertTrue(TestHelper.verify(
        () -> manager.getHandlers().stream().allMatch(CallbackHandler::isReady),
        SESSION_WAIT_DURATION));

    // Clean up.
    manager.disconnect();
    deleteCluster(clusterName);
  }

  class MockLiveInstanceChangeListener implements LiveInstanceChangeListener {
    private final HelixManager _manager;
    private final Set<String> _expectedLiveInstances;

    public MockLiveInstanceChangeListener(HelixManager manager,
        Set<String> expectedLiveInstanceNames) {
      _manager = manager;
      _expectedLiveInstances = expectedLiveInstanceNames;
    }

    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances,
        NotificationContext changeContext) {
      if (changeContext.getType() != NotificationContext.Type.FINALIZE) {
        for (LiveInstance liveInstance : liveInstances) {
          if (_expectedLiveInstances.contains(liveInstance.getInstanceName())) {
            try {
              _manager.addCurrentStateChangeListener(
                  (CurrentStateChangeListener) (instanceName, statesInfo, currentStateChangeContext) -> {
                    // empty callback
                  }, liveInstance.getInstanceName(), liveInstance.getEphemeralOwner());
            } catch (Exception e) {
              throw new HelixException("Unexpected exception in the test method.", e);
            }
          }
        }
      }
    }
  }

  @Test
  public void testConcurrentInitCallbackHandlers() throws Exception {
    final String clusterName =
        CLUSTER_PREFIX + "_" + _className + "_" + TestHelper.getTestMethodName();
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    final String spectatorName = TestHelper.getTestMethodName() + "Spectator";
    try {
      BlockingHandleNewSessionZkHelixManager helixManager =
          new BlockingHandleNewSessionZkHelixManager(clusterName, spectatorName,
              InstanceType.SPECTATOR, _gZkClient.getServers());
      helixManager.connect();
      // Add two mock listeners that will add more callback handlers while handling INIT or CALLBACK event.
      // Note that we have to test with 2 separate listeners so one of them has a chance to fail if
      // there is a concurrent modification exception.
      helixManager.addLiveInstanceChangeListener(
          new MockLiveInstanceChangeListener(helixManager, Collections.singleton("localhost_1")));
      helixManager.addLiveInstanceChangeListener(
          new MockLiveInstanceChangeListener(helixManager, Collections.singleton("localhost_2")));

      // Session expire will trigger all callbacks to be init. And the injected liveInstance
      // listener will trigger more callbackhandlers to be registered during the init process.
      ZkTestHelper.asyncExpireSession(helixManager.getZkClient());
      // Create mock live instance znodes to trigger the internal callback handling logic which will
      // modify the handler list.
      setupLiveInstances(clusterName, new int[] { 1, 2 });
      // Start new session handling so the manager will call the initHandler() for initializing all
      // existing handlers.
      helixManager.proceedNewSessionHandling();
      // Ensure the new session has been processed.
      TestHelper.verify(() -> helixManager.getHandleNewSessionEndTime() != 0, 3000);
      // Verify that both new mock current state callback handlers have been initialized normally.
      // Note that if there is concurrent modification that cause errors, one of the callback will
      // not be initialized normally.
      for (CallbackHandler handler : helixManager.getHandlers()) {
        Assert.assertTrue(handler.isReady(),
            "CallbackHandler is not initialized as expected. It might be caused by a ConcurrentModificationException");
      }
    } finally {
      TestHelper.dropCluster(clusterName, _gZkClient);
    }
  }

  static class BlockingHandleNewSessionZkHelixManager extends ZKHelixManager {
    private final Semaphore newSessionHandlingCount = new Semaphore(1);
    private long handleNewSessionStartTime = 0L;
    private long handleNewSessionEndTime = 0L;

    public BlockingHandleNewSessionZkHelixManager(String clusterName, String instanceName,
        InstanceType instanceType, String zkAddress) {
      super(clusterName, instanceName, instanceType, zkAddress);
    }

    @Override
    public void handleNewSession(final String sessionId) throws Exception {
      newSessionHandlingCount.acquire();
      handleNewSessionStartTime = System.currentTimeMillis();
      super.handleNewSession(sessionId);
      handleNewSessionEndTime = System.currentTimeMillis();
    }

    void proceedNewSessionHandling() {
      handleNewSessionStartTime = 0L;
      handleNewSessionEndTime = 0L;
      newSessionHandlingCount.release();
    }

    List<CallbackHandler> getHandlers() {
      return _handlers;
    }

    RealmAwareZkClient getZkClient() {
      return _zkclient;
    }

    long getHandleNewSessionStartTime() {
      return handleNewSessionStartTime;
    }

    long getHandleNewSessionEndTime() {
      return handleNewSessionEndTime;
    }
  }

  /*
   * A ZkHelixManager that simulates long time cost in resetting handlers.
   */
  static class BlockingResetHandlersZkHelixManager extends ZKHelixManager {
    private final Semaphore resetHandlersSemaphore = new Semaphore(1);
    private long resetHandlersStartTime = 0L;

    public BlockingResetHandlersZkHelixManager(String clusterName, String instanceName,
        InstanceType instanceType, String zkAddress) {
      super(clusterName, instanceName, instanceType, zkAddress);
    }

    @Override
    void resetHandlers(boolean isShutdown) {
      resetHandlersStartTime = System.currentTimeMillis();
      try {
        if (!isShutdown) {
          // Safety net only: the test releases the semaphore explicitly. Must exceed the
          // helper thread's polling windows so a slow machine cannot break the premise.
          resetHandlersSemaphore.tryAcquire(2 * SESSION_WAIT_DURATION, TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException ignored) {
        // Ignore the exception.
      }
      super.resetHandlers(isShutdown);
    }

    void proceedResetHandlers() {
      resetHandlersSemaphore.release();
    }

    List<CallbackHandler> getHandlers() {
      return _handlers;
    }

    RealmAwareZkClient getZkClient() {
      return _zkclient;
    }

    long getResetHandlersStartTime() {
      return resetHandlersStartTime;
    }
  }
}
