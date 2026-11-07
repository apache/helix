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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConsecutiveZkSessionExpiry extends ZkUnitTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestConsecutiveZkSessionExpiry.class);

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
    public void onPreConnect() {
      // TODO Auto-generated method stub
      LOG.info("handleNewSession for instance: " + instanceName + ", count: " + count);
      if (count++ == 1) {
        startCountDown.countDown();
        LOG.info("wait session expiry to happen");
        try {
          endCountDown.await();
        } catch (Exception e) {
          LOG.error("interrupted in waiting", e);
        }
      }
    }

  }

  @Test
  public void testParticipant() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    final ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start participants
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);

      if (i == 0) {
        participants[i].addPreConnectCallback(new PreConnectTestCallback(instanceName,
            startCountdown, endCountdown));
      }
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(
                new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName),
                TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    // expire the session of participant
    LOG.info("1st Expiring participant session...");
    String oldSessionId = participants[0].getSessionId();

    ZkTestHelper.asyncExpireSession(participants[0].getZkClient());
    String newSessionId = participants[0].getSessionId();
    LOG.info("Expried participant session. oldSessionId: " + oldSessionId + ", newSessionId: "
        + newSessionId);

    // expire zk session again during HelixManager#handleNewSession()
    startCountdown.await();
    LOG.info("2nd Expiring participant session...");
    oldSessionId = participants[0].getSessionId();

    ZkTestHelper.asyncExpireSession(participants[0].getZkClient());
    newSessionId = participants[0].getSessionId();
    LOG.info("Expried participant session. oldSessionId: " + oldSessionId + ", newSessionId: "
        + newSessionId);

    endCountdown.countDown();

    // Wait for the participant to fully re-join the cluster under its new session before
    // verifying cluster state. Recovering from consecutive session expiries (re-joining the
    // cluster and carrying over the current states) can take a while on overloaded machines.
    waitForRejoinWithNewSession(participants[0], clusterName, oldSessionId);

    verifyClusterConverged(clusterName);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDistributedController() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterDistributedController[] distributedControllers = new ClusterDistributedController[n];
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    for (int i = 0; i < n; i++) {
      String contrllerName = "localhost_" + (12918 + i);
      distributedControllers[i] =
          new ClusterDistributedController(ZK_ADDR, clusterName, contrllerName);
      distributedControllers[i].getStateMachineEngine().registerStateModelFactory("MasterSlave",
          new MockMSModelFactory());
      if (i == 0) {
        distributedControllers[i].addPreConnectCallback(new PreConnectTestCallback(contrllerName,
            startCountdown, endCountdown));
      }
      distributedControllers[i].connect();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName), TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    // expire the session of distributedController
    LOG.info("1st Expiring distributedController session...");
    String oldSessionId = distributedControllers[0].getSessionId();

    ZkTestHelper.asyncExpireSession(distributedControllers[0].getZkClient());
    String newSessionId = distributedControllers[0].getSessionId();
    LOG.info("Expried distributedController session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    // expire zk session again during HelixManager#handleNewSession()
    // startCountdown is counted down by PreConnectTestCallback once the first reconnect
    // reaches handleNewSession, so awaiting it guarantees the first expiry has taken effect
    startCountdown.await();
    LOG.info("2nd Expiring distributedController session...");
    oldSessionId = distributedControllers[0].getSessionId();

    ZkTestHelper.asyncExpireSession(distributedControllers[0].getZkClient());
    newSessionId = distributedControllers[0].getSessionId();
    LOG.info("Expried distributedController session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    endCountdown.countDown();

    // Wait for the distributed controller to fully re-join the cluster under its new session
    // before verifying cluster state. Recovering from consecutive session expiries can be slow
    // on overloaded machines.
    waitForRejoinWithNewSession(distributedControllers[0], clusterName, oldSessionId);

    verifyClusterConverged(clusterName);

    // verify leader changes to localhost_12919
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNotNull(pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12918"), true));
    LiveInstance leader =
        pollForProperty(LiveInstance.class, accessor, keyBuilder.controllerLeader(), true);
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getId(), "localhost_12919");

    // Wait for handler count to stabilize after losing leadership
    // This is needed because handler cleanup is async and may take time in CI
    List<CallbackHandler> handlers = null;
    int expectedHandlerCount = 1;
    long maxWaitTime = TestHelper.WAIT_DURATION;
    int pollInterval = 500;
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < maxWaitTime) {
      handlers = distributedControllers[0].getHandlers();
      if (handlers.size() == expectedHandlerCount) {
        LOG.info("Handler count stabilized at {} after {} ms",
            handlers.size(), System.currentTimeMillis() - startTime);
        break;
      }
      Thread.sleep(pollInterval);
    }
    LOG.debug("handlers: " + TestHelper.printHandlers(distributedControllers[0]));
    Assert.assertEquals(handlers.size(), expectedHandlerCount,
        "Distributed controller should have 1 handler (message) after lose leadership, but was "
            + handlers.size());

    // clean up
    distributedControllers[0].disconnect();
    distributedControllers[1].disconnect();
    Assert.assertNull(pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12918"), false));
    Assert.assertNull(pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12919"), false));
    Assert.assertNull(pollForProperty(LiveInstance.class, accessor, keyBuilder.controllerLeader(),
        false));

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Waits until the manager has re-joined the cluster under a session other than the expired one:
   * the ZK client is connected with a new session id AND the LIVEINSTANCE znode has been
   * re-created by that session. The second condition is what matters for the cluster state
   * verification that follows: it means HelixManager#handleNewSession() for the surviving session
   * got past the live instance creation (the point where the handler for the intermediate,
   * expired-while-being-handled session fails by design in this test). Only checking the client's
   * session id would let the test proceed while the manager is still (re)initializing.
   */
  private void waitForRejoinWithNewSession(final HelixManager manager, String clusterName,
      final String expiredSessionId) throws Exception {
    final HelixDataAccessor accessor = accessor(clusterName);
    final PropertyKey liveInstanceKey =
        accessor.keyBuilder().liveInstance(manager.getInstanceName());
    boolean rejoined = TestHelper.verify(() -> {
      // getSessionId() throws when the manager is (or becomes) disconnected; treat that as
      // "not re-joined yet" and keep polling instead of aborting the wait.
      String currentSessionId = currentSessionIdOrNull(manager);
      if (currentSessionId == null || currentSessionId.equals(expiredSessionId)) {
        return false;
      }
      LiveInstance liveInstance = accessor.getProperty(liveInstanceKey);
      return liveInstance != null && currentSessionId.equals(liveInstance.getEphemeralOwner());
    }, TestHelper.WAIT_DURATION);
    if (!rejoined) {
      dumpClusterState(clusterName, "instance " + manager.getInstanceName()
          + " did not re-join with a new session (expired session: " + expiredSessionId
          + ", current session: " + currentSessionIdOrNull(manager) + ", connected: "
          + manager.isConnected() + ")");
    }
    Assert.assertTrue(rejoined, manager.getInstanceName()
        + " should re-create its live instance with a new session after consecutive expiries");
  }

  /**
   * Verifies that the external views converge to the best possible state. On failure, dumps the
   * relevant cluster state to stdout (the CI log level is ERROR only, so test logging is not
   * available there) before failing, so that a non-converging cluster can be diagnosed from the
   * CI output instead of just "expected:<true> but was:<false>".
   */
  private void verifyClusterConverged(String clusterName) {
    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName), 2 * TestHelper.WAIT_DURATION);
    if (!result) {
      dumpClusterState(clusterName, "cluster did not converge after consecutive session expiries");
    }
    Assert.assertTrue(result,
        "Cluster should converge to the best possible state after consecutive session expiries");
  }

  private HelixDataAccessor accessor(String clusterName) {
    return new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
  }

  private static String currentSessionIdOrNull(HelixManager manager) {
    try {
      return manager.isConnected() ? manager.getSessionId() : null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Best effort: every read below tolerates znodes disappearing underneath it (which is exactly
   * what happens around a session expiry) so that one failed section does not hide the others.
   */
  private void dumpClusterState(String clusterName, String reason) {
    StringBuilder sb = new StringBuilder();
    sb.append("==== ").append(clusterName).append(": ").append(reason).append(" ====\n");
    try {
      HelixDataAccessor accessor = accessor(clusterName);
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();

      LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
      sb.append("LEADER: ").append(leader == null ? "none"
          : leader.getInstanceName() + " (session " + leader.getEphemeralOwner() + ", "
              + describeStat(leader) + ")").append('\n');

      Map<String, LiveInstance> liveInstances =
          accessor.getChildValuesMap(keyBuilder.liveInstances(), false);
      for (Map.Entry<String, LiveInstance> entry : liveInstances.entrySet()) {
        sb.append("LIVEINSTANCE ").append(entry.getKey()).append(": session ")
            .append(entry.getValue().getEphemeralOwner()).append(", ")
            .append(describeStat(entry.getValue())).append('\n');
      }

      for (String instance : accessor.getChildNames(keyBuilder.instances())) {
        for (String session : accessor.getChildNames(keyBuilder.sessions(instance))) {
          List<CurrentState> currentStates =
              accessor.getChildValues(keyBuilder.currentStates(instance, session), false);
          for (CurrentState currentState : currentStates) {
            sb.append("CURRENTSTATE ").append(instance).append('/').append(session).append('/')
                .append(currentState.getResourceName()).append(" (session field ")
                .append(currentState.getSessionId()).append(", ").append(describeStat(currentState))
                .append("): ").append(currentState.getPartitionStateMap()).append('\n');
          }
        }
        List<Message> messages = accessor.getChildValues(keyBuilder.messages(instance), false);
        for (Message message : messages) {
          sb.append("MESSAGE ").append(instance).append('/').append(message.getMsgId())
              .append(": ").append(message.getMsgType()).append(' ')
              .append(message.getResourceName()).append('/').append(message.getPartitionName())
              .append(' ').append(message.getFromState()).append("->").append(message.getToState())
              .append(" tgtSession=").append(message.getTgtSessionId()).append(" state=")
              .append(message.getMsgState()).append('\n');
        }
      }

      List<ExternalView> externalViews =
          accessor.getChildValues(keyBuilder.externalViews(), false);
      for (ExternalView externalView : externalViews) {
        sb.append("EXTERNALVIEW ").append(externalView.getResourceName()).append(" (")
            .append(describeStat(externalView)).append("): ")
            .append(externalView.getRecord().getMapFields()).append('\n');
      }
    } catch (Exception e) {
      sb.append("failed to dump cluster state: ").append(e).append('\n');
    }
    System.out.println(sb);
  }

  /**
   * Version and modification time of a znode, so that the order in which the dumped znodes were
   * last written can be reconstructed (e.g. whether the external view was written before or after
   * the last current state change).
   */
  private static String describeStat(HelixProperty property) {
    HelixProperty.Stat stat = property.getStat();
    return "version " + stat.getVersion() + ", modified " + new Date(stat.getModifiedTime());
  }
}
