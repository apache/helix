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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.model.CurrentState;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkCallbackHandlerLeak extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestZkCallbackHandlerLeak.class);

  @Test
  public void testCbHdlrLeakOnParticipantSessionExpiry() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    final ClusterControllerManager controller =
        new ClusterControllerManager(_zkaddr, clusterName, "controller");
    controller.connect();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // check controller zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(_zkaddr);
        LOG.debug("all watchers: " + watchers);
        Set<String> watchPaths = watchers.get("0x" + controller.getSessionId());
        LOG.debug("controller watch paths: " + watchPaths);

        // controller should have 5 + 2n + m + (m+2)n zk-watchers
        // where n is number of nodes and m is number of resources
        return watchPaths.size() == (6 + 5 * n);
      }
    }, 500);
    Assert.assertTrue(result, "Controller should have 6 + 5*n zk-watchers.");

    // check participant zk-watchers
    final MockParticipantManager participantManagerToExpire = participants[0];
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(_zkaddr);
        Set<String> watchPaths = watchers.get("0x" + participantManagerToExpire.getSessionId());
        LOG.debug("participant watch paths: " + watchPaths);

        // participant should have 1 zk-watcher: 1 for MESSAGE
        return watchPaths.size() == 1;
      }
    }, 500);
    Assert.assertTrue(result, "Participant should have 1 zk-watcher.");

    // check HelixManager#_handlers
    // printHandlers(controllerManager);
    // printHandlers(participantManagerToExpire);
    int controllerHandlerNb = controller.getHandlers().size();
    int particHandlerNb = participantManagerToExpire.getHandlers().size();
    Assert.assertEquals(controllerHandlerNb, (5 + 2 * n),
        "HelixController should have 9 (5+2n) callback handlers for 2 (n) participant");
    Assert.assertEquals(particHandlerNb, 1,
        "HelixParticipant should have 1 (msg->HelixTaskExecutor) callback handlers");

    // expire the session of participant
    LOG.debug("Expiring participant session...");
    String oldSessionId = participantManagerToExpire.getSessionId();

    ZkTestHelper.expireSession(participantManagerToExpire.getZkClient());
    String newSessionId = participantManagerToExpire.getSessionId();
    LOG.debug("Expried participant session. oldSessionId: " + oldSessionId + ", newSessionId: "
        + newSessionId);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // check controller zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(_zkaddr);
        Set<String> watchPaths = watchers.get("0x" + controller.getSessionId());
        LOG.debug("controller watch paths after session expiry: " + watchPaths);

        // controller should have 5 + 2n + m + (m+2)n zk-watchers
        // where n is number of nodes and m is number of resources
        return watchPaths.size() == (6 + 5 * n);
      }
    }, 500);
    Assert.assertTrue(result, "Controller should have 6 + 5*n zk-watchers after session expiry.");

    // check participant zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(_zkaddr);
        Set<String> watchPaths = watchers.get("0x" + participantManagerToExpire.getSessionId());
        LOG.debug("participant watch paths after session expiry: " + watchPaths);

        // participant should have 1 zk-watcher: 1 for MESSAGE
        return watchPaths.size() == 1;
      }
    }, 500);
    Assert.assertTrue(result, "Participant should have 1 zk-watcher after session expiry.");

    // check handlers
    // printHandlers(controllerManager);
    // printHandlers(participantManagerToExpire);
    int handlerNb = controller.getHandlers().size();
    Assert.assertEquals(handlerNb, controllerHandlerNb,
        "controller callback handlers should not increase after participant session expiry");
    handlerNb = participantManagerToExpire.getHandlers().size();
    Assert.assertEquals(handlerNb, particHandlerNb,
        "participant callback handlers should not increase after participant session expiry");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCbHdlrLeakOnControllerSessionExpiry() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    final ClusterControllerManager controller =
        new ClusterControllerManager(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // wait until we get all the listeners registered
    final MockParticipantManager participantManager = participants[0];
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        int controllerHandlerNb = controller.getHandlers().size();
        int particHandlerNb = participantManager.getHandlers().size();
        if (controllerHandlerNb == 9 && particHandlerNb == 2)
          return true;
        else
          return false;
      }
    }, 1000);

    int controllerHandlerNb = controller.getHandlers().size();
    int particHandlerNb = participantManager.getHandlers().size();
    Assert.assertEquals(controllerHandlerNb, (5 + 2 * n),
        "HelixController should have 9 (5+2n) callback handlers for 2 participant, but was "
            + controllerHandlerNb + ", " + TestHelper.printHandlers(controller));
    Assert.assertEquals(particHandlerNb, 1,
        "HelixParticipant should have 1 (msg->HelixTaskExecutor) callback handler, but was "
            + particHandlerNb + ", " + TestHelper.printHandlers(participantManager));

    // expire controller
    LOG.debug("Expiring controller session...");
    String oldSessionId = controller.getSessionId();

    ZkTestHelper.expireSession(controller.getZkClient());
    String newSessionId = controller.getSessionId();
    LOG.debug("Expired controller session. oldSessionId: " + oldSessionId + ", newSessionId: "
        + newSessionId);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // check controller zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(_zkaddr);
        Set<String> watchPaths = watchers.get("0x" + controller.getSessionId());
        // System.out.println("controller watch paths after session expiry: " +
        // watchPaths);

        // controller should have 5 + 2n + m + (m+2)n zk-watchers
        // where n is number of nodes and m is number of resources
        return watchPaths.size() == (6 + 5 * n);
      }
    }, 500);
    Assert.assertTrue(result, "Controller should have 6 + 5*n zk-watchers after session expiry.");

    // check participant zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(_zkaddr);
        Set<String> watchPaths = watchers.get("0x" + participantManager.getSessionId());
        LOG.debug("participant watch paths after session expiry: " + watchPaths);

        // participant should have 1 zk-watcher: 1 for MESSAGE
        return watchPaths.size() == 1;
      }
    }, 500);
    Assert.assertTrue(result, "Participant should have 1 zk-watcher after session expiry.");

    // check HelixManager#_handlers
    // printHandlers(controllerManager);
    int handlerNb = controller.getHandlers().size();
    Assert.assertEquals(handlerNb, controllerHandlerNb,
        "controller callback handlers should not increase after participant session expiry, but was "
            + TestHelper.printHandlers(controller));
    handlerNb = participantManager.getHandlers().size();
    Assert.assertEquals(handlerNb, particHandlerNb,
        "participant callback handlers should not increase after participant session expiry, but was "
            + TestHelper.printHandlers(participantManager));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testRemoveUserCbHdlrOnPathRemoval() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 3;
    final String zkAddr = _zkaddr;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, zkAddr, 12918, "localhost", "TestDB", 1, // resource
        32, // partitions
        n, // nodes
        2, // replicas
        "MasterSlave", true);

    final ClusterControllerManager controller =
        new ClusterControllerManager(zkAddr, clusterName, "controller");
    controller.syncStart();

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(zkAddr, clusterName, instanceName);
      participants[i].syncStart();

      // register a controller listener on participant_0
      if (i == 0) {
        MockParticipantManager manager = participants[0];
        manager.addCurrentStateChangeListener(new CurrentStateChangeListener() {
          @Override
          public void onStateChange(String instanceName, List<CurrentState> statesInfo,
              NotificationContext changeContext) {
            // To change body of implemented methods use File | Settings | File Templates.
            // System.out.println(instanceName + " on current-state change, type: " +
            // changeContext.getType());
          }
        }, manager.getInstanceName(), manager.getSessionId());
      }
    }

    Boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(zkAddr,
                clusterName));
    Assert.assertTrue(result);

    MockParticipantManager participantToExpire = participants[0];
    String oldSessionId = participantToExpire.getSessionId();
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);

    // check manager#hanlders
    Assert.assertEquals(participantToExpire.getHandlers().size(), 2,
        "Should have 2 handlers: CURRENTSTATE/{sessionId}, and MESSAGES");

    // check zkclient#listeners
    Map<String, Set<IZkDataListener>> dataListeners =
        ZkTestHelper.getZkDataListener(participantToExpire.getZkClient());
    Map<String, Set<IZkChildListener>> childListeners =
        ZkTestHelper.getZkChildListener(participantToExpire.getZkClient());
    // printZkListeners(participantToExpire.getZkClient());
    Assert.assertEquals(dataListeners.size(), 1,
        "Should have 1 path (CURRENTSTATE/{sessionId}/TestDB0) which has 1 data-listeners");
    String path =
        keyBuilder.currentState(participantToExpire.getInstanceName(), oldSessionId, "TestDB0")
            .getPath();
    Assert.assertEquals(dataListeners.get(path).size(), 1, "Should have 1 data-listeners on path: "
        + path);
    Assert
        .assertEquals(
            childListeners.size(),
            2,
            "Should have 2 paths (CURRENTSTATE/{sessionId}, and MESSAGES) each of which has 1 child-listener");
    path = keyBuilder.currentStates(participantToExpire.getInstanceName(), oldSessionId).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);
    path = keyBuilder.messages(participantToExpire.getInstanceName()).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);
    path = keyBuilder.controller().getPath();
    Assert.assertNull(childListeners.get(path),
        "Should have no child-listener on path: " + path);

    // check zookeeper#watches on client side
    Map<String, List<String>> watchPaths =
        ZkTestHelper.getZkWatch(participantToExpire.getZkClient());
    LOG.debug("localhost_12918 zk-client side watchPaths: " + watchPaths);
    Assert
        .assertEquals(
            watchPaths.get("dataWatches").size(),
            3,
            "Should have 3 data-watches: CURRENTSTATE/{sessionId}, CURRENTSTATE/{sessionId}/TestDB, MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 2,
        "Should have 2 child-watches: MESSAGES, and CURRENTSTATE/{sessionId}");

    // expire localhost_12918
    System.out.println("Expire participant: " + participantToExpire.getInstanceName()
        + ", session: " + participantToExpire.getSessionId());
    ZkTestHelper.expireSession(participantToExpire.getZkClient());
    String newSessionId = participantToExpire.getSessionId();
    System.out.println(participantToExpire.getInstanceName() + " oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);
    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(zkAddr,
                clusterName));
    Assert.assertTrue(result);

    // check manager#hanlders
    Assert
        .assertEquals(
            participantToExpire.getHandlers().size(),
            1,
            "Should have 1 handler: MESSAGES. CURRENTSTATE/{sessionId} handler should be removed by CallbackHandler#handleChildChange()");

    // check zkclient#listeners
    dataListeners = ZkTestHelper.getZkDataListener(participantToExpire.getZkClient());
    childListeners = ZkTestHelper.getZkChildListener(participantToExpire.getZkClient());
    // printZkListeners(participantToExpire.getZkClient());
    Assert.assertTrue(dataListeners.isEmpty(), "Should have no data-listeners");
    Assert
        .assertEquals(
            childListeners.size(),
            2,
            "Should have 2 paths (CURRENTSTATE/{oldSessionId}, and MESSAGES). "
                + "MESSAGE has 1 child-listener each. CURRENTSTATE/{oldSessionId} doesn't have listener (ZkClient doesn't remove empty childListener set. probably a ZkClient bug. see ZkClient#unsubscribeChildChange())");
    path = keyBuilder.currentStates(participantToExpire.getInstanceName(), oldSessionId).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 0,
        "Should have no child-listener on path: " + path);
    path = keyBuilder.messages(participantToExpire.getInstanceName()).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);
    path = keyBuilder.controller().getPath();
    Assert.assertNull(childListeners.get(path),
        "Should have no child-listener on path: " + path);

    // check zookeeper#watches on client side
    watchPaths = ZkTestHelper.getZkWatch(participantToExpire.getZkClient());
    LOG.debug("localhost_12918 zk-client side watchPaths: " + watchPaths);
    Assert.assertEquals(watchPaths.get("dataWatches").size(), 1,
        "Should have 1 data-watch: MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 1,
        "Should have 1 child-watch: MESSAGES");
    Assert
        .assertEquals(watchPaths.get("existWatches").size(), 2,
            "Should have 2 exist-watches: CURRENTSTATE/{oldSessionId} and CURRENTSTATE/{oldSessionId}/TestDB0");

    // another session expiry on localhost_12918 should clear the two exist-watches on
    // CURRENTSTATE/{oldSessionId}
    System.out.println("Expire participant: " + participantToExpire.getInstanceName()
        + ", session: " + participantToExpire.getSessionId());
    ZkTestHelper.expireSession(participantToExpire.getZkClient());
    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(zkAddr,
                clusterName));
    Assert.assertTrue(result);

    // check zookeeper#watches on client side
    watchPaths = ZkTestHelper.getZkWatch(participantToExpire.getZkClient());
    LOG.debug("localhost_12918 zk-client side watchPaths: " + watchPaths);
    Assert.assertEquals(watchPaths.get("dataWatches").size(), 1,
        "Should have 1 data-watch: MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 1,
        "Should have 1 child-watch: MESSAGES");
    Assert
        .assertEquals(
            watchPaths.get("existWatches").size(),
            0,
            "Should have no exist-watches. exist-watches on CURRENTSTATE/{oldSessionId} and CURRENTSTATE/{oldSessionId}/TestDB0 should be cleared during handleNewSession");

    // Thread.sleep(1000);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
