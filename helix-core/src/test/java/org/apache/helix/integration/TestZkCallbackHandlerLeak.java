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
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.manager.ZkTestManager;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.CurrentState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkCallbackHandlerLeak extends ZkUnitTestBase {

  private static Logger LOG = LoggerFactory.getLogger(TestZkCallbackHandlerLeak.class);

  @Test
  public void testCbHandlerLeakOnParticipantSessionExpiry() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;
    final int r = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        r, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    final ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ZkHelixClusterVerifier verifier = new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());
    final MockParticipantManager participantManagerToExpire = participants[1];

    // check controller zk-watchers
    boolean result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        // Set<String> watchPaths = watchers.get("0x" + controllerManager.getSessionId());
        Set<String> watchPaths = watchers.get("0x" + controller.getSessionId());
        // System.out.println("controller watch paths: " + watchPaths);

        // where n is number of nodes and r is number of resources
        return watchPaths.size() == (7 + r + ( 4 + r) * n);
      }
    }, 2000);
    Assert.assertTrue(result, "Controller has incorrect number of zk-watchers.");

    // check participant zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + participantManagerToExpire.getSessionId());
        // System.out.println("participant watch paths: " + watchPaths);

        // participant should have 1 zk-watcher: 1 for MESSAGE
        return watchPaths.size() == 1;
      }
    }, 2000);
    Assert.assertTrue(result, "Participant should have 1 zk-watcher. MESSAGES->HelixTaskExecutor");

    // check HelixManager#_handlers
    // printHandlers(controllerManager);
    // printHandlers(participantManagerToExpire);
    int controllerHandlerNb = controller.getHandlers().size();
    int particHandlerNb = participantManagerToExpire.getHandlers().size();
    Assert.assertEquals(controllerHandlerNb, 11,
        "HelixController should have 10 (5+2n) callback handlers for 2 (n) participant");
    Assert.assertEquals(particHandlerNb, 1,
        "HelixParticipant should have 1 (msg->HelixTaskExecutor) callback handlers");

    // expire the session of participant
    System.out.println("Expiring participant session...");
    String oldSessionId = participantManagerToExpire.getSessionId();

    ZkTestHelper.expireSession(participantManagerToExpire.getZkClient());
    String newSessionId = participantManagerToExpire.getSessionId();
    System.out.println(
        "Expried participant session. oldSessionId: " + oldSessionId + ", newSessionId: "
            + newSessionId);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // check controller zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + controller.getSessionId());
        // System.out.println("controller watch paths after session expiry: " + watchPaths);

        // where n is number of nodes and r is number of resources
        return watchPaths.size() == (7 + r + ( 4 + r) * n);
      }
    }, 2000);
    Assert.assertTrue(result, "Controller has incorrect number of zk-watchers after session expiry.");

    // check participant zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + participantManagerToExpire.getSessionId());
        // System.out.println("participant watch paths after session expiry: " + watchPaths);

        // participant should have 1 zk-watcher: 1 for MESSAGE
        return watchPaths.size() == 1;
      }
    }, 2000);
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

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCbHandlerLeakOnControllerSessionExpiry() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;
    final int r = 1;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        r, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    final ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());
    final MockParticipantManager participantManager = participants[0];

    // wait until we get all the listeners registered
    TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        int controllerHandlerNb = controller.getHandlers().size();
        int particHandlerNb = participantManager.getHandlers().size();
        if (controllerHandlerNb == 10 && particHandlerNb == 2)
          return true;
        else
          return false;
      }
    }, 1000);

    int controllerHandlerNb = controller.getHandlers().size();
    int particHandlerNb = participantManager.getHandlers().size();
    Assert.assertEquals(controllerHandlerNb, 7 + 2 * n,
        "HelixController should have 10 (6+2n) callback handlers for 2 participant, but was "
            + controllerHandlerNb + ", " + printHandlers(controller));
    Assert.assertEquals(particHandlerNb, 1,
        "HelixParticipant should have 1 (msg->HelixTaskExecutor) callback handler, but was "
            + particHandlerNb + ", " + printHandlers(participantManager));

    // expire controller
    System.out.println("Expiring controller session...");
    String oldSessionId = controller.getSessionId();

    ZkTestHelper.expireSession(controller.getZkClient());
    String newSessionId = controller.getSessionId();
    System.out.println(
        "Expired controller session. oldSessionId: " + oldSessionId + ", newSessionId: "
            + newSessionId);

    Assert.assertTrue(verifier.verifyByPolling());

    // check controller zk-watchers
    boolean result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + controller.getSessionId());
        System.err.println("controller watch paths after session expiry: " + watchPaths.size());

        // where r is number of resources and n is number of nodes
        int expected = (7 + r + (4 + r) * n);
        return watchPaths.size() == expected;
      }
    }, 2000);
    Assert.assertTrue(result, "Controller has incorrect zk-watchers after session expiry.");

    // check participant zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + participantManager.getSessionId());
        // System.out.println("participant watch paths after session expiry: " + watchPaths);

        // participant should have 1 zk-watcher: 1 for MESSAGE
        return watchPaths.size() == 1;
      }
    }, 2000);
    Assert.assertTrue(result, "Participant should have 1 zk-watcher after session expiry.");

    // check HelixManager#_handlers
    // printHandlers(controllerManager);
    int handlerNb = controller.getHandlers().size();
    Assert.assertEquals(handlerNb, controllerHandlerNb,
        "controller callback handlers should not increase after participant session expiry, but was "
            + printHandlers(controller));
    handlerNb = participantManager.getHandlers().size();
    Assert.assertEquals(handlerNb, particHandlerNb,
        "participant callback handlers should not increase after participant session expiry, but was "
            + printHandlers(participantManager));

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testRemoveUserCbHandlerOnPathRemoval() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 3;
    final String zkAddr = ZK_ADDR;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, zkAddr, 12918, "localhost", "TestDB", 1, // resource
        32, // partitions
        n, // nodes
        2, // replicas
        "MasterSlave", true);

    final ClusterControllerManager controller =
        new ClusterControllerManager(zkAddr, clusterName, "controller_0");
    controller.syncStart();

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(zkAddr, clusterName, instanceName);
      participants[i].syncStart();

      // register a controller listener on participant_0
      if (i == 0) {
        // ZkHelixTestManager manager = participants[0].getManager();
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
        .assertEquals(childListeners.size(), 2,
            "Should have 2 paths (CURRENTSTATE/{sessionId}, and MESSAGES) each of which has 1 child-listener");
    path = keyBuilder.currentStates(participantToExpire.getInstanceName(), oldSessionId).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);
    path = keyBuilder.messages(participantToExpire.getInstanceName()).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);
    path = keyBuilder.controller().getPath();
    Assert.assertNull(childListeners.get(path), "Should have no child-listener on path: " + path);

    // check zookeeper#watches on client side
    Map<String, List<String>> watchPaths =
        ZkTestHelper.getZkWatch(participantToExpire.getZkClient());
    // System.out.println("localhost_12918 zk-client side watchPaths: " + watchPaths + "\n");
    Assert
        .assertEquals(watchPaths.get("dataWatches").size(), 3,
            "Should have 3 data-watches: CURRENTSTATE/{sessionId}, CURRENTSTATE/{sessionId}/TestDB, MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 2,
        "Should have 2 child-watches: MESSAGES, and CURRENTSTATE/{sessionId}");

    // expire localhost_12918
    System.out.println(
        "Expire participant: " + participantToExpire.getInstanceName() + ", session: "
            + participantToExpire.getSessionId());
    ZkTestHelper.expireSession(participantToExpire.getZkClient());
    String newSessionId = participantToExpire.getSessionId();
    System.out.println(participantToExpire.getInstanceName() + " oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);
    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // check manager#hanlders
    Assert
        .assertEquals(
            participantToExpire.getHandlers().size(),
            1,
            "Should have 1 handlers: MESSAGES. CURRENTSTATE/{sessionId} handler should be removed by CallbackHandler#handleChildChange()");

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
                + "CONTROLLER and MESSAGE has 1 child-listener each. CURRENTSTATE/{oldSessionId} doesn't have listener (ZkClient doesn't remove empty childListener set. probably a ZkClient bug. see ZkClient#unsubscribeChildChange())");
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
    // System.out.println("localhost_12918 zk-client side watchPaths: " + watchPaths + "\n");
    Assert.assertEquals(watchPaths.get("dataWatches").size(), 1,
        "Should have 1 data-watches: MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 1,
        "Should have 1 child-watches: MESSAGES");
    Assert
        .assertEquals(watchPaths.get("existWatches").size(), 2,
            "Should have 2 exist-watches: CURRENTSTATE/{oldSessionId} and CURRENTSTATE/{oldSessionId}/TestDB0");

    // another session expiry on localhost_12918 should clear the two exist-watches on
    // CURRENTSTATE/{oldSessionId}
    System.out.println(
        "Expire participant: " + participantToExpire.getInstanceName() + ", session: "
            + participantToExpire.getSessionId());
    ZkTestHelper.expireSession(participantToExpire.getZkClient());

    Assert.assertTrue(verifier.verifyByPolling());

    // check zookeeper#watches on client side
    watchPaths = ZkTestHelper.getZkWatch(participantToExpire.getZkClient());
    // System.out.println("localhost_12918 zk-client side watchPaths: " + watchPaths + "\n");
    Assert.assertEquals(watchPaths.get("dataWatches").size(), 1,
        "Should have 1 data-watches: MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 1,
        "Should have 1 child-watches: MESSAGES");
    Assert
        .assertEquals(
            watchPaths.get("existWatches").size(),
            0,
            "Should have no exist-watches. exist-watches on CURRENTSTATE/{oldSessionId} and CURRENTSTATE/{oldSessionId}/TestDB0 should be cleared during handleNewSession");

    // Thread.sleep(1000);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  // debug code
  static String printHandlers(ZkTestManager manager) {
    StringBuilder sb = new StringBuilder();
    List<CallbackHandler> handlers = manager.getHandlers();
    sb.append(manager.getInstanceName() + " has " + handlers.size() + " cb-handlers. [");

    for (int i = 0; i < handlers.size(); i++) {
      CallbackHandler handler = handlers.get(i);
      String path = handler.getPath();
      sb.append(path.substring(manager.getClusterName().length() + 1) + ": "
          + handler.getListener());
      if (i < (handlers.size() - 1)) {
        sb.append(", ");
      }
    }
    sb.append("]");

    return sb.toString();
  }

  void printZkListeners(HelixZkClient client) throws Exception {
    Map<String, Set<IZkDataListener>> datalisteners = ZkTestHelper.getZkDataListener(client);
    Map<String, Set<IZkChildListener>> childListeners = ZkTestHelper.getZkChildListener(client);

    System.out.println("dataListeners {");
    for (String path : datalisteners.keySet()) {
      System.out.println("\t" + path + ": ");
      Set<IZkDataListener> set = datalisteners.get(path);
      for (IZkDataListener listener : set) {
        CallbackHandler handler = (CallbackHandler) listener;
        System.out.println("\t\t" + handler.getListener());
      }
    }
    System.out.println("}");

    System.out.println("childListeners {");
    for (String path : childListeners.keySet()) {
      System.out.println("\t" + path + ": ");
      Set<IZkChildListener> set = childListeners.get(path);
      for (IZkChildListener listener : set) {
        CallbackHandler handler = (CallbackHandler) listener;
        System.out.println("\t\t" + handler.getListener());
      }
    }
    System.out.println("}");
  }
}
