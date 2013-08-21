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
import org.apache.helix.*;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.model.CurrentState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkCallbackHandlerLeak extends ZkUnitTestBase {

  @Test
  public void testCbHandlerLeakOnParticipantSessionExpiry() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
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

    ClusterController controller = new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);
    final ZkHelixTestManager controllerManager = controller.getManager();
    final ZkHelixTestManager participantManagerToExpire =
        (ZkHelixTestManager) participants[1].getManager();

    // check controller zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + controllerManager.getSessionId());
        // System.out.println("controller watch paths: " + watchPaths);

        // controller should have 5 + 2n + m + (m+2)n zk-watchers
        // where n is number of nodes and m is number of resources
        return watchPaths.size() == (6 + 5 * n);
      }
    }, 500);
    Assert.assertTrue(result, "Controller should have 6 + 5*n zk-watchers.");

    // check participant zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + participantManagerToExpire.getSessionId());
        // System.out.println("participant watch paths: " + watchPaths);

        // participant should have 2 zk-watchers: 1 for MESSAGE and 1 for CONTROLLER
        return watchPaths.size() == 2;
      }
    }, 500);
    Assert.assertTrue(result, "Participant should have 2 zk-watchers.");

    // check HelixManager#_handlers
    // printHandlers(controllerManager);
    // printHandlers(participantManagerToExpire);
    int controllerHandlerNb = controllerManager.getHandlers().size();
    int particHandlerNb = participantManagerToExpire.getHandlers().size();
    Assert.assertEquals(controllerHandlerNb, 9,
        "HelixController should have 9 (5+2n) callback handlers for 2 (n) participant");
    Assert.assertEquals(particHandlerNb, 2,
        "HelixParticipant should have 2 (msg+cur-state) callback handlers");

    // expire the session of participant
    System.out.println("Expiring participant session...");
    String oldSessionId = participantManagerToExpire.getSessionId();

    ZkTestHelper.expireSession(participantManagerToExpire.getZkClient());
    String newSessionId = participantManagerToExpire.getSessionId();
    System.out.println("Expried participant session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // check controller zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + controllerManager.getSessionId());
        // System.out.println("controller watch paths after session expiry: " + watchPaths);

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
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + participantManagerToExpire.getSessionId());
        // System.out.println("participant watch paths after session expiry: " + watchPaths);

        // participant should have 2 zk-watchers: 1 for MESSAGE and 1 for CONTROLLER
        return watchPaths.size() == 2;
      }
    }, 500);
    Assert.assertTrue(result, "Participant should have 2 zk-watchers after session expiry.");

    // check handlers
    // printHandlers(controllerManager);
    // printHandlers(participantManagerToExpire);
    int handlerNb = controllerManager.getHandlers().size();
    Assert.assertEquals(handlerNb, controllerHandlerNb,
        "controller callback handlers should not increase after participant session expiry");
    handlerNb = participantManagerToExpire.getHandlers().size();
    Assert.assertEquals(handlerNb, particHandlerNb,
        "participant callback handlers should not increase after participant session expiry");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCbHandlerLeakOnControllerSessionExpiry() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
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

    ClusterController controller = new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);
    final ZkHelixTestManager controllerManager = controller.getManager();
    final ZkHelixTestManager participantManager = participants[0].getManager();

    // wait until we get all the listeners registered
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        int controllerHandlerNb = controllerManager.getHandlers().size();
        int particHandlerNb = participantManager.getHandlers().size();
        if (controllerHandlerNb == 9 && particHandlerNb == 2)
          return true;
        else
          return false;
      }
    }, 1000);

    int controllerHandlerNb = controllerManager.getHandlers().size();
    int particHandlerNb = participantManager.getHandlers().size();
    Assert.assertEquals(controllerHandlerNb, 9,
        "HelixController should have 9 (5+2n) callback handlers for 2 participant, but was "
            + controllerHandlerNb + ", " + printHandlers(controllerManager));
    Assert.assertEquals(particHandlerNb, 2,
        "HelixParticipant should have 2 (msg+cur-state) callback handlers, but was "
            + particHandlerNb + ", " + printHandlers(participantManager));

    // expire controller
    System.out.println("Expiring controller session...");
    String oldSessionId = controllerManager.getSessionId();

    ZkTestHelper.expireSession(controllerManager.getZkClient());
    String newSessionId = controllerManager.getSessionId();
    System.out.println("Expired controller session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // check controller zk-watchers
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + controllerManager.getSessionId());
        // System.out.println("controller watch paths after session expiry: " + watchPaths);

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
        Map<String, Set<String>> watchers = ZkTestHelper.getListenersBySession(ZK_ADDR);
        Set<String> watchPaths = watchers.get("0x" + participantManager.getSessionId());
        // System.out.println("participant watch paths after session expiry: " + watchPaths);

        // participant should have 2 zk-watchers: 1 for MESSAGE and 1 for CONTROLLER
        return watchPaths.size() == 2;
      }
    }, 500);
    Assert.assertTrue(result, "Participant should have 2 zk-watchers after session expiry.");

    // check HelixManager#_handlers
    // printHandlers(controllerManager);
    int handlerNb = controllerManager.getHandlers().size();
    Assert.assertEquals(handlerNb, controllerHandlerNb,
        "controller callback handlers should not increase after participant session expiry, but was "
            + printHandlers(controllerManager));
    handlerNb = participantManager.getHandlers().size();
    Assert.assertEquals(handlerNb, particHandlerNb,
        "participant callback handlers should not increase after participant session expiry, but was "
            + printHandlers(participantManager));

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

    ClusterController controller = new ClusterController(clusterName, "controller_0", zkAddr);
    controller.syncStart();

    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipant(clusterName, instanceName, zkAddr, null);
      participants[i].syncStart();

      // register a controller listener on participant_0
      if (i == 0) {
        ZkHelixTestManager manager = participants[0].getManager();
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

    ZkHelixTestManager participantToExpire = participants[0].getManager();
    String oldSessionId = participantToExpire.getSessionId();
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);

    // check manager#hanlders
    Assert.assertEquals(participantToExpire.getHandlers().size(), 3,
        "Should have 3 handlers: CURRENTSTATE/{sessionId}, CONTROLLER, and MESSAGES");

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
            3,
            "Should have 3 paths (CURRENTSTATE/{sessionId}, CONTROLLER, and MESSAGES) each of which has 1 child-listener");
    path = keyBuilder.currentStates(participantToExpire.getInstanceName(), oldSessionId).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);
    path = keyBuilder.messages(participantToExpire.getInstanceName()).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);
    path = keyBuilder.controller().getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);

    // check zookeeper#watches on client side
    Map<String, List<String>> watchPaths =
        ZkTestHelper.getZkWatch(participantToExpire.getZkClient());
    // System.out.println("localhost_12918 zk-client side watchPaths: " + watchPaths + "\n");
    Assert
        .assertEquals(
            watchPaths.get("dataWatches").size(),
            4,
            "Should have 4 data-watches: CURRENTSTATE/{sessionId}, CURRENTSTATE/{sessionId}/TestDB, CONTROLLER, MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 3,
        "Should have 3 child-watches: CONTROLLER, MESSAGES, and CURRENTSTATE/{sessionId}");

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
            2,
            "Should have 2 handlers: CONTROLLER and MESSAGES. CURRENTSTATE/{sessionId} handler should be removed by CallbackHandler#handleChildChange()");

    // check zkclient#listeners
    dataListeners = ZkTestHelper.getZkDataListener(participantToExpire.getZkClient());
    childListeners = ZkTestHelper.getZkChildListener(participantToExpire.getZkClient());
    // printZkListeners(participantToExpire.getZkClient());
    Assert.assertTrue(dataListeners.isEmpty(), "Should have no data-listeners");
    Assert
        .assertEquals(
            childListeners.size(),
            3,
            "Should have 3 paths (CURRENTSTATE/{oldSessionId}, CONTROLLER, and MESSAGES). "
                + "CONTROLLER and MESSAGE has 1 child-listener each. CURRENTSTATE/{oldSessionId} doesn't have listener (ZkClient doesn't remove empty childListener set. probably a ZkClient bug. see ZkClient#unsubscribeChildChange())");
    path = keyBuilder.currentStates(participantToExpire.getInstanceName(), oldSessionId).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 0,
        "Should have no child-listener on path: " + path);
    path = keyBuilder.messages(participantToExpire.getInstanceName()).getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);
    path = keyBuilder.controller().getPath();
    Assert.assertEquals(childListeners.get(path).size(), 1,
        "Should have 1 child-listener on path: " + path);

    // check zookeeper#watches on client side
    watchPaths = ZkTestHelper.getZkWatch(participantToExpire.getZkClient());
    // System.out.println("localhost_12918 zk-client side watchPaths: " + watchPaths + "\n");
    Assert.assertEquals(watchPaths.get("dataWatches").size(), 2,
        "Should have 2 data-watches: CONTROLLER and MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 2,
        "Should have 2 child-watches: CONTROLLER and MESSAGES");
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
    // System.out.println("localhost_12918 zk-client side watchPaths: " + watchPaths + "\n");
    Assert.assertEquals(watchPaths.get("dataWatches").size(), 2,
        "Should have 2 data-watches: CONTROLLER and MESSAGES");
    Assert.assertEquals(watchPaths.get("childWatches").size(), 2,
        "Should have 2 child-watches: CONTROLLER and MESSAGES");
    Assert
        .assertEquals(
            watchPaths.get("existWatches").size(),
            0,
            "Should have no exist-watches. exist-watches on CURRENTSTATE/{oldSessionId} and CURRENTSTATE/{oldSessionId}/TestDB0 should be cleared during handleNewSession");

    // Thread.sleep(1000);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  // debug code
  static String printHandlers(ZkHelixTestManager manager) {
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

  void printZkListeners(ZkClient client) throws Exception {
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
