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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MultiTypeMessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkSessionExpiry extends ZkUnitTestBase {
  private final static String DUMMY_MSG_TYPE = "DUMMY";

  static class DummyMessageHandler extends MessageHandler {
    final Set<String> _handledMsgSet;

    DummyMessageHandler(Message message, NotificationContext context, Set<String> handledMsgSet) {
      super(message, context);
      _handledMsgSet = handledMsgSet;
    }

    @Override
    public HelixTaskResult handleMessage() {
      _handledMsgSet.add(_message.getId());
      HelixTaskResult ret = new HelixTaskResult();
      ret.setSuccess(true);
      return ret;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      // Do nothing
    }

  }

  static class DummyMessageHandlerFactory implements MultiTypeMessageHandlerFactory {
    final Set<String> _handledMsgSet;

    DummyMessageHandlerFactory(Set<String> handledMsgSet) {
      _handledMsgSet = handledMsgSet;
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new DummyMessageHandler(message, context, _handledMsgSet);
    }

    @Override
    public String getMessageType() {
      return DUMMY_MSG_TYPE;
    }

    @Override
    public List<String> getMessageTypes() {
      return ImmutableList.of(DUMMY_MSG_TYPE);
    }

    @Override
    public void reset() {
      // Do nothing
    }

  }

  @Test
  public void testMsgHdlrFtyReRegistration() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    Set<String> handledMsgSet = new HashSet<>();
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].getMessagingService().registerMessageHandlerFactory(DUMMY_MSG_TYPE,
          new DummyMessageHandlerFactory(handledMsgSet));
      participants[i].syncStart();
    }

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // trigger dummy message handler
    checkDummyMsgHandler(participants[0], handledMsgSet);

    // expire localhost_12918
    ZkTestHelper.expireSession(participants[0].getZkClient());
    result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // trigger dummy message handler again
    checkDummyMsgHandler(participants[0], handledMsgSet);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * trigger dummy message handler and verify it's invoked
   * @param manager
   * @param handledMsgSet
   * @throws Exception
   */
  private static void checkDummyMsgHandler(HelixManager manager, final Set<String> handledMsgSet)
      throws Exception {

    final Message aMsg = newMsg();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.message(manager.getInstanceName(), aMsg.getId()), aMsg);
    boolean result = TestHelper.verify(() -> handledMsgSet.contains(aMsg.getId()), 5 * 1000);
    Assert.assertTrue(result);
  }

  private static Message newMsg() {
    Message msg = new Message(DUMMY_MSG_TYPE, UUID.randomUUID().toString());
    msg.setTgtSessionId("*");
    msg.setTgtName("localhost_12918");
    return msg;
  }
}
