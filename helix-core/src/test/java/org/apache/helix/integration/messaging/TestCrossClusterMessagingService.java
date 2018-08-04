package org.apache.helix.integration.messaging;

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

import java.util.UUID;

import org.apache.helix.Criteria;
import org.apache.helix.Criteria.DataSource;
import org.apache.helix.InstanceType;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCrossClusterMessagingService extends TestMessagingService {
  private final String ADMIN_CLUSTER_NAME = "ADMIN_" + CLUSTER_NAME;
  private ClusterControllerManager _adminController;
  private String _hostSrc;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    // setup the admin cluster for sending cross cluster messages
    _gSetupTool.addCluster(ADMIN_CLUSTER_NAME, true);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_1";
    _hostSrc = controllerName;
    _adminController = new ClusterControllerManager(ZK_ADDR, ADMIN_CLUSTER_NAME, controllerName);
    _adminController.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(ADMIN_CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_adminController != null && _adminController.isConnected()) {
      _adminController.syncStop();
    }

    deleteCluster(ADMIN_CLUSTER_NAME);
    super.afterClass();
  }

  @Test()
  public void TestMessageSimpleSend() throws Exception {
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _participants[1].getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageTypes(), factory);

    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(factory.getMessageTypes().get(0), msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(_hostSrc);
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setClusterName(CLUSTER_NAME);

    int nMsgs = _adminController.getMessagingService().send(cr, msg);
    AssertJUnit.assertTrue(nMsgs == 1);
    Thread.sleep(2500);
    AssertJUnit.assertTrue(TestMessagingHandlerFactory._processedMsgIds.contains(para));

    cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setDataSource(DataSource.IDEALSTATES);
    cr.setClusterName(CLUSTER_NAME);

    // nMsgs = _startCMResultMap.get(hostSrc)._manager.getMessagingService().send(cr, msg);
    nMsgs = _adminController.getMessagingService().send(cr, msg);
    AssertJUnit.assertTrue(nMsgs == 1);
    Thread.sleep(2500);
    AssertJUnit.assertTrue(TestMessagingHandlerFactory._processedMsgIds.contains(para));
  }

  @Test()
  public void TestMessageSimpleSendReceiveAsync() throws Exception {
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _participants[1].getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageTypes(), factory);
    _participants[0].getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageTypes(), factory);

    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(factory.getMessageTypes().get(0), msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(_hostSrc);
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setClusterName(CLUSTER_NAME);

    TestAsyncCallback callback = new TestAsyncCallback(60000);

    _adminController.getMessagingService().send(cr, msg, callback, 60000);

    Thread.sleep(2000);
    AssertJUnit.assertTrue(TestAsyncCallback._replyedMessageContents.contains("TestReplyMessage"));
    AssertJUnit.assertTrue(callback.getMessageReplied().size() == 1);

    TestAsyncCallback callback2 = new TestAsyncCallback(500);
    _adminController.getMessagingService().send(cr, msg, callback2, 500);

    Thread.sleep(3000);
    AssertJUnit.assertTrue(callback2.isTimedOut());

    cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setDataSource(DataSource.IDEALSTATES);
    cr.setClusterName(CLUSTER_NAME);

    callback = new TestAsyncCallback(60000);

    _adminController.getMessagingService().send(cr, msg, callback, 60000);

    Thread.sleep(2000);
    AssertJUnit.assertTrue(TestAsyncCallback._replyedMessageContents.contains("TestReplyMessage"));
    AssertJUnit.assertTrue(callback.getMessageReplied().size() == 1);

    callback2 = new TestAsyncCallback(500);
    _adminController.getMessagingService().send(cr, msg, callback2, 500);

    Thread.sleep(3000);
    AssertJUnit.assertTrue(callback2.isTimedOut());
  }

  @Test()
  public void TestBlockingSendReceive() {
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _participants[1].getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageTypes(), factory);

    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(factory.getMessageTypes().get(0), msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(_hostSrc);
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setClusterName(CLUSTER_NAME);

    AsyncCallback asyncCallback = new MockAsyncCallback();
    int messagesSent =
        _adminController.getMessagingService().sendAndWait(cr, msg, asyncCallback, 60000);

    AssertJUnit.assertTrue(asyncCallback.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ReplyMessage")
        .equals("TestReplyMessage"));
    AssertJUnit.assertTrue(asyncCallback.getMessageReplied().size() == 1);

    AsyncCallback asyncCallback2 = new MockAsyncCallback();
    messagesSent = _adminController.getMessagingService().sendAndWait(cr, msg, asyncCallback2, 500);
    AssertJUnit.assertTrue(asyncCallback2.isTimedOut());
  }

  @Test()
  public void TestMultiMessageCriteria() throws Exception {
    for (int i = 0; i < NODE_NR; i++) {
      TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
      _participants[i].getMessagingService()
          .registerMessageHandlerFactory(factory.getMessageTypes(), factory);
    }
    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(new TestMessagingHandlerFactory().getMessageTypes().get(0), msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(_hostSrc);
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName("%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setClusterName(CLUSTER_NAME);

    AsyncCallback callback1 = new MockAsyncCallback();
    int messageSent1 =
        _adminController.getMessagingService().sendAndWait(cr, msg, callback1, 10000);

    AssertJUnit.assertTrue(callback1.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ReplyMessage")
        .equals("TestReplyMessage"));
    AssertJUnit.assertTrue(callback1.getMessageReplied().size() == NODE_NR);

    AsyncCallback callback2 = new MockAsyncCallback();
    int messageSent2 = _adminController.getMessagingService().sendAndWait(cr, msg, callback2, 500);

    AssertJUnit.assertTrue(callback2.isTimedOut());

    cr.setPartition("TestDB_17");
    AsyncCallback callback3 = new MockAsyncCallback();
    int messageSent3 =
        _adminController.getMessagingService().sendAndWait(cr, msg, callback3, 10000);
    AssertJUnit.assertTrue(callback3.getMessageReplied().size() == _replica);

    cr.setPartition("TestDB_15");
    AsyncCallback callback4 = new MockAsyncCallback();
    int messageSent4 =
        _adminController.getMessagingService().sendAndWait(cr, msg, callback4, 10000);
    AssertJUnit.assertTrue(callback4.getMessageReplied().size() == _replica);

    cr.setPartitionState("SLAVE");
    AsyncCallback callback5 = new MockAsyncCallback();
    int messageSent5 =
        _adminController.getMessagingService().sendAndWait(cr, msg, callback5, 10000);
    AssertJUnit.assertTrue(callback5.getMessageReplied().size() == _replica - 1);

    cr.setDataSource(DataSource.IDEALSTATES);
    AsyncCallback callback6 = new MockAsyncCallback();
    int messageSent6 =
        _adminController.getMessagingService().sendAndWait(cr, msg, callback6, 10000);
    AssertJUnit.assertTrue(callback6.getMessageReplied().size() == _replica - 1);
  }

  @Test()
  public void TestControllerMessage() {
    for (int i = 0; i < NODE_NR; i++) {
      TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
      _participants[i].getMessagingService()
          .registerMessageHandlerFactory(factory.getMessageTypes(), factory);

    }
    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(MessageType.CONTROLLER_MSG, msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(_hostSrc);
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName("*");
    cr.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr.setSessionSpecific(false);
    cr.setClusterName(CLUSTER_NAME);

    AsyncCallback callback1 = new MockAsyncCallback();
    int messagesSent =
        _adminController.getMessagingService().sendAndWait(cr, msg, callback1, 10000);

    AssertJUnit.assertTrue(callback1.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ControllerResult")
        .indexOf(_hostSrc) != -1);
    AssertJUnit.assertTrue(callback1.getMessageReplied().size() == 1);

    msgId = UUID.randomUUID().toString();
    msg.setMsgId(msgId);
    cr.setPartition("TestDB_17");
    AsyncCallback callback2 = new MockAsyncCallback();
    messagesSent = _adminController.getMessagingService().sendAndWait(cr, msg, callback2, 10000);

    AssertJUnit.assertTrue(callback2.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ControllerResult")
        .indexOf(_hostSrc) != -1);

    AssertJUnit.assertTrue(callback2.getMessageReplied().size() == 1);

    msgId = UUID.randomUUID().toString();
    msg.setMsgId(msgId);
    cr.setPartitionState("SLAVE");
    AsyncCallback callback3 = new MockAsyncCallback();
    messagesSent = _adminController.getMessagingService().sendAndWait(cr, msg, callback3, 10000);
    AssertJUnit.assertTrue(callback3.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ControllerResult")
        .indexOf(_hostSrc) != -1);

    AssertJUnit.assertTrue(callback3.getMessageReplied().size() == 1);
  }

  @Test(enabled = false)
  public void sendSelfMsg() {
    // Override the test defined in parent class.
  }
}
