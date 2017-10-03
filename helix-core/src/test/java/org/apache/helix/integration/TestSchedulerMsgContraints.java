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

import java.io.StringWriter;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchedulerMsgContraints extends ZkStandAloneCMTestBase {

  @Test
  public void testSchedulerMsgContraints() throws Exception {
    TestSchedulerMessage.TestMessagingHandlerFactoryLatch factory =
        new TestSchedulerMessage.TestMessagingHandlerFactoryLatch();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);

      _participants[i].getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(factory.getMessageType(), "Template");
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResource("%");
    cr.setPartition("%");

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();

    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");
    schedulerMessage.getRecord().setSimpleField("WAIT_ALL", "true");
    schedulerMessage.getRecord().setSimpleField(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsgContraints");

    Criteria cr2 = new Criteria();
    cr2.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr2.setInstanceName("*");
    cr2.setSessionSpecific(false);

    TestSchedulerMessage.MockAsyncCallback callback = new TestSchedulerMessage.MockAsyncCallback();
    mapper = new ObjectMapper();
    serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    sw = new StringWriter();
    mapper.writeValue(sw, cr);

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();

    // Set constraints that only 1 message per participant
    Map<String, String> constraints = new TreeMap<String, String>();
    constraints.put("MESSAGE_TYPE", "STATE_TRANSITION");
    constraints.put("TRANSITION", "OFFLINE-COMPLETED");
    constraints.put("CONSTRAINT_VALUE", "1");
    constraints.put("INSTANCE", ".*");
    manager.getClusterManagmentTool().setConstraint(manager.getClusterName(),
        ConstraintType.MESSAGE_CONSTRAINT, "constraint1", new ConstraintItem(constraints));

    // Send scheduler message
    crString = sw.toString();
    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);
    String msgId =
        callback._message.getResultMap()
            .get(DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);

    for (int j = 0; j < 10; j++) {
      Thread.sleep(200);
      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.name(), msgId);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      if (statusUpdate.getMapFields().containsKey("SentMessageCount")) {
        Assert.assertEquals(
            statusUpdate.getMapFields().get("SentMessageCount").get("MessageCount"), ""
                + (_PARTITIONS * 3));
        break;
      }
    }

    for (int i = 0; i < _PARTITIONS * 3 / 5; i++) {
      for (int j = 0; j < 10; j++) {
        Thread.sleep(300);
        if (factory._messageCount == 5 * (i + 1))
          break;
      }
      Thread.sleep(300);
      Assert.assertEquals(factory._messageCount, 5 * (i + 1));
      factory.signal();
      // System.err.println(i);
    }

    for (int j = 0; j < 10; j++) {
      Thread.sleep(200);
      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.name(), msgId);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      if (statusUpdate.getMapFields().containsKey("Summary")) {
        break;
      }
    }

    Assert.assertEquals(_PARTITIONS, factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.name(), msgId);
    ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
    Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
        .equals("" + (_PARTITIONS * 3)));
    int messageResultCount = 0;
    for (String key : statusUpdate.getMapFields().keySet()) {
      if (key.startsWith("MessageResult ")) {
        messageResultCount++;
      }
    }
    Assert.assertEquals(messageResultCount, _PARTITIONS * 3);

    int count = 0;
    for (Set<String> val : factory._results.values()) {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);

    manager.getClusterManagmentTool().removeConstraint(manager.getClusterName(),
        ConstraintType.MESSAGE_CONSTRAINT, "constraint1");

  }
}
