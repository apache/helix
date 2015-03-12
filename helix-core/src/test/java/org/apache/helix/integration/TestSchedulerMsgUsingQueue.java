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
import java.util.Set;
import java.util.UUID;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchedulerMsgUsingQueue extends ZkStandAloneCMTestBase {
  TestSchedulerMessage.TestMessagingHandlerFactory _factory =
      new TestSchedulerMessage.TestMessagingHandlerFactory();

  @Test()
  public void testSchedulerMsgUsingQueue() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    _factory._results.clear();
    Thread.sleep(2000);
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");
    schedulerMessage.getRecord().setSimpleField(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsg");
    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), "Template");
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

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
    helixDataAccessor.createProperty(keyBuilder.controllerMessage(schedulerMessage.getMsgId()),
        schedulerMessage);

    for (int i = 0; i < 30; i++) {
      Thread.sleep(2000);
      if (_PARTITIONS == _factory._results.size()) {
        break;
      }
    }

    Assert.assertEquals(_PARTITIONS, _factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(),
            schedulerMessage.getMsgId());

    int messageResultCount = 0;
    for (int i = 0; i < 10; i++) {
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
          .equals("" + (_PARTITIONS * 3)));
      for (String key : statusUpdate.getMapFields().keySet()) {
        if (key.startsWith("MessageResult ")) {
          messageResultCount++;
        }
      }
      if (messageResultCount == _PARTITIONS * 3) {
        break;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.assertEquals(messageResultCount, _PARTITIONS * 3);
    int count = 0;
    for (Set<String> val : _factory._results.values()) {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);

  }
}
