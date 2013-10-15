package org.apache.helix.tools;

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

import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.util.HelixUtil;

public class MessagePoster {
  public void post(String zkServer, Message message, String clusterName, String instanceName) {
    ZkClient client = new ZkClient(zkServer);
    client.setZkSerializer(new ZNRecordSerializer());
    String path = HelixUtil.getMessagePath(clusterName, instanceName) + "/" + message.getId();
    client.delete(path);
    ZNRecord record = client.readData(HelixUtil.getLiveInstancePath(clusterName, instanceName));
    message.setTgtSessionId(SessionId.from(record.getSimpleField(
        LiveInstanceProperty.SESSION_ID.toString()).toString()));
    message.setTgtName(record.getId());
    // System.out.println(message);
    client.createPersistent(path, message.getRecord());
  }

  public void postFaultInjectionMessage(String zkServer, String clusterName, String instanceName,
      String payloadString, String partition) {
    Message message = new Message("FaultInjection", MessageId.from(UUID.randomUUID().toString()));
    if (payloadString != null) {
      message.getRecord().setSimpleField("faultType", payloadString);
    }
    if (partition != null) {
      message.setPartitionId(PartitionId.from(partition));
    }

    post(zkServer, message, clusterName, instanceName);
  }

  public void postTestMessage(String zkServer, String clusterName, String instanceName) {
    String msgSrc = "cm-instance-0";
    MessageId msgId = MessageId.from("TestMessageId-2");

    Message message = new Message(MessageType.STATE_TRANSITION, msgId);
    message.setMessageId(msgId);
    message.setSrcName(msgSrc);
    message.setTgtName(instanceName);
    message.setMsgState(MessageState.NEW);
    message.setFromState(State.from("Slave"));
    message.setToState(State.from("Master"));
    message.setPartitionId(PartitionId.from("EspressoDB.partition-0." + instanceName));

    post(zkServer, message, clusterName, instanceName);
  }

  public static void main(String[] args) {
    if (args.length < 4 || args.length > 6) {
      System.err.println("Usage: java " + MessagePoster.class.getName()
          + " zkServer cluster instance msgType [payloadString] [partition]");
      System.err.println("msgType can be one of test, fault");
      System.err.println("payloadString is sent along with the fault msgType");
      System.exit(1);
    }
    String zkServer = args[0];
    String cluster = args[1];
    String instance = args[2];
    String msgType = args[3];
    String payloadString = (args.length >= 5 ? args[4] : null);
    String partition = (args.length == 6 ? args[5] : null);

    MessagePoster messagePoster = new MessagePoster();
    if (msgType.equals("test")) {
      messagePoster.postTestMessage(zkServer, cluster, instance);
    } else if (msgType.equals("fault")) {
      messagePoster
          .postFaultInjectionMessage(zkServer, cluster, instance, payloadString, partition);
      System.out.println("Posted " + msgType);
    } else {
      System.err.println("Message was not posted. Unknown msgType:" + msgType);
    }
  }
}
