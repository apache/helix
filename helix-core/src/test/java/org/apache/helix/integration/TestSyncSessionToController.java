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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.Message;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSyncSessionToController extends ZkTestBase {
  @Test
  public void testSyncSessionToController() throws Exception {
    System.out
        .println("START testSyncSessionToController at " + new Date(System.currentTimeMillis()));

    String clusterName = getShortClassName();
    MockParticipantManager[] participants = new MockParticipantManager[5];
    int resourceNb = 10;
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        resourceNb, // resources
        1, // partitions per resource
        5, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ZKHelixManager zkHelixManager = new ZKHelixManager(clusterName, "controllerMessageListener",
        InstanceType.CONTROLLER, ZK_ADDR);
    zkHelixManager.connect();
    MockMessageListener mockMessageListener = new MockMessageListener();
    zkHelixManager.addControllerMessageListener(mockMessageListener);

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<>(_gZkClient);
    String path = keyBuilder.liveInstance("localhost_12918").getPath();
    Stat stat = new Stat();
    ZNRecord data = accessor.get(path, stat, 2);
    data.getSimpleFields().put("SESSION_ID", "invalid-id");
    accessor.set(path, data, 2);
    Thread.sleep(2000);
    // Since we always read the content from ephemeral nodes, sync message won't be sent
    Assert.assertFalse(mockMessageListener.isSessionSyncMessageSent());

    // Even after reconnect, session sync won't happen
    ZkTestHelper.expireSession(participants[0].getZkClient());
    Assert.assertFalse(mockMessageListener.isSessionSyncMessageSent());

    // Inject an invalid session message to trigger sync message
    PropertyKey messageKey = keyBuilder.message("localhost_12918", "Mocked Invalid Message");
    Message msg = new Message(Message.MessageType.STATE_TRANSITION, "Mocked Invalid Message");
    msg.setSrcName(controller.getInstanceName());
    msg.setTgtSessionId("invalid-id");
    msg.setMsgState(Message.MessageState.NEW);
    msg.setMsgId("Mocked Invalid Message");
    msg.setTgtName("localhost_12918");
    msg.setPartitionName("foo");
    msg.setResourceName("bar");
    msg.setFromState("SLAVE");
    msg.setToState("MASTER");
    msg.setSrcSessionId(controller.getSessionId());
    msg.setStateModelDef("MasterSlave");
    msg.setStateModelFactoryName("DEFAULT");
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(clusterName, accessor);
    dataAccessor.setProperty(messageKey, msg);
    Assert.assertTrue(TestHelper.verify(() -> mockMessageListener.isSessionSyncMessageSent(), 1500));

    // Cleanup
    controller.syncStop();
    zkHelixManager.disconnect();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);
  }

  class MockMessageListener implements MessageListener {
    private boolean sessionSyncMessageSent = false;

    @Override
    public void onMessage(String instanceName, List<Message> messages,
        NotificationContext changeContext) {
      for (Message message : messages) {
        if (message.getMsgId().equals("SESSION-SYNC")) {
          sessionSyncMessageSent = true;
        }
      }
    }

    boolean isSessionSyncMessageSent() {
      return sessionSyncMessageSent;
    }
  }
}
