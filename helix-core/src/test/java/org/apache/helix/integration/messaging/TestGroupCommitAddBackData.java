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

import java.util.Date;
import java.util.UUID;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestGroupCommitAddBackData extends ZkTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestGroupCommitAddBackData.class);
  private static final int START_PORT = 12918;
  private static final int DEFAULT_TIMEOUT = 30 * 1000;

  private HelixManager _manager;
  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private MockParticipantManager _participant;

  @BeforeClass
  public void beforeClass() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    String storageNodeName = PARTICIPANT_PREFIX + "_" + START_PORT;
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    _participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
    _participant.syncStart();

    // create cluster manager
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_participant != null && _participant.isConnected()) {
      _participant.syncStop();
    }

    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      try {
        _gSetupTool.deleteCluster(CLUSTER_NAME);
      } catch (Exception ex) {
        System.err.println(
            "Failed to delete cluster " + CLUSTER_NAME + ", error: " + ex.getLocalizedMessage());
        _gSetupTool.deleteCluster(CLUSTER_NAME);
      }
    }

    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testGroupCommitAddCurrentStateBack() throws InterruptedException {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Message initMessage = generateMessage("OFFLINE", "ONLINE");
    accessor.setProperty(
        accessor.keyBuilder().message(_participant.getInstanceName(), initMessage.getMsgId()),
        initMessage);
    Assert.assertTrue(waitForMessageProcessed(accessor, initMessage.getMsgId()));
    Message toOffline = generateMessage("ONLINE", "OFFLINE");
    accessor.setProperty(
        accessor.keyBuilder().message(_participant.getInstanceName(), toOffline.getMsgId()),
        toOffline);
    Assert.assertTrue(waitForMessageProcessed(accessor, toOffline.getMsgId()));

    // Consequential 10 messages
    for (int i = 0; i < 10; i++) {
      Message dropped = generateMessage("OFFLINE", "DROPPED");
      accessor.setProperty(
          accessor.keyBuilder().message(_participant.getInstanceName(), dropped.getMsgId()),
          dropped);
      Assert.assertTrue(waitForMessageProcessed(accessor, dropped.getMsgId()));
      Assert.assertFalse(accessor.getBaseDataAccessor()
          .exists(accessor.keyBuilder().currentState(_participant.getInstanceName(),
              _participant.getSessionId(), WorkflowGenerator.DEFAULT_TGT_DB).getPath(), 0));
    }
  }

  private Message generateMessage(String from, String to) {
    String uuid = UUID.randomUUID().toString();
    Message message = new Message(Message.MessageType.STATE_TRANSITION, uuid);
    message.setSrcName("ADMIN");
    message.setTgtName(_participant.getInstanceName());
    message.setMsgState(Message.MessageState.NEW);
    message.setPartitionName("P");
    message.setResourceName(WorkflowGenerator.DEFAULT_TGT_DB);
    message.setFromState(from);
    message.setToState(to);
    message.setTgtSessionId(_participant.getSessionId());
    message.setSrcSessionId(_manager.getSessionId());
    message.setStateModelDef("OnlineOffline");
    message.setStateModelFactoryName("DEFAULT");
    return message;
  }

  private boolean waitForMessageProcessed(HelixDataAccessor accessor, String messageId)
      throws InterruptedException {
    String path =
        accessor.keyBuilder().message(_participant.getInstanceName(), messageId).getPath();
    long startTime = System.currentTimeMillis();
    while (accessor.getBaseDataAccessor().exists(path, 0)) {
      if (System.currentTimeMillis() - startTime > DEFAULT_TIMEOUT) {
        return false;
      }
      Thread.sleep(200);
    }
    return true;
  }
}
