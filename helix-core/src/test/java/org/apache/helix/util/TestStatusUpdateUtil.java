package org.apache.helix.util;

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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.helix.HelixConstants;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.messaging.handling.HelixStateTransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStatusUpdateUtil extends ZkTestBase {
  private String clusterName = TestHelper.getTestClassName();
  private int n = 1;
  private Message message = new Message(Message.MessageType.STATE_TRANSITION, "Some unique id");
  private MockParticipantManager[] participants = new MockParticipantManager[n];

  @AfterClass
  public void afterClass() {
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        1, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true);

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    message.setSrcName("cm-instance-0");
    message.setTgtSessionId(participants[0].getSessionId());
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setPartitionName("TestDB_0");
    message.setMsgId("Some unique message id");
    message.setResourceName("TestDB");
    message.setTgtName("localhost_12918");
    message.setStateModelDef("MasterSlave");
    message.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);

  }

  @Test(dependsOnMethods = "testDisableErrorLogByDefault")
  public void testEnableErrorLog() throws Exception {
    StatusUpdateUtil statusUpdateUtil = new StatusUpdateUtil();


    String errPath = PropertyPathBuilder
        .instanceError(clusterName, "localhost_12918", participants[0].getSessionId(), "TestDB",
            "TestDB_0");
    ZNRecord messageRecord = statusUpdateUtil.createMessageStatusUpdateRecord(message, StatusUpdateUtil.Level.HELIX_ERROR, HelixStateTransitionHandler.class,
        "testEnableErrorLog");
    // logged to Zookeeper
    statusUpdateUtil.publishErrorRecord(messageRecord, participants[0].getInstanceName(), message.getResourceName(), message.getPartitionName(), participants[0].getSessionId(),
        participants[0].getHelixDataAccessor(), false, true);

    try {
      ZNRecord error = _gZkClient.readData(errPath);
    } catch (ZkException zke) {
      Assert.fail("expecting being able to send error logs to ZK.", zke);
    }
  }

  @Test
  public void testDisableErrorLogByDefault() throws Exception {
    StatusUpdateUtil statusUpdateUtil = new StatusUpdateUtil();
    ZNRecord messageRecord = statusUpdateUtil.createMessageStatusUpdateRecord(message, StatusUpdateUtil.Level.HELIX_ERROR, HelixStateTransitionHandler.class,
        "testDisableErrorLogByDefault");
    statusUpdateUtil.publishErrorRecord(messageRecord, participants[0].getInstanceName(), message.getResourceName(), message.getPartitionName(), participants[0].getSessionId(),
        participants[0].getHelixDataAccessor(), false, false);
    // assert by default, not logged to Zookeeper
    String errPath = PropertyPathBuilder
        .instanceError(clusterName, "localhost_12918", participants[0].getSessionId(), "TestDB",
            "TestDB_0");

    try {
      ZNRecord error = _gZkClient.readData(errPath);
      Assert.fail(String.format(
          "not expecting being able to send error logs to ZK by default. Read data %s",
          error.toString()));
    } catch (ZkException zke) {
      // expected
    }
  }
}
