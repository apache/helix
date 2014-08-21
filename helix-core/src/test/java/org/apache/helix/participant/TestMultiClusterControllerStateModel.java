package org.apache.helix.participant;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.participant.MultiClusterControllerTransitionHandler;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestMultiClusterControllerStateModel extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestMultiClusterControllerStateModel.class);

  final String clusterName = "TestMultiClusterControllerStateModel";
  MultiClusterControllerTransitionHandler stateModel = null;

  @BeforeMethod()
  public void beforeMethod() {
    stateModel = new MultiClusterControllerTransitionHandler(_zkaddr);
    if (_zkclient.exists("/" + clusterName)) {
      _zkclient.deleteRecursive("/" + clusterName);
    }
    TestHelper.setupEmptyCluster(_zkclient, clusterName);
  }

  @Test()
  public void testOnBecomeStandbyFromOffline() {
    stateModel.onBecomeStandbyFromOffline(null, null);
  }

  @Test()
  public void testOnBecomeLeaderFromStandby() {
    Message message = new Message(MessageType.STATE_TRANSITION, MessageId.from("0"));
    message.setPartitionId(PartitionId.from(clusterName));
    message.setTgtName("controller_0");
    try {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    } catch (Exception e) {
      LOG.error("Exception becoming leader from standby", e);
    }
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }

  @Test()
  public void testOnBecomeStandbyFromLeader() {
    Message message = new Message(MessageType.STATE_TRANSITION, MessageId.from("0"));
    message.setPartitionId(PartitionId.from(clusterName));
    message.setTgtName("controller_0");
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }

  @Test()
  public void testOnBecomeOfflineFromStandby() {
    Message message = new Message(MessageType.STATE_TRANSITION, MessageId.from("0"));
    message.setPartitionId(PartitionId.from(clusterName));
    message.setTgtName("controller_0");

    stateModel.onBecomeOfflineFromStandby(message, null);
  }

  @Test()
  public void testOnBecomeDroppedFromOffline() {
    stateModel.onBecomeDroppedFromOffline(null, null);
  }

  @Test()
  public void testOnBecomeOfflineFromDropped() {
    stateModel.onBecomeOfflineFromDropped(null, null);
  }

  @Test()
  public void testRollbackOnError() {
    Message message = new Message(MessageType.STATE_TRANSITION, MessageId.from("0"));
    message.setPartitionId(PartitionId.from(clusterName));
    message.setTgtName("controller_0");
    try {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    } catch (Exception e) {
      LOG.error("Exception becoming leader from standby", e);
    }
    stateModel.rollbackOnError(message, new NotificationContext(null), null);
  }

  @Test()
  public void testReset() {
    Message message = new Message(MessageType.STATE_TRANSITION, MessageId.from("0"));
    message.setPartitionId(PartitionId.from(clusterName));
    message.setTgtName("controller_0");
    try {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    } catch (Exception e) {
      LOG.error("Exception becoming leader from standby", e);
    }
    stateModel.reset();
  }

}
