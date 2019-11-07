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

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMessagePartitionStateMismatch extends ZkStandAloneCMTestBase {
  @Test
  public void testStateMismatch() throws InterruptedException {
    // String controllerName = CONTROLLER_PREFIX + "_0";

    HelixManager manager = _controller; // _startCMResultMap.get(controllerName)._manager;
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder kb = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(kb.externalView(TEST_DB));
    Map<String, LiveInstance> liveinstanceMap =
        accessor.getChildValuesMap(accessor.keyBuilder().liveInstances());

    for (String instanceName : liveinstanceMap.keySet()) {
      String sessionid = liveinstanceMap.get(instanceName).getEphemeralOwner();
      for (String partition : ev.getPartitionSet()) {
        if (ev.getStateMap(partition).containsKey(instanceName)) {
          String uuid = UUID.randomUUID().toString();
          Message message = new Message(MessageType.STATE_TRANSITION, uuid);
          boolean rand = new Random().nextInt(10) > 5;
          if (ev.getStateMap(partition).get(instanceName).equals("MASTER")) {
            message.setSrcName(manager.getInstanceName());
            message.setTgtName(instanceName);
            message.setMsgState(MessageState.NEW);
            message.setPartitionName(partition);
            message.setResourceName(TEST_DB);
            message.setFromState(rand ? "SLAVE" : "OFFLINE");
            message.setToState(rand ? "MASTER" : "SLAVE");
            message.setTgtSessionId(sessionid);
            message.setSrcSessionId(manager.getSessionId());
            message.setStateModelDef("MasterSlave");
            message.setStateModelFactoryName("DEFAULT");
          } else if (ev.getStateMap(partition).get(instanceName).equals("SLAVE")) {
            message.setSrcName(manager.getInstanceName());
            message.setTgtName(instanceName);
            message.setMsgState(MessageState.NEW);
            message.setPartitionName(partition);
            message.setResourceName(TEST_DB);
            message.setFromState(rand ? "MASTER" : "OFFLINE");
            message.setToState(rand ? "SLAVE" : "SLAVE");
            message.setTgtSessionId(sessionid);
            message.setSrcSessionId(manager.getSessionId());
            message.setStateModelDef("MasterSlave");
            message.setStateModelFactoryName("DEFAULT");
          }
          accessor.setProperty(accessor.keyBuilder().message(instanceName, message.getMsgId()),
              message);
        }
      }
    }
    Thread.sleep(3000);
    ExternalView ev2 = accessor.getProperty(kb.externalView(TEST_DB));
    Assert.assertTrue(ev.equals(ev2));
  }
}
