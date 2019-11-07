package org.apache.helix.integration.paticipant;

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
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.manager.zk.DefaultParticipantErrorMessageHandlerFactory;
import org.apache.helix.manager.zk.DefaultParticipantErrorMessageHandlerFactory.ActionOnError;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestParticipantErrorMessage extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestParticipantErrorMessage.class);

  @Test()
  public void TestParticipantErrorMessageSend() {
    String participant1 = "localhost_" + START_PORT;
    String participant2 = "localhost_" + (START_PORT + 1);

    Message errorMessage1 =
        new Message(MessageType.PARTICIPANT_ERROR_REPORT, UUID.randomUUID().toString());
    errorMessage1.setTgtSessionId("*");
    errorMessage1.getRecord().setSimpleField(
        DefaultParticipantErrorMessageHandlerFactory.ACTIONKEY,
        ActionOnError.DISABLE_INSTANCE.toString());
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    recipientCriteria.setSessionSpecific(false);
    _participants[0].getMessagingService().send(recipientCriteria,
        errorMessage1);

    Message errorMessage2 =
        new Message(MessageType.PARTICIPANT_ERROR_REPORT, UUID.randomUUID().toString());
    errorMessage2.setTgtSessionId("*");
    errorMessage2.setResourceName("TestDB");
    errorMessage2.setPartitionName("TestDB_14");
    errorMessage2.getRecord().setSimpleField(
        DefaultParticipantErrorMessageHandlerFactory.ACTIONKEY,
        ActionOnError.DISABLE_PARTITION.toString());
    Criteria recipientCriteria2 = new Criteria();
    recipientCriteria2.setRecipientInstanceType(InstanceType.CONTROLLER);
    recipientCriteria2.setSessionSpecific(false);
    _participants[1].getMessagingService().send(recipientCriteria2,
        errorMessage2);

    try {
      Thread.sleep(1500);
    } catch (InterruptedException e) {
      LOG.error("Interrupted sleep", e);
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            CLUSTER_NAME));
    Assert.assertTrue(result);
    Builder kb = _participants[1].getHelixDataAccessor().keyBuilder();
    ExternalView externalView =
        _participants[1].getHelixDataAccessor().getProperty(
            kb.externalView("TestDB"));

    for (String partitionName : externalView.getRecord().getMapFields().keySet()) {
      for (String hostName : externalView.getRecord().getMapField(partitionName).keySet()) {
        if (hostName.equals(participant1)) {
          Assert.assertTrue(externalView.getRecord().getMapField(partitionName).get(hostName)
              .equalsIgnoreCase("OFFLINE"));
        }
      }
    }
    Assert.assertTrue(externalView.getRecord().getMapField("TestDB_14").get(participant2)
        .equalsIgnoreCase("OFFLINE"));
  }
}
