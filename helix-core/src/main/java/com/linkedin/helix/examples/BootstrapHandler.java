/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.examples;

import java.util.UUID;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

public class BootstrapHandler extends StateModelFactory<StateModel>
{

  @Override
  public StateModel createNewStateModel(String stateUnitKey)
  {
    return new BootstrapStateModel(stateUnitKey);
  }

  @StateModelInfo(initialState = "OFFLINE", states = "{'OFFLINE','SLAVE','MASTER'}")
  public static class BootstrapStateModel extends StateModel
  {

    private final String _stateUnitKey;

    public BootstrapStateModel(String stateUnitKey)
    {
      _stateUnitKey = stateUnitKey;

    }
    @Transition(from = "MASTER", to = "SLAVE")
    public void masterToSlave(Message message, NotificationContext context)
    {
      
    }
    @Transition(from = "OFFLINE", to = "SLAVE")
    public void offlineToSlave(Message message, NotificationContext context)
    {
      System.out
          .println("BootstrapProcess.BootstrapStateModel.offlineToSlave()");
      HelixManager manager = context.getManager();
      ClusterMessagingService messagingService = manager.getMessagingService();
      Message requestBackupUriRequest = new Message(
          MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
      requestBackupUriRequest
          .setMsgSubType(BootstrapProcess.REQUEST_BOOTSTRAP_URL);
      requestBackupUriRequest.setMsgState(MessageState.NEW);
      Criteria recipientCriteria = new Criteria();
      recipientCriteria.setInstanceName("*");
      recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
      recipientCriteria.setResource(message.getResourceName());
      recipientCriteria.setPartition(message.getPartitionName());
      recipientCriteria.setSessionSpecific(true);
      // wait for 30 seconds
      int timeout = 30000;
      BootstrapReplyHandler responseHandler = new BootstrapReplyHandler();

      int sentMessageCount = messagingService.sendAndWait(recipientCriteria,
          requestBackupUriRequest, responseHandler, timeout);
      if (sentMessageCount == 0)
      {
        // could not find any other node hosting the partition
      } else if (responseHandler.getBootstrapUrl() != null)
      {
        System.out.println("Got bootstrap url:"+ responseHandler.getBootstrapUrl() );
        System.out.println("Got backup time:"+ responseHandler.getBootstrapTime() );
        // Got the url fetch it
      } else
      {
        // Either go to error state
        // throw new Exception("Cant find backup/bootstrap data");
        // Request some node to start backup process
      }
    }
    @Transition(from = "SLAVE", to = "OFFLINE")
    public void slaveToOffline(Message message, NotificationContext context)
    {
      System.out
          .println("BootstrapProcess.BootstrapStateModel.slaveToOffline()");
    }
    @Transition(from = "SLAVE", to = "MASTER")
    public void slaveToMaster(Message message, NotificationContext context)
    {
      System.out
          .println("BootstrapProcess.BootstrapStateModel.slaveToMaster()");
    }

  }
}