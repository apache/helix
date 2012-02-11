package com.linkedin.helix.examples;

import java.util.UUID;

import com.linkedin.helix.HelixAgent;
import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
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
      HelixAgent manager = context.getManager();
      ClusterMessagingService messagingService = manager.getMessagingService();
      Message requestBackupUriRequest = new Message(
          MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
      requestBackupUriRequest
          .setMsgSubType(BootstrapProcess.REQUEST_BOOTSTRAP_URL);
      requestBackupUriRequest.setMsgState("new");
      Criteria recipientCriteria = new Criteria();
      recipientCriteria.setInstanceName("*");
      recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
      recipientCriteria.setResourceGroup(message.getResourceGroupName());
      recipientCriteria.setResourceKey(message.getResourceKey());
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