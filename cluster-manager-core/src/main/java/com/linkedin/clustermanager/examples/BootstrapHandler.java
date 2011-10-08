package com.linkedin.clustermanager.examples;

import java.util.UUID;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.Criteria;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;
import com.linkedin.clustermanager.participant.statemachine.StateModelInfo;
import com.linkedin.clustermanager.participant.statemachine.Transition;

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

    @Transition(from = "OFFLINE", to = "SLAVE")
    public void offlineToSlave(Message message, NotificationContext context)
    {
      System.out
          .println("BootstrapProcess.BootstrapStateModel.offlineToSlave()");
      ClusterManager manager = context.getManager();
      ClusterMessagingService messagingService = manager.getMessagingService();
      Message requestBackupUriRequest = new Message(
          MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
      requestBackupUriRequest
          .setMsgSubType(BootstrapProcess.REQUEST_BOOTSTRAP_URL);
      Criteria recipientCriteria = new Criteria();
      recipientCriteria.setInstanceName("*");
      recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
      recipientCriteria.setResourceGroup(message.getResourceGroupName());
      recipientCriteria.setResourceKey(message.getResourceKey());
      recipientCriteria.setSessionSpecific(true);
      // wait for 30 seconds
      int timeout = 30000;
      BootstrapReplyHandler responseHandler = new BootstrapReplyHandler(timeout);

      int sentMessageCount = messagingService.sendAndWait(recipientCriteria,
          requestBackupUriRequest, responseHandler);
      if (sentMessageCount == 0)
      {
        // could not find any other node hosting the partition
      } else if (responseHandler.getBootstrapUrl() != null)
      {
        // Got the url fetch it
      } else
      {
        // Either go to error state
        // throw new Exception("Cant find backup/bootstrap data");
        // Request some node to start backup process
      }
    }

    @Transition(from = "SLAVE", to = "MASTER")
    public void slaveToMaster(Message message, NotificationContext context)
    {
      System.out
          .println("BootstrapProcess.BootstrapStateModel.slaveToMaster()");
    }

  }
}