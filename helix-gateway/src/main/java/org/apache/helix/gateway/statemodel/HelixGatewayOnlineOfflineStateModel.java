package org.apache.helix.gateway.statemodel;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.NotificationContext;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;

public class HelixGatewayOnlineOfflineStateModel extends StateModel {
  private boolean _firstTime = true;
  private GatewayServiceManager _gatewayServiceManager;

  private String _resourceName;
  private String _partitionKey;

  private AtomicBoolean _completed;

  public HelixGatewayOnlineOfflineStateModel(String resourceName, String partitionKey,
      GatewayServiceManager gatewayServiceManager) {
    _resourceName = resourceName;
    _partitionKey = partitionKey;
    _gatewayServiceManager = gatewayServiceManager;
  }

  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    if (_firstTime) {
      wait(_gatewayServiceManager.sendTransitionRequestToApplicationInstance());
      System.out.println(
          "Message for " + message.getPartitionName() + " instance " + message.getTgtName() + " with ADD for "
              + message.getResourceName() + " processed");
      _firstTime = false;
    }
    wait(_gatewayServiceManager.sendTransitionRequestToApplicationInstance());
    System.out.println("Message for " + message.getPartitionName() + " instance " + message.getTgtName()
        + " with CHANGE_ROLE_OFFLINE_ONLINE for " + message.getResourceName() + " processed");
  }

  public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    wait(_gatewayServiceManager.sendTransitionRequestToApplicationInstance());
    System.out.println("Message for " + message.getPartitionName() + " instance " + message.getTgtName()
        + " with CHANGE_ROLE_ONLINE_OFFLINE for " + message.getResourceName() + " processed");
  }

  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    wait(_gatewayServiceManager.sendTransitionRequestToApplicationInstance());
    System.out.println(
        "Message for " + message.getPartitionName() + " instance " + message.getTgtName() + " with REMOVE for "
            + message.getResourceName() + " processed");
  }

  private void wait(AtomicBoolean completed) {
    _completed = completed;
    while (true) {
      try {
        if (_completed.get()) {
          break;
        }
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
