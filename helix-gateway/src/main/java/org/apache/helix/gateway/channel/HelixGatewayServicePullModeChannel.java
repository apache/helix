package org.apache.helix.gateway.channel;

import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import org.apache.helix.model.Message;


public class HelixGatewayServicePullModeChannel implements HelixGatewayServiceChannel {

    public HelixGatewayServicePullModeChannel() {
    }
  @Override
  public void sendStateTransitionMessage(String instanceName, String currentState, Message message) {

  }

  @Override
  public void closeConnectionWithError(String instanceName, String reason) {

  }

  @Override
  public void completeConnection(String instanceName) {

  }
}
