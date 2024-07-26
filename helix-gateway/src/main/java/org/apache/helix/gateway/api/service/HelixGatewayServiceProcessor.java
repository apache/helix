package org.apache.helix.gateway.api.service;

import org.apache.helix.gateway.constant.MessageType;
import org.apache.helix.model.Message;

/**
 * Translate from/to GRPC function call to Helix Gateway Service event.
 */
public interface HelixGatewayServiceProcessor {

  public void sendStateTransitionMessage(String instanceName, MessageType messageType,
      Message message);

}
