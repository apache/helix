package org.apache.helix.gateway.api.service;

import org.apache.helix.gateway.constant.MessageType;
import org.apache.helix.model.Message;

/**
 * Helix Gateway Service Processor interface allows sending state transition messages to
 * participants through services implementing this interface.
 */
public interface HelixGatewayServiceProcessor {

  public void sendStateTransitionMessage(String instanceName, MessageType messageType,
      Message message);

}
