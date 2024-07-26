package org.apache.helix.gateway.api.participant;

import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateTransitionError;

/**
 * Process multi-top state model state transition message.
 */
public interface HelixGatewayMultiTopStateStateTransitionProcessor {
  /**
   * Process multi-top state model state transition message.
   * @param message state transition message
   * @throws Exception if failed to process the message
   */
  void processMultiTopStateModelStateTransitionMessage(Message message) throws Exception;

  /**
   * Handle state transition error. This results from state transition handler throwing an exception or
   * timing out.
   *
   * @param message state transition message
   * @param error state transition error
   */
  void handleStateTransitionError(Message message, StateTransitionError error);
}
