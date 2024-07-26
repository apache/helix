package org.apache.helix.gateway.statemodel;

import org.apache.helix.NotificationContext;
import org.apache.helix.gateway.api.participant.HelixGatewayMultiTopStateStateTransitionProcessor;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = "OFFLINE", states = {})
public class HelixGatewayMultiTopStateStateModel extends StateModel {
  private static final Logger _logger =
      LoggerFactory.getLogger(HelixGatewayMultiTopStateStateModel.class);

  private final HelixGatewayMultiTopStateStateTransitionProcessor _stateTransitionProcessor;

  public HelixGatewayMultiTopStateStateModel(
      HelixGatewayMultiTopStateStateTransitionProcessor stateTransitionProcessor) {
    _stateTransitionProcessor = stateTransitionProcessor;
  }

  @Transition(to = "*", from = "*")
  public void genericStateTransitionHandler(Message message, NotificationContext context)
      throws Exception {
    _stateTransitionProcessor.processMultiTopStateModelStateTransitionMessage(message);
  }

  @Override
  public void reset() {
    // no-op we don't want to start from init state again.
  }

  @Override
  public void rollbackOnError(Message message, NotificationContext context,
      StateTransitionError error) {
    _stateTransitionProcessor.handleStateTransitionError(message, error);
  }
}