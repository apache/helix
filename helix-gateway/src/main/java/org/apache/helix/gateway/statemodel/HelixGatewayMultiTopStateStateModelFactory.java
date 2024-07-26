package org.apache.helix.gateway.statemodel;

import org.apache.helix.gateway.api.participant.HelixGatewayMultiTopStateStateTransitionProcessor;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class HelixGatewayMultiTopStateStateModelFactory extends StateModelFactory<HelixGatewayMultiTopStateStateModel> {
  private final HelixGatewayMultiTopStateStateTransitionProcessor _stateTransitionProcessor;

  public HelixGatewayMultiTopStateStateModelFactory(
      HelixGatewayMultiTopStateStateTransitionProcessor stateTransitionProcessor) {
    _stateTransitionProcessor = stateTransitionProcessor;
  }

  @Override
  public HelixGatewayMultiTopStateStateModel createNewStateModel(String resourceName,
      String partitionKey) {
    return new HelixGatewayMultiTopStateStateModel(_stateTransitionProcessor);
  }
}
