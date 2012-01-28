package com.linkedin.helix.participant;

import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class DistClusterControllerStateModelFactory extends StateModelFactory<DistClusterControllerStateModel>
{
  private final String _zkAddr;
  public DistClusterControllerStateModelFactory(String zkAddr)
  {
    _zkAddr = zkAddr;
  }

  @Override
  public DistClusterControllerStateModel createNewStateModel(String stateUnitKey)
  {
    return new DistClusterControllerStateModel(_zkAddr);
  }

}
