package com.linkedin.clustermanager.participant;

import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;

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
