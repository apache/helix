package com.linkedin.clustermanager.mock.relay;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.mock.storage.StorageStateModel;
import com.linkedin.clustermanager.mock.storage.StorageStateModelFactory;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;

public class RelayStateModelFactory extends StateModelFactory
{
  private static Logger logger = Logger
      .getLogger(StorageStateModelFactory.class);

  private RelayAdapter relayAdapter;

  public RelayStateModelFactory(RelayAdapter relay)
  {
    relayAdapter = relay;
  }

  @Override
  public StateModel createNewStateModel(String stateUnitKey)
  {
    logger.info("RelayStateModelFactory.getStateModel()");
    return new RelayStateModel(stateUnitKey, relayAdapter);
  }

}
