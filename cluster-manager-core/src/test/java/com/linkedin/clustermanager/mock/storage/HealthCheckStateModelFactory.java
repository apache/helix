package com.linkedin.clustermanager.mock.storage;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;

public class HealthCheckStateModelFactory extends StateModelFactory
{
  private static Logger logger = Logger
      .getLogger(HealthCheckStateModelFactory.class);

  private StorageAdapter storageAdapter;

  // private ConsumerAdapter consumerAdapter;

  public HealthCheckStateModelFactory(StorageAdapter storage)
  {
    storageAdapter = storage;
  }

  HealthCheckStateModel getStateModelForPartition(Integer partition)
  {
    return null;
  }

  @Override
  public StateModel createNewStateModel(String stateUnitKey)
  {
    logger.info("HealthCheckStateModelFactory.getStateModel()");
    //TODO: fix these parameters
    return new HealthCheckStateModel(stateUnitKey, storageAdapter, null, 0, null, null);
  }

}
