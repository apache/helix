package com.linkedin.clustermanager.mock.storage;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.mock.consumer.ConsumerAdapter;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;

public class StorageStateModelFactory extends StateModelFactory
{
  private static Logger logger = Logger
      .getLogger(StorageStateModelFactory.class);

  private StorageAdapter storageAdapter;

  // private ConsumerAdapter consumerAdapter;

  public StorageStateModelFactory(StorageAdapter storage)
  {
    storageAdapter = storage;
  }

  StorageStateModel getStateModelForPartition(Integer partition)
  {
    return null;
  }

  @Override
  public StateModel createNewStateModel(String stateUnitKey)
  {
    logger.info("StorageStateModelFactory.getStateModel()");
    return new StorageStateModel(stateUnitKey, storageAdapter);
  }

}
