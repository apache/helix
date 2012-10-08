package com.linkedin.helix.recipes.rabbitmq;

import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class ConsumerStateModelFactory extends StateModelFactory<ConsumerStateModel>
{
  private final String _consumerId;
  private final String _mqServer;
  public ConsumerStateModelFactory(String consumerId, String msServer)
  {
    _consumerId = consumerId;
    _mqServer = msServer;
  }
  
  @Override
  public ConsumerStateModel createNewStateModel(String partition)
  {
    ConsumerStateModel model = new ConsumerStateModel(_consumerId, partition, _mqServer);
    return model;
  }
}