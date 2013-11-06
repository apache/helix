package org.apache.helix.participant.statemachine;

import org.apache.helix.api.id.PartitionId;

public class HelixStateModelFactoryAdaptor<T extends StateModel> extends StateModelFactory<T> {
  final HelixStateModelFactory<T> _factory;

  public HelixStateModelFactoryAdaptor(HelixStateModelFactory<T> factory) {
    _factory = factory;
  }

  @Override
  public T createNewStateModel(String partitionName) {
    return _factory.createNewStateModel(PartitionId.from(partitionName));
  }

}
