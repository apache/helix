package org.apache.helix.gateway.statemodel;

import org.apache.helix.gateway.service.ClusterManager;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.statemodel.HelixGatewayOnlineOfflineStateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class HelixGatewayOnlineOfflineStateModelFactory extends StateModelFactory<HelixGatewayOnlineOfflineStateModel> {
  private GatewayServiceManager _clusterManager;

  public HelixGatewayOnlineOfflineStateModelFactory(GatewayServiceManager clusterManager) {
    _clusterManager = clusterManager;
  }

  @Override
  public HelixGatewayOnlineOfflineStateModel createNewStateModel(String resourceName,
      String partitionKey) {
    return new HelixGatewayOnlineOfflineStateModel(resourceName, partitionKey, _clusterManager);
  }
}
