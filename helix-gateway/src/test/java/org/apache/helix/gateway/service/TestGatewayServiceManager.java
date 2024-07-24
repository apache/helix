package org.apache.helix.gateway.service;

import org.apache.helix.gateway.grpcservice.HelixGatewayServiceGrpcService;
import org.testng.annotations.Test;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public class TestGatewayServiceManager {

  private GatewayServiceManager manager;

  @Test
  public void testConnectionAndDisconnectionEvents() {

    manager = mock(GatewayServiceManager.class);
    HelixGatewayServiceGrpcService grpcService = new HelixGatewayServiceGrpcService(manager);
    // Mock a connection event
    HelixGatewayServiceOuterClass.ShardStateMessage connectionEvent =
        HelixGatewayServiceOuterClass.ShardStateMessage.newBuilder()
            .setShardState(HelixGatewayServiceOuterClass.ShardState.newBuilder()
                .setInstanceName("instance1")
                .setClusterName("cluster1")
                .build())
            .build();

    // Mock a disconnection event
    HelixGatewayServiceOuterClass.ShardStateMessage disconnectionEvent =
        HelixGatewayServiceOuterClass.ShardStateMessage.newBuilder()
            .setShardState(HelixGatewayServiceOuterClass.ShardState.newBuilder()
                .setInstanceName("instance1")
                .setClusterName("cluster1")
                .build())
            .build();

    // Process connection event
    grpcService.report(null).onNext(connectionEvent);

    // Process disconnection event
    grpcService.report(null).onNext(disconnectionEvent);
    HelixGatewayService gatewayService = manager.getHelixGatewayService("cluster1");
    // Verify the events were processed in sequence
    verify(manager, times(2)).newGatewayServiceEvent(any());
  }
}
