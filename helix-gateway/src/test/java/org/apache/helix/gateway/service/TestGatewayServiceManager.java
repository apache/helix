package org.apache.helix.gateway.service;

import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;
import org.apache.helix.gateway.channel.HelixGatewayServiceGrpcService;
import org.testng.annotations.Test;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public class TestGatewayServiceManager {

  private GatewayServiceManager manager;

  @Test
  public void testConnectionAndDisconnectionEvents() {

    manager = mock(GatewayServiceManager.class);
    GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder builder = new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder().setGrpcServerPort(50051);
    HelixGatewayServiceGrpcService grpcService = new HelixGatewayServiceGrpcService(manager,builder.build());
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
    // Verify the events were processed in sequence
    verify(manager, times(2)).onGatewayServiceEvent(any());

    grpcService.stop();
  }
}
