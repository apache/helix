package org.apache.helix.gateway.service;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;
import org.apache.helix.gateway.channel.HelixGatewayServiceGrpcService;
import org.testng.annotations.Test;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


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
  @Test
  public void testGetAllTargetStates() {
    GatewayServiceManager gatewayServiceManager = new GatewayServiceManager("localhost:2181");
    String clusterName = "TestCluster";
    String instanceName = "instance1";
    String resourceId = "resource1";
    String shardId = "shard1";
    String state = "ONLINE";

    // Add target state
    gatewayServiceManager.updateTargetState(clusterName, instanceName, resourceId, shardId, state);

    // Expected target states
    Map<String, Map<String, Map<String, String>>> expectedTargetStates = new HashMap<>();
    Map<String, Map<String, String>> instanceMap = new HashMap<>();
    Map<String, String> shardMap = new HashMap<>();
    shardMap.put(shardId, state);
    instanceMap.put(resourceId, shardMap);
    expectedTargetStates.put(instanceName, instanceMap);

    // Get all target states
    Map<String, Map<String, Map<String, String>>> actualTargetStates = gatewayServiceManager.getAllTargetStates(clusterName);

    // Verify the target states
    assertEquals(actualTargetStates, expectedTargetStates);
  }
}
