package org.apache.helix.gateway.channel;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.helix.gateway.api.constant.GatewayServiceEventType;
import org.apache.helix.gateway.service.GatewayServiceEvent;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.junit.Assert;
import org.testng.annotations.Test;

import static org.apache.helix.gateway.channel.GatewayServiceChannelConfig.ChannelMode.*;
import static org.mockito.Mockito.*;


public class TestHelixGatewayServicePollModeChannel {
  private HelixGatewayServicePollModeChannel pollModeChannel;
  private GatewayServiceManager manager;
  private ScheduledExecutorService scheduler;

  int connectEventCount = 0;
  int disconnectEventCount = 0;
  int updateEventCount = 0;

  @Test
  public void testFetchUpdates() {
    scheduler = mock(ScheduledExecutorService.class);
    GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder builder =
        new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder().setChannelMode(POLL_MODE)
            .setHealthCheckEndpointMap(Map.of("cluster1", Map.of("instance1", "endpoint1")))
            .setParticipantConnectionChannelType(GatewayServiceChannelConfig.ChannelType.FILE)
            .setShardStateProcessorType(GatewayServiceChannelConfig.ChannelType.FILE)
            .addPollModeConfig(GatewayServiceChannelConfig.FileBasedConfigType.PARTICIPANT_CURRENT_STATE_PATH,
                "CurrentStatePath")
            .addPollModeConfig(GatewayServiceChannelConfig.FileBasedConfigType.SHARD_TARGET_STATE_PATH,
                "shardTargetStatePath")
            .setPollIntervalSec(60 * 1000) // set a larger number to avoid recurrent polling
            .setPollStartDelaySec(60 * 1000)
            .setTargetFileUpdateIntervalSec(60 * 1000);

    manager = new DummyGatewayServiceManager(builder.build());
    pollModeChannel = spy(new HelixGatewayServicePollModeChannel(manager, builder.build()));
    pollModeChannel._scheduler = scheduler; // Inject the mocked scheduler

    // Mock the necessary methods and data
    doReturn(true).when(pollModeChannel).fetchInstanceLivenessStatus("cluster1", "instance1");
    Map<String, Map<String, Map<String, Map<String, String>>>> currentStateMap =
        Map.of("cluster1", Map.of("instance1", Map.of("resource1", Map.of("shard", "ONLINE"))));
    doReturn(currentStateMap).when(pollModeChannel).getChangedParticipantsCurrentState(any());

    // 1. Call fetch update for first time, verify we got a init connect event
    pollModeChannel.fetchUpdates();

    Assert.assertEquals(1, connectEventCount);

    // 2. Change currentStateMap, Call fetch update for second time, verify we got an update event
    Map<String, Map<String, Map<String, Map<String, String>>>> currentStateMap2 =
        Map.of("cluster1", Map.of("instance1", Map.of("resource1", Map.of("shard", "OFFLINE"))));
    doReturn(currentStateMap2).when(pollModeChannel).getChangedParticipantsCurrentState(any());
    pollModeChannel.fetchUpdates();
    Assert.assertEquals(1, updateEventCount);

    // call pne more time with same shard state, verify no new event
    pollModeChannel.fetchUpdates();
    Assert.assertEquals(1, updateEventCount);

    // 3. Change health check result, Call fetch update for third time, verify we got a disconnect event
    doReturn(false).when(pollModeChannel).fetchInstanceLivenessStatus("cluster1", "instance1");
    pollModeChannel.fetchUpdates();
    Assert.assertEquals(1, disconnectEventCount);
  }

  class DummyGatewayServiceManager extends GatewayServiceManager {

    public DummyGatewayServiceManager(GatewayServiceChannelConfig gatewayServiceChannelConfig) {
      super("dummyZkAddress", gatewayServiceChannelConfig);
    }

    @Override
    public void onGatewayServiceEvent(GatewayServiceEvent event) {
      if (event.getEventType().equals(GatewayServiceEventType.CONNECT)) {
        connectEventCount++;
      } else if (event.getEventType().equals(GatewayServiceEventType.DISCONNECT)) {
        disconnectEventCount++;
      } else if (event.getEventType().equals(GatewayServiceEventType.UPDATE)) {
        updateEventCount++;
      }
      System.out.println("Received event: " + event.getEventType());
    }
  }
}
