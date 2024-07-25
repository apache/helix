package org.apache.helix.gateway.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.gateway.constant.GatewayServiceEventType;
import org.apache.helix.gateway.service.GatewayServiceEvent;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardState;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardTransitionStatus;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage;


public final class StateTransitionMessageTranslateUtil {

  public static TransitionMessage translateSTMsgToTransitionMessage() {
    return null;
  }

  /**
   * Translate from user sent ShardStateMessage message to Helix Gateway Service event.
   */
  public static GatewayServiceEvent translateShardStateMessageToEvent(ShardStateMessage request) {

    GatewayServiceEvent.GateWayServiceEventBuilder builder;
    if (request.hasShardState()) { // init connection to gateway service
      ShardState shardState = request.getShardState();
      Map<String, String> shardStateMap = new HashMap<>();
      for (HelixGatewayServiceOuterClass.SingleResourceState resourceState : shardState.getResourceStateList()) {
        for (HelixGatewayServiceOuterClass.SingleShardState state : resourceState.getShardStatesList()) {
          shardStateMap.put(resourceState.getResource() + "_" + state.getShardName(), state.getCurrentState());
        }
      }
      builder = new GatewayServiceEvent.GateWayServiceEventBuilder(GatewayServiceEventType.CONNECT).setClusterName(
          shardState.getClusterName()).setParticipantName(shardState.getInstanceName());
    } else {
      ShardTransitionStatus shardTransitionStatus = request.getShardTransitionStatus();
      // this is status update for established connection
      List<HelixGatewayServiceOuterClass.SingleShardTransitionStatus> status =
          shardTransitionStatus.getShardTransitionStatusList();
      List<GatewayServiceEvent.StateTransitionResult> stResult = new ArrayList<>();
      for (HelixGatewayServiceOuterClass.SingleShardTransitionStatus shardTransition : status) {
        String stateTransitionId = shardTransition.getTransitionID();
        String stateTransitionStatus = shardTransition.getCurrentState();
        String shardState = shardTransition.getCurrentState();
        GatewayServiceEvent.StateTransitionResult result =
            new GatewayServiceEvent.StateTransitionResult(stateTransitionId, stateTransitionStatus, shardState);
        stResult.add(result);
      }
      builder = new GatewayServiceEvent.GateWayServiceEventBuilder(GatewayServiceEventType.UPDATE).setClusterName(
              shardTransitionStatus.getClusterName())
          .setParticipantName(shardTransitionStatus.getInstanceName())
          .setStateTransitionStatusMap(stResult);
    }
    return builder.build();
  }

  /**
   * Translate termination event to GatewayServiceEvent.
   */

  public static GatewayServiceEvent translateClientCloseToEvent(String instanceName, String clusterName) {
    GatewayServiceEvent.GateWayServiceEventBuilder builder =
        new GatewayServiceEvent.GateWayServiceEventBuilder(GatewayServiceEventType.DISCONNECT).setClusterName(
            clusterName).setParticipantName(instanceName);
    return builder.build();
  }
}
