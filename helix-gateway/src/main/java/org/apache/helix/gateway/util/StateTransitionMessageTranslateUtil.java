package org.apache.helix.gateway.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
        GatewayServiceEvent.StateTransitionResult result =
            new GatewayServiceEvent.StateTransitionResult(shardTransition.getTransitionID(),
                shardTransition.getCurrentState(), shardTransition.getCurrentState());
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
