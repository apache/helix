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
import org.apache.helix.gateway.constant.MessageType;
import org.apache.helix.gateway.service.GatewayServiceEvent;
import org.apache.helix.model.Message;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardState;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardTransitionStatus;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage;


public final class StateTransitionMessageTranslateUtil {

  /**
   * Translate from user sent MessageType to Helix Gateway Service TransitionType.
   *
   * @param messageType MessageType
   * @return TransitionType
   */
  public static HelixGatewayServiceOuterClass.SingleTransitionMessage.TransitionType translateMessageTypeToTransitionType(
      MessageType messageType) {
    switch (messageType) {
      case ADD:
        return HelixGatewayServiceOuterClass.SingleTransitionMessage.TransitionType.ADD_SHARD;
      case DELETE:
        return HelixGatewayServiceOuterClass.SingleTransitionMessage.TransitionType.DELETE_SHARD;
      case CHANGE_ROLE:
        return HelixGatewayServiceOuterClass.SingleTransitionMessage.TransitionType.CHANGE_ROLE;
      default:
        throw new IllegalArgumentException("Unknown message type: " + messageType);
    }
  }

  /**
   * Translate from user sent Message to Helix Gateway Service event.
   *
   * @param messageType MessageType
   * @param message     Message
   * @return TransitionMessage
   */
  public static TransitionMessage translateSTMsgToTransitionMessage(MessageType messageType,
      Message message) {
    return TransitionMessage.newBuilder().addRequest(
        HelixGatewayServiceOuterClass.SingleTransitionMessage.newBuilder()
            .setTransitionID(message.getMsgId())
            .setTransitionType(translateMessageTypeToTransitionType(messageType))
            .setResourceID(message.getResourceName()).setShardID(message.getPartitionName())
            .setTargetState(message.getToState()).build()).build();
  }

  /**
   * Translate from user sent ShardStateMessage message to Helix Gateway Service event.
   *
   * @param request ShardStateMessage message
   *                contains the state of each shard upon connection or result of state transition request.
   * @return GatewayServiceEvent
   */
  public static GatewayServiceEvent translateShardStateMessageToEvent(ShardStateMessage request) {
    GatewayServiceEvent.GateWayServiceEventBuilder builder;
    if (request.hasShardState()) { // init connection to gateway service
      ShardState shardState = request.getShardState();
      Map<String, Map<String, String>> shardStateMap = new HashMap<>();
      for (HelixGatewayServiceOuterClass.SingleResourceState resourceState : shardState.getResourceStateList()) {
        for (HelixGatewayServiceOuterClass.SingleShardState state : resourceState.getShardStatesList()) {
          shardStateMap.computeIfAbsent(resourceState.getResource(), k -> new HashMap<>())
              .put(state.getShardName(), state.getCurrentState());
        }
      }
      builder = new GatewayServiceEvent.GateWayServiceEventBuilder(GatewayServiceEventType.CONNECT).setClusterName(
              shardState.getClusterName()).setParticipantName(shardState.getInstanceName())
          .setShardStateMap(shardStateMap);
    } else {
      ShardTransitionStatus shardTransitionStatus = request.getShardTransitionStatus();
      // this is status update for established connection
      List<HelixGatewayServiceOuterClass.SingleShardTransitionStatus> status =
          shardTransitionStatus.getShardTransitionStatusList();
      List<GatewayServiceEvent.StateTransitionResult> stResult = new ArrayList<>();
      for (HelixGatewayServiceOuterClass.SingleShardTransitionStatus shardTransition : status) {
        GatewayServiceEvent.StateTransitionResult result =
            new GatewayServiceEvent.StateTransitionResult(shardTransition.getTransitionID(),
                shardTransition.getIsSuccess(), shardTransition.getCurrentState());
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
   * Translate from client close to Helix Gateway Service event.
   *
   * @param instanceName the instance name to send the message to
   * @param clusterName the cluster name
   * @return GatewayServiceEvent
   */

  public static GatewayServiceEvent translateClientCloseToEvent(String instanceName, String clusterName) {
    GatewayServiceEvent.GateWayServiceEventBuilder builder =
        new GatewayServiceEvent.GateWayServiceEventBuilder(GatewayServiceEventType.DISCONNECT).setClusterName(
            clusterName).setParticipantName(instanceName);
    return builder.build();
  }
}
