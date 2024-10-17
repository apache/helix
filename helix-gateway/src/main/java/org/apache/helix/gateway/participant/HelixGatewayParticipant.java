package org.apache.helix.gateway.participant;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import org.apache.helix.gateway.channel.HelixGatewayServicePollModeChannel;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.statemodel.HelixGatewayMultiTopStateStateModelFactory;
import org.apache.helix.gateway.util.StateTransitionMessageTranslateUtil;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HelixGatewayParticipant encapsulates the Helix Participant Manager and handles tracking the state
 * of a remote participant connected to the Helix Gateway Service. It processes state transitions
 * for the participant and updates the state of the participant's shards upon successful state
 * transitions signaled by remote participant.
 */
public class HelixGatewayParticipant implements HelixManagerStateListener {
  private static final Logger logger = LoggerFactory.getLogger(HelixGatewayParticipant.class);
  public static final String UNASSIGNED_STATE = "UNASSIGNED";
  private final HelixGatewayServiceChannel _gatewayServiceChannel;
  private final HelixManager _helixManager;
  private final Runnable _onDisconnectedCallback;
  private final Map<String, CompletableFuture<String>> _stateTransitionResultMap;

  private final GatewayServiceManager _gatewayServiceManager;

  private HelixGatewayParticipant(HelixGatewayServiceChannel gatewayServiceChannel, Runnable onDisconnectedCallback,
      HelixManager helixManager, Map<String, Map<String, String>> initialShardStateMap,
      GatewayServiceManager gatewayServiceManager) {
    _gatewayServiceChannel = gatewayServiceChannel;
    _helixManager = helixManager;
    _onDisconnectedCallback = onDisconnectedCallback;
    _stateTransitionResultMap = new ConcurrentHashMap<>();
    _gatewayServiceManager = gatewayServiceManager;
  }

  public void processStateTransitionMessage(Message message) throws Exception {
    String resourceId = message.getResourceName();
    String shardId = message.getPartitionName();
    String toState = message.getToState();
    String concatenatedShardName = resourceId + shardId;

    try {
      // update the target state in cache
      _gatewayServiceManager.updateTargetState(_helixManager.getClusterName(), _helixManager.getInstanceName(),
          resourceId, shardId, toState);

      if (isCurrentStateAlreadyTarget(resourceId, shardId, toState)) {
        return;
      }
      CompletableFuture<String> future = new CompletableFuture<>();
      _stateTransitionResultMap.put(concatenatedShardName, future);
      _gatewayServiceChannel.sendStateChangeRequests(_helixManager.getInstanceName(),
          StateTransitionMessageTranslateUtil.translateSTMsgToShardChangeRequests(message));

      if (!toState.equals(future.get())) {
        throw new Exception("Failed to transition to state " + toState);
      }
    } finally {
      logger.info("State transition finished for shard: {}{}", resourceId, shardId);
      _stateTransitionResultMap.remove(concatenatedShardName);
    }
  }

  public void handleStateTransitionError(Message message, StateTransitionError error) {
    // Remove the stateTransitionResultMap future for the message
    String transitionId = message.getMsgId();

    // Remove the future from the stateTransitionResultMap since we are no longer able
    // to process the state transition due to participant manager either timing out
    // or failing to process the state transition
    _stateTransitionResultMap.remove(transitionId);

    // Notify the HelixGatewayParticipantClient that it is in ERROR state
    // TODO: We need a better way than sending the state transition with a toState of ERROR
  }

  /**
   * Get the instance name of the participant.
   * @return participant instance name
   */
  public String getInstanceName() {
    return _helixManager.getInstanceName();
  }

  /**
   * Completes the state transition with the given transitionId.
   */
  public void completeStateTransition(String resourceId, String shardId, String currentState) {
    logger.info("Completing state transition for shard: {}{} to state: {}", resourceId, shardId, currentState);
    String concatenatedShardName = resourceId + shardId;
    CompletableFuture<String> future = _stateTransitionResultMap.get(concatenatedShardName);
    if (future != null) {
      future.complete(currentState);
    }
  }

  private boolean isCurrentStateAlreadyTarget(String resourceId, String shardId, String targetState) {
    return getCurrentState(resourceId, shardId).equals(targetState);
  }

  /**
   * Get the current state of the shard.
   *
   * @param resourceId the resource id
   * @param shardId    the shard id
   * @return the current state of the shard or DROPPED if it does not exist
   */
  public String getCurrentState(String resourceId, String shardId) {
    String currentState =
        _gatewayServiceManager.getCurrentState(_helixManager.getClusterName(), _helixManager.getInstanceName(),
            resourceId, shardId);
    return currentState == null ? UNASSIGNED_STATE : currentState;
  }

  /**
   * Invoked when the HelixManager connection to zookeeper is established
   *
   * @param helixManager HelixManager that is successfully connected
   */
  public void onConnected(HelixManager helixManager) throws Exception {
    // Do nothing
  }

  /**
   * Invoked when the HelixManager connection to zookeeper is closed unexpectedly. This will not be
   * run if the remote participant disconnects from gateway.
   *
   * @param helixManager HelixManager that fails to be connected
   * @param error        connection error
   */
  @Override
  public void onDisconnected(HelixManager helixManager, Throwable error) throws Exception {
    _onDisconnectedCallback.run();
    _gatewayServiceChannel.closeConnectionWithError(_helixManager.getClusterName(), _helixManager.getInstanceName(),
        error.getMessage());
  }

  public void disconnect() {
    logger.info("Disconnecting from HelixManager {}", _helixManager.getInstanceName() );
    if (_helixManager.isConnected()) {
      _helixManager.disconnect();
    }
    _gatewayServiceChannel.completeConnection(_helixManager.getClusterName(), _helixManager.getInstanceName());
  }

  public static class Builder {
    private final HelixGatewayServiceChannel _helixGatewayServiceChannel;
    private final String _instanceName;
    private final String _clusterName;
    private final String _zkAddress;
    private final Runnable _onDisconnectedCallback;
    private final List<String> _multiTopStateModelDefinitions;
    private final Map<String, Map<String, String>> _initialShardStateMap;
    private final GatewayServiceManager _gatewayServiceManager;

    public Builder(HelixGatewayServiceChannel helixGatewayServiceChannel, String instanceName, String clusterName,
        String zkAddress, Runnable onDisconnectedCallback, GatewayServiceManager gatewayServiceManager) {
      _helixGatewayServiceChannel = helixGatewayServiceChannel;
      _instanceName = instanceName;
      _clusterName = clusterName;
      _zkAddress = zkAddress;
      _gatewayServiceManager = gatewayServiceManager;
      _onDisconnectedCallback = onDisconnectedCallback;
      _multiTopStateModelDefinitions = new ArrayList<>();
      _initialShardStateMap = new ConcurrentHashMap<>();
    }

    /**
     * Add a multi-top state model definition to the participant to be registered in the
     * participant's state machine engine.
     *
     * @param stateModelDefinitionName the state model definition name to add (should be multi-top
     *                                 state model)
     * @return the builder
     */
    public Builder addMultiTopStateStateModelDefinition(String stateModelDefinitionName) {
      // TODO: Add validation that the state model definition is a multi-top state model
      _multiTopStateModelDefinitions.add(stateModelDefinitionName);
      return this;
    }

    /**
     * Add initial shard state to the participant. This is used to initialize the participant with
     * the initial state of the shards in order to reduce unnecessary state transitions from being
     * forwarded to the participant.
     *
     * @param initialShardStateMap the initial shard state map to add
     * @return the Builder
     */
    public Builder setInitialShardState(Map<String, Map<String, String>> initialShardStateMap) {
      // TODO: Add handling for shard states that where never assigned to the participant since
      //  the participant was last online.
      // deep copy into the initialShardStateMap into concurrent hash map
      initialShardStateMap.forEach((resourceId, shardStateMap) -> {
        _initialShardStateMap.put(resourceId, new ConcurrentHashMap<>(shardStateMap));
      });

      return this;
    }

    /**
     * Build the HelixGatewayParticipant. This will create a HelixManager for the participant and
     * connect to the Helix cluster. The participant will be registered with the multi-top state
     * model definitions and initialized with the initial shard state map.
     *
     * @return the HelixGatewayParticipant
     */
    public HelixGatewayParticipant build() {
      HelixManager participantManager =
          new ZKHelixManager(_clusterName, _instanceName, InstanceType.PARTICIPANT, _zkAddress);
      HelixGatewayParticipant participant =
          new HelixGatewayParticipant(_helixGatewayServiceChannel, _onDisconnectedCallback, participantManager,
              _initialShardStateMap, _gatewayServiceManager);
      _multiTopStateModelDefinitions.forEach(stateModelDefinition -> participantManager.getStateMachineEngine()
          .registerStateModelFactory(stateModelDefinition,
              new HelixGatewayMultiTopStateStateModelFactory(participant)));
      try {
        participantManager.connect();
      } catch (Exception e) {
        // TODO: When API for gracefully triggering disconnect from remote participant
        //  is available, we should call it here instead of throwing exception.
        throw new RuntimeException(e);
      }
      return participant;
    }
  }
}
