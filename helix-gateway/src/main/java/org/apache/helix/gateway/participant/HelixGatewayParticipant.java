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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import org.apache.helix.gateway.statemodel.HelixGatewayMultiTopStateStateModelFactory;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.zookeeper.Watcher;

/**
 * HelixGatewayParticipant encapsulates the Helix Participant Manager and handles tracking the state
 * of a remote participant connected to the Helix Gateway Service. It processes state transitions
 * for the participant and updates the state of the participant's shards upon successful state
 * transitions signaled by remote participant.
 */
public class HelixGatewayParticipant implements HelixManagerStateListener, IdealStateChangeListener {
  public static final String UNASSIGNED_STATE = "UNASSIGNED";
  private final HelixGatewayServiceChannel _gatewayServiceChannel;
  private final HelixManager _helixManager;
  private final Runnable _onDisconnectedCallback;
  private final Map<String, Map<String, String>> _shardStateMap;
  private final Map<String, IdealState> _idealStateMap;
  private final Map<String, CompletableFuture<Boolean>> _stateTransitionResultMap;

  private HelixGatewayParticipant(HelixGatewayServiceChannel gatewayServiceChannel,
      Runnable onDisconnectedCallback, HelixManager helixManager,
      Map<String, Map<String, String>> initialShardStateMap) {
    _gatewayServiceChannel = gatewayServiceChannel;
    _helixManager = helixManager;
    _onDisconnectedCallback = onDisconnectedCallback;
    _shardStateMap = initialShardStateMap;
    _idealStateMap = new ConcurrentHashMap<>();
    _stateTransitionResultMap = new ConcurrentHashMap<>();
  }

  public void processStateTransitionMessage(Message message) throws Exception {
    String transitionId = message.getMsgId();
    String resourceId = message.getResourceName();
    String shardId = message.getPartitionName();

    try {
      // If the current state is already the target, the client does not need to
      // process the state transition.
      if (isGatewayCurrentStateAlreadyTargetState(resourceId, shardId)) {
        return;
      }

      // Create a new message with the target state instead of the actual toState
      Message targetStateMessage = new Message(message.getRecord());
      targetStateMessage.setToState(getTargetState(resourceId, shardId));

      CompletableFuture<Boolean> future = new CompletableFuture<>();
      _stateTransitionResultMap.put(transitionId, future);
      _gatewayServiceChannel.sendStateTransitionMessage(getInstanceName(),
          getCurrentState(resourceId, shardId), targetStateMessage);

      if (!future.get()) {
        throw new Exception("Failed to transition to state " + targetStateMessage.getToState());
      }

      updateGatewayCurrentState(resourceId, shardId, targetStateMessage.getToState());
    } finally {
      _stateTransitionResultMap.remove(transitionId);
    }
  }

  public void handleStateTransitionError(Message message, StateTransitionError error) {
    // Remove the stateTransitionResultMap future for the message
    String transitionId = message.getMsgId();
    String resourceId = message.getResourceName();
    String shardId = message.getPartitionName();

    // Remove the future from the stateTransitionResultMap since we are no longer able
    // to process the state transition due to participant manager either timing out
    // or failing to process the state transition
    _stateTransitionResultMap.remove(transitionId);

    // Set the replica state to ERROR
    updateGatewayCurrentState(resourceId, shardId, HelixDefinedState.ERROR.name());

    // Notify the HelixGatewayParticipantClient that it is in ERROR state
    // TODO: We need a better way than sending the state transition with a toState of ERROR
  }

  /**
   * Get the instance name of the participant.
   *
   * @return participant instance name
   */
  public String getInstanceName() {
    return _helixManager.getInstanceName();
  }

  /**
   * Completes the state transition with the given transitionId.
   *
   * @param transitionId the transitionId to complete
   * @param isSuccess    whether the state transition was successful
   */
  public void completeStateTransition(String transitionId, boolean isSuccess) {
    CompletableFuture<Boolean> future = _stateTransitionResultMap.get(transitionId);
    if (future != null) {
      future.complete(isSuccess);
    }
  }

  @VisibleForTesting
  Map<String, Map<String, String>> getShardStateMap() {
    return _shardStateMap;
  }

  /**
   * Get the current state of the shard.
   *
   * @param resourceId the resource id
   * @param shardId    the shard id
   * @return the current state of the shard or DROPPED if it does not exist
   */
  public String getCurrentState(String resourceId, String shardId) {
    return getShardStateMap().getOrDefault(resourceId, Collections.emptyMap())
        .getOrDefault(shardId, UNASSIGNED_STATE);
  }

  /**
   * Get the target state of the shard.
   *
   * @param resourceId the helix resources id
   * @param shardId    the shard id
   * @return the target state of the shard
   */
  private String getTargetState(String resourceId, String shardId) {
    // If it is not in the ideal state map, add it by getting the ideal state from helix
    // and add a listener to update the ideal state map when the ideal state changes
    if (!_idealStateMap.containsKey(resourceId)) {
      registerIdealStateListener(resourceId);
    }

    // If the ideal state is null, return DROPPED
    // If the instance is not in the ideal state map, return DROPPED
    // If the instance is in the ideal state map, return the target state
//    IdealState idealState = _idealStateMap.get(resourceId);
    IdealState idealState = _helixManager.getClusterManagmentTool()
        .getResourceIdealState(_helixManager.getClusterName(), resourceId);
    return idealState != null ? idealState.getInstanceStateMap(shardId)
        .getOrDefault(getInstanceName(), HelixDefinedState.DROPPED.name())
        : HelixDefinedState.DROPPED.name();
  }

  /**
   * Check if the gateway's current state is already the target state.
   *
   * @param resourceId the resource id
   * @param shardId    the shard id
   * @return true if the gateway's current state is already the target state, false otherwise
   */
  private boolean isGatewayCurrentStateAlreadyTargetState(String resourceId, String shardId) {
    String currentState = getCurrentState(resourceId, shardId);
    String targetState = getTargetState(resourceId, shardId);
    // If the targetState is DROPPED and the currentState is UNASSIGNED_STATE, then the
    // currentState is already the targetState. Otherwise, the currentState must equal the
    // targetState.
    return (targetState.equals(HelixDefinedState.DROPPED.name()) && currentState.equals(
        UNASSIGNED_STATE)) || currentState.equals(targetState);
  }

  /**
   * Update the gateways current state for the shard.
   *
   * @param resourceId the resource id
   * @param shardId    the shard id
   * @param state      the state to update to
   */
  private void updateGatewayCurrentState(String resourceId, String shardId, String state) {
    if (state.equals(HelixDefinedState.DROPPED.name())) {
      getShardStateMap().computeIfPresent(resourceId, (k, v) -> {
        v.remove(shardId);
        if (v.isEmpty()) {
          // Resource no longer has any shards assigned to the participant,
          // so we can stop listening for ideal state changes
          unregisterIdealStateListener(resourceId);
          return null;
        }
        return v;
      });
    } else {
      getShardStateMap().computeIfAbsent(resourceId, k -> new ConcurrentHashMap<>())
          .put(shardId, state);
    }
  }

  synchronized private void registerIdealStateListener(String resourceId) {
    IdealState idealState = _helixManager.getClusterManagmentTool()
        .getResourceIdealState(_helixManager.getClusterName(), resourceId);
    if (idealState != null) {
      _idealStateMap.put(resourceId, idealState);
      _helixManager.addListener(this,
          new PropertyKey.Builder(_helixManager.getClusterName()).idealStates(resourceId),
          HelixConstants.ChangeType.IDEAL_STATE,
          new Watcher.Event.EventType[]{Watcher.Event.EventType.NodeDataChanged});
    }
  }

  synchronized private void unregisterIdealStateListener(String resourceId) {
    _helixManager.removeListener(
        new PropertyKey.Builder(_helixManager.getClusterName()).idealStates(resourceId), this);
    // Also remove from the ideal state map
    _idealStateMap.remove(resourceId);
  }

  @Override
  @PreFetch(enabled = true)
  public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
    // Update the ideal state map with the new ideal states
    idealState.forEach(is -> _idealStateMap.put(is.getResourceName(), is));
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
    _gatewayServiceChannel.closeConnectionWithError(getInstanceName(),
        error.getMessage());
  }

  public void disconnect() {
    if (_helixManager.isConnected()) {
      _helixManager.disconnect();
    }
    _gatewayServiceChannel.completeConnection(getInstanceName());
  }

  public static class Builder {
    private final HelixGatewayServiceChannel _helixGatewayServiceChannel;
    private final String _instanceName;
    private final String _clusterName;
    private final String _zkAddress;
    private final Runnable _onDisconnectedCallback;
    private final List<String> _multiTopStateModelDefinitions;
    private final Map<String, Map<String, String>> _initialShardStateMap;

    public Builder(HelixGatewayServiceChannel helixGatewayServiceChannel, String instanceName,
        String clusterName, String zkAddress, Runnable onDisconnectedCallback) {
      _helixGatewayServiceChannel = helixGatewayServiceChannel;
      _instanceName = instanceName;
      _clusterName = clusterName;
      _zkAddress = zkAddress;
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
          new HelixGatewayParticipant(_helixGatewayServiceChannel, _onDisconnectedCallback,
              participantManager,
              _initialShardStateMap);
      _multiTopStateModelDefinitions.forEach(
          stateModelDefinition -> participantManager.getStateMachineEngine()
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
