package org.apache.helix.gateway.participant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.gateway.api.participant.HelixGatewayMultiTopStateStateTransitionProcessor;
import org.apache.helix.gateway.api.service.HelixGatewayServiceProcessor;
import org.apache.helix.gateway.constant.MessageType;
import org.apache.helix.gateway.statemodel.HelixGatewayMultiTopStateStateModelFactory;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateTransitionError;

public class HelixGatewayParticipant implements HelixGatewayMultiTopStateStateTransitionProcessor {
  private final HelixGatewayServiceProcessor _gatewayServiceProcessor;
  private final HelixManager _participantManager;
  private final Map<String, Map<String, String>> _shardStateMap;
  private final Map<String, CompletableFuture<Boolean>> _stateTransitionResultMap;

  private HelixGatewayParticipant(HelixGatewayServiceProcessor gatewayServiceProcessor,
      HelixManager participantManager, Map<String, Map<String, String>> initialShardStateMap) {
    _gatewayServiceProcessor = gatewayServiceProcessor;
    _participantManager = participantManager;
    _shardStateMap = initialShardStateMap;
    _stateTransitionResultMap = new ConcurrentHashMap<>();
  }

  @Override
  public void processMultiTopStateModelStateTransitionMessage(Message message) throws Exception {
    String transitionId = message.getMsgId();
    String resourceId = message.getResourceName();
    String shardId = message.getPartitionName();
    String toState = message.getToState();

    try {
      if (isCurrentStateAlreadyTarget(resourceId, shardId, toState)) {
        return;
      }

      CompletableFuture<Boolean> future = new CompletableFuture<>();
      _stateTransitionResultMap.put(transitionId, future);
      _gatewayServiceProcessor.sendStateTransitionMessage(
          _participantManager.getInstanceName(),
          determineTransitionType(resourceId, shardId, toState), message);

      boolean success = future.get();
      if (!success) {
        throw new Exception("Failed to transition to state " + toState);
      }

      updateState(resourceId, shardId, toState);
    } finally {
      _stateTransitionResultMap.remove(transitionId);
    }
  }

  @Override
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
    updateState(resourceId, shardId, HelixDefinedState.ERROR.name());

    // Notify the HelixGatewayParticipantClient that it is in ERROR state
    // TODO: We need a better way than sending the state transition with a toState of ERROR
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

  private boolean isCurrentStateAlreadyTarget(String resourceId, String shardId,
      String targetState) {
    return (_shardStateMap.containsKey(resourceId) && _shardStateMap.get(resourceId)
        .containsKey(shardId) && _shardStateMap.get(resourceId).get(shardId).equals(targetState))
        || targetState.equals(HelixDefinedState.DROPPED.name());
  }

  private MessageType determineTransitionType(String resourceId, String shardId,
      String toState) {
    boolean containsShard = _shardStateMap.containsKey(resourceId) && _shardStateMap.get(resourceId)
        .containsKey(shardId);
    if (toState.equals(HelixDefinedState.DROPPED.name())) {
      return containsShard ? MessageType.DELETE : MessageType.ADD;
    } else {
      return containsShard ? MessageType.CHANGE_ROLE : MessageType.ADD;
    }
  }

  private void updateState(String resourceId, String shardId, String state) {
    if (state.equals(HelixDefinedState.DROPPED.name())) {
      _shardStateMap.computeIfPresent(resourceId, (k, v) -> {
        v.remove(shardId);
        if (v.isEmpty()) {
          return null;
        }
        return v;
      });
    } else {
      _shardStateMap.computeIfAbsent(resourceId, k -> new ConcurrentHashMap<>())
          .put(shardId, state);
    }
  }

  public void disconnect() {
    _participantManager.disconnect();
  }

  public static class Builder {
    private final HelixGatewayServiceProcessor _helixGatewayServiceProcessor;
    private final String _instanceName;
    private final String _clusterName;
    private final String _zkAddress;
    private final List<String> _multiTopStateModelDefinitions;
    private final Map<String, Map<String, String>> _initialShardStateMap;

    public Builder(HelixGatewayServiceProcessor helixGatewayServiceProcessor, String instanceName,
        String clusterName, String zkAddress) {
      _helixGatewayServiceProcessor = helixGatewayServiceProcessor;
      _instanceName = instanceName;
      _clusterName = clusterName;
      _zkAddress = zkAddress;
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
      // TODO: Add validation that thi state model definition is a multi-top state model
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
    public Builder addInitialShardState(Map<String, Map<String, String>> initialShardStateMap) {
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
          new HelixGatewayParticipant(_helixGatewayServiceProcessor, participantManager,
              _initialShardStateMap);
      _multiTopStateModelDefinitions.forEach(
          stateModelDefinition -> participantManager.getStateMachineEngine()
              .registerStateModelFactory(stateModelDefinition,
                  new HelixGatewayMultiTopStateStateModelFactory(participant)));
      try {
        participantManager.connect();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return participant;
    }
  }
}
