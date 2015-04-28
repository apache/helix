package org.apache.helix.participant;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.MapKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.State;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.messaging.handling.BatchMessageHandler;
import org.apache.helix.messaging.handling.BatchMessageWrapper;
import org.apache.helix.messaging.handling.HelixStateTransitionHandler;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.TaskExecutor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.log4j.Logger;

public class HelixStateMachineEngine implements StateMachineEngine {
  private static Logger LOG = Logger.getLogger(HelixStateMachineEngine.class);

  /**
   * Map of StateModelDefId to map of FactoryName to StateModelFactory
   */
  private final Map<StateModelDefId, Map<String, StateTransitionHandlerFactory<? extends TransitionHandler>>> _stateModelFactoryMap;
  private final StateModelParser _stateModelParser;
  private final HelixManager _manager;
  private final ConcurrentHashMap<StateModelDefId, StateModelDefinition> _stateModelDefs;

  public HelixStateMachineEngine(HelixManager manager) {
    _stateModelParser = new StateModelParser();
    _manager = manager;

    _stateModelFactoryMap =
        new ConcurrentHashMap<StateModelDefId, Map<String, StateTransitionHandlerFactory<? extends TransitionHandler>>>();
    _stateModelDefs = new ConcurrentHashMap<StateModelDefId, StateModelDefinition>();
  }

  public StateTransitionHandlerFactory<? extends TransitionHandler> getStateModelFactory(
      StateModelDefId stateModelName) {
    return getStateModelFactory(stateModelName, HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
  }

  public StateTransitionHandlerFactory<? extends TransitionHandler> getStateModelFactory(
      StateModelDefId stateModelName, String factoryName) {
    if (!_stateModelFactoryMap.containsKey(stateModelName)) {
      return null;
    }
    return _stateModelFactoryMap.get(stateModelName).get(factoryName);
  }

  // TODO: duplicated code in DefaultMessagingService
  private void sendNopMessage() {
    if (_manager.isConnected()) {
      try {
        Message nopMsg =
            new Message(MessageType.NO_OP, MessageId.from(UUID.randomUUID().toString()));
        nopMsg.setSrcName(_manager.getInstanceName());

        HelixDataAccessor accessor = _manager.getHelixDataAccessor();
        Builder keyBuilder = accessor.keyBuilder();

        if (_manager.getInstanceType() == InstanceType.CONTROLLER
            || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
          nopMsg.setTgtName("Controller");
          accessor.setProperty(keyBuilder.controllerMessage(nopMsg.getId()), nopMsg);
        }

        if (_manager.getInstanceType() == InstanceType.PARTICIPANT
            || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
          nopMsg.setTgtName(_manager.getInstanceName());
          accessor.setProperty(keyBuilder.message(nopMsg.getTgtName(), nopMsg.getId()), nopMsg);
        }
        LOG.info("Send NO_OP message to " + nopMsg.getTgtName() + ", msgId: " + nopMsg.getId());
      } catch (Exception e) {
        LOG.error(e);
      }
    }
  }

  @Override
  public void reset() {
    for (Map<String, StateTransitionHandlerFactory<? extends TransitionHandler>> ftyMap : _stateModelFactoryMap
        .values()) {
      for (StateTransitionHandlerFactory<? extends TransitionHandler> stateModelFactory : ftyMap
          .values()) {
        for (ResourceId resource : stateModelFactory.getResourceSet()) {
          for (PartitionId partition : stateModelFactory.getPartitionSet(resource)) {
            TransitionHandler stateModel =
                stateModelFactory.getTransitionHandler(resource, partition);
            stateModel.reset();
            String initialState = _stateModelParser.getInitialState(stateModel.getClass());
            stateModel.updateState(initialState);
            // TODO probably should update the state on ZK. Shi confirm what needs
            // to be done here.
          }
        }
      }
    }
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String type = message.getMsgType();

    if (!type.equals(MessageType.STATE_TRANSITION.toString())) {
      throw new HelixException("Expect state-transition message type, but was "
          + message.getMsgType() + ", msgId: " + message.getMessageId());
    }

    PartitionId partitionKey = message.getPartitionId();
    StateModelDefId stateModelId = message.getStateModelDefId();
    ResourceId resourceId = message.getResourceId();
    SessionId sessionId = message.getTypedTgtSessionId();
    int bucketSize = message.getBucketSize();

    if (stateModelId == null) {
      LOG.error("Fail to create msg-handler because message does not contain stateModelDef. msgId: "
          + message.getId());
      return null;
    }

    String factoryName = message.getStateModelFactoryName();
    if (factoryName == null) {
      factoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY;
    }

    StateTransitionHandlerFactory<? extends TransitionHandler> stateModelFactory =
        getStateModelFactory(stateModelId, factoryName);
    if (stateModelFactory == null) {
      LOG.warn("Fail to create msg-handler because cannot find stateModelFactory for model: "
          + stateModelId + " using factoryName: " + factoryName + " for resource: " + resourceId);
      return null;
    }

    // check if the state model definition exists and cache it
    if (!_stateModelDefs.containsKey(stateModelId)) {
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();
      StateModelDefinition stateModelDef =
          accessor.getProperty(keyBuilder.stateModelDef(stateModelId.stringify()));
      if (stateModelDef == null) {
        throw new HelixException("fail to create msg-handler because stateModelDef for "
            + stateModelId + " does NOT exist");
      }
      _stateModelDefs.put(stateModelId, stateModelDef);
    }

    if (message.getBatchMessageMode() == false) {
      // create currentStateDelta for this partition
      String initState = _stateModelDefs.get(message.getStateModelDefId()).getInitialState();
      TransitionHandler stateModel =
          stateModelFactory.getTransitionHandler(resourceId, partitionKey);
      if (stateModel == null) {
        stateModel = stateModelFactory.createAndAddSTransitionHandler(resourceId, partitionKey);
        stateModel.updateState(initState);
      }

      // TODO: move currentStateDelta to StateTransitionMsgHandler
      CurrentState currentStateDelta = new CurrentState(resourceId.stringify());
      currentStateDelta.setSessionId(sessionId);
      currentStateDelta.setStateModelDefRef(stateModelId.stringify());
      currentStateDelta.setStateModelFactoryName(factoryName);
      currentStateDelta.setBucketSize(bucketSize);

      currentStateDelta.setState(
          partitionKey,
          (stateModel.getCurrentState() == null) ? State.from(initState) : State.from(stateModel
              .getCurrentState()));

      return new HelixStateTransitionHandler(stateModelFactory, stateModel, message, context,
          currentStateDelta);
    } else {
      BatchMessageWrapper wrapper = stateModelFactory.getBatchMessageWrapper(resourceId);
      if (wrapper == null) {
        wrapper = stateModelFactory.createAndAddBatchMessageWrapper(resourceId);
      }

      // get executor-service for the message
      TaskExecutor executor = (TaskExecutor) context.get(MapKey.TASK_EXECUTOR.toString());
      if (executor == null) {
        LOG.error("fail to get executor-service for batch message: " + message.getId()
            + ". msgType: " + message.getMsgType() + ", resource: " + message.getResourceId());
        return null;
      }
      return new BatchMessageHandler(message, context, this, wrapper, executor);
    }
  }

  @Override
  public String getMessageType() {
    return MessageType.STATE_TRANSITION.toString();
  }

  @Override
  public boolean registerStateModelFactory(StateModelDefId stateModelDefId,
      StateTransitionHandlerFactory<? extends TransitionHandler> factory) {
    return registerStateModelFactory(stateModelDefId, HelixConstants.DEFAULT_STATE_MODEL_FACTORY,
        factory);
  }

  @Override
  public boolean registerStateModelFactory(StateModelDefId stateModelDefId, String factoryName,
      StateTransitionHandlerFactory<? extends TransitionHandler> factory) {
    if (stateModelDefId == null || factoryName == null || factory == null) {
      LOG.info("stateModelDefId|factoryName|stateModelFactory is null");
      return false;
    }

    LOG.info("Registering state model factory for state-model-definition: " + stateModelDefId
        + " using factory-name: " + factoryName + " with: " + factory);

    if (!_stateModelFactoryMap.containsKey(stateModelDefId)) {
      _stateModelFactoryMap
          .put(
              stateModelDefId,
              new ConcurrentHashMap<String, StateTransitionHandlerFactory<? extends TransitionHandler>>());
    }

    if (_stateModelFactoryMap.get(stateModelDefId).containsKey(factoryName)) {
      LOG.info("Skip register state model factory for " + stateModelDefId + " using factory-name "
          + factoryName + ", since it has already been registered.");
      return false;
    }

    _stateModelFactoryMap.get(stateModelDefId).put(factoryName, factory);

    sendNopMessage();
    return true;
  }

  @Override
  public boolean removeStateModelFactory(StateModelDefId stateModelDefId) {
    return removeStateModelFactory(stateModelDefId, HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
  }

  @Override
  public boolean removeStateModelFactory(StateModelDefId stateModelDefId, String factoryName) {
    if (stateModelDefId == null || factoryName == null) {
      LOG.info("stateModelDefId|factoryName is null");
      return false;
    }

    LOG.info("Removing state model factory for state-model-definition: " + stateModelDefId
        + " using factory-name: " + factoryName);

    Map<String, StateTransitionHandlerFactory<? extends TransitionHandler>> ftyMap =
        _stateModelFactoryMap.get(stateModelDefId);
    if (ftyMap == null) {
      LOG.info("Skip remove state model factory " + stateModelDefId + ", since it does NOT exist");
      return false;
    }

    StateTransitionHandlerFactory<? extends TransitionHandler> fty = ftyMap.remove(factoryName);
    if (fty == null) {
      LOG.info("Skip remove state model factory " + stateModelDefId + " using factory-name "
          + factoryName + ", since it does NOT exist");
      return false;
    }

    if (ftyMap.isEmpty()) {
      _stateModelFactoryMap.remove(stateModelDefId);
    }

    for (ResourceId resource : fty.getResourceSet()) {
      for (PartitionId partition : fty.getPartitionSet(resource)) {
        TransitionHandler stateModel = fty.getTransitionHandler(resource, partition);
        stateModel.reset();
        // TODO probably should remove the state from zookeeper
      }
    }

    return true;
  }
}
