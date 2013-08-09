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
import org.apache.helix.messaging.handling.BatchMessageHandler;
import org.apache.helix.messaging.handling.BatchMessageWrapper;
import org.apache.helix.messaging.handling.HelixStateTransitionHandler;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.TaskExecutor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.log4j.Logger;


public class HelixStateMachineEngine implements StateMachineEngine
{
  private static Logger logger = Logger.getLogger(HelixStateMachineEngine.class);

  // StateModelName->FactoryName->StateModelFactory
  private final Map<String, Map<String, StateModelFactory<? extends StateModel>>> _stateModelFactoryMap;
  private final StateModelParser _stateModelParser;
  private final HelixManager _manager;
  private final ConcurrentHashMap<String, StateModelDefinition> _stateModelDefs;

  public HelixStateMachineEngine(HelixManager manager)
  {
    _stateModelParser = new StateModelParser();
    _manager = manager;

    _stateModelFactoryMap =
        new ConcurrentHashMap<String, Map<String, StateModelFactory<? extends StateModel>>>();
    _stateModelDefs = new ConcurrentHashMap<String, StateModelDefinition>();
  }

  public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName)
  {
    return getStateModelFactory(stateModelName,
                                HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
  }

  public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName,
                                                                      String factoryName)
  {
    if (!_stateModelFactoryMap.containsKey(stateModelName))
    {
      return null;
    }
    return _stateModelFactoryMap.get(stateModelName).get(factoryName);
  }

  @Override
  public boolean registerStateModelFactory(String stateModelDef,
                                           StateModelFactory<? extends StateModel> factory)
  {
    return registerStateModelFactory(stateModelDef,
                                     factory,
                                     HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
  }

  @Override
  public boolean registerStateModelFactory(String stateModelName,
                                           StateModelFactory<? extends StateModel> factory,
                                           String factoryName)
  {
    if (stateModelName == null || factory == null || factoryName == null)
    {
      throw new HelixException("stateModelDef|stateModelFactory|factoryName cannot be null");
    }

    logger.info("Register state model factory for state model " + stateModelName
        + " using factory name " + factoryName + " with " + factory);

    if (!_stateModelFactoryMap.containsKey(stateModelName))
    {
      _stateModelFactoryMap.put(stateModelName,
                                new ConcurrentHashMap<String, StateModelFactory<? extends StateModel>>());
    }

    if (_stateModelFactoryMap.get(stateModelName).containsKey(factoryName))
    {
      logger.warn("stateModelFactory for " + stateModelName + " using factoryName "
          + factoryName + " has already been registered.");
      return false;
    }

    _stateModelFactoryMap.get(stateModelName).put(factoryName, factory);
    sendNopMessage();
    return true;
  }

  // TODO: duplicated code in DefaultMessagingService
  private void sendNopMessage()
  {
    if (_manager.isConnected())
    {
      try
      {
        Message nopMsg = new Message(MessageType.NO_OP, UUID.randomUUID().toString());
        nopMsg.setSrcName(_manager.getInstanceName());

        HelixDataAccessor accessor = _manager.getHelixDataAccessor();
        Builder keyBuilder = accessor.keyBuilder();

        if (_manager.getInstanceType() == InstanceType.CONTROLLER
            || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT)
        {
          nopMsg.setTgtName("Controller");
          accessor.setProperty(keyBuilder.controllerMessage(nopMsg.getId()), nopMsg);
        }

        if (_manager.getInstanceType() == InstanceType.PARTICIPANT
            || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT)
        {
          nopMsg.setTgtName(_manager.getInstanceName());
          accessor.setProperty(keyBuilder.message(nopMsg.getTgtName(), nopMsg.getId()),
                               nopMsg);
        }
        logger.info("Send NO_OP message to " + nopMsg.getTgtName() + ", msgId: "
            + nopMsg.getId());
      }
      catch (Exception e)
      {
        logger.error(e);
      }
    }
  }

  @Override
  public void reset()
  {
    for (Map<String, StateModelFactory<? extends StateModel>> ftyMap : _stateModelFactoryMap.values())
    {
      for (StateModelFactory<? extends StateModel> stateModelFactory : ftyMap.values())
      {
        for (String resourceKey : stateModelFactory.getPartitionSet())
        {
          StateModel stateModel = stateModelFactory.getStateModel(resourceKey);
          stateModel.reset();
          String initialState = _stateModelParser.getInitialState(stateModel.getClass());
          stateModel.updateState(initialState);
          // TODO probably should update the state on ZK. Shi confirm what needs
          // to be done here.
        }
      }
    }
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context)
  {
    String type = message.getMsgType();

    if (!type.equals(MessageType.STATE_TRANSITION.toString()))
    {
      throw new HelixException("Expect state-transition message type, but was "
    		  + message.getMsgType() + ", msgId: " + message.getMsgId());
    }

    String partitionKey = message.getPartitionName();
    String stateModelName = message.getStateModelDef();
    String resourceName = message.getResourceName();
    String sessionId = message.getTgtSessionId();
    int bucketSize = message.getBucketSize();

    if (stateModelName == null)
    {
      logger.error("Fail to create msg-handler because message does not contain stateModelDef. msgId: " + message.getId());
      return null;
    }

    String factoryName = message.getStateModelFactoryName();
    if (factoryName == null)
    {
      factoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY;
    }

    StateModelFactory<? extends StateModel> stateModelFactory =
        getStateModelFactory(stateModelName, factoryName);
    if (stateModelFactory == null)
    {
      logger.warn("Fail to create msg-handler because cannot find stateModelFactory for model: " + stateModelName
          + " using factoryName: " + factoryName + " for resource: " + resourceName);
      return null;
    }

    // check if the state model definition exists and cache it
    if (!_stateModelDefs.containsKey(stateModelName))
    {
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();
      StateModelDefinition stateModelDef =
          accessor.getProperty(keyBuilder.stateModelDef(stateModelName));
      if (stateModelDef == null)
      {
        throw new HelixException("fail to create msg-handler because stateModelDef for " + stateModelName
            + " does NOT exist");
      }
      _stateModelDefs.put(stateModelName, stateModelDef);
    }

    if (message.getBatchMessageMode() == false) {
        // create currentStateDelta for this partition
        String initState = _stateModelDefs.get(message.getStateModelDef()).getInitialState();
        StateModel stateModel = stateModelFactory.getStateModel(partitionKey);
        if (stateModel == null)
        {
          stateModel = stateModelFactory.createAndAddStateModel(partitionKey);
          stateModel.updateState(initState);
        }

        // TODO: move currentStateDelta to StateTransitionMsgHandler
        CurrentState currentStateDelta = new CurrentState(resourceName);
        currentStateDelta.setSessionId(sessionId);
        currentStateDelta.setStateModelDefRef(stateModelName);
        currentStateDelta.setStateModelFactoryName(factoryName);
        currentStateDelta.setBucketSize(bucketSize);

        currentStateDelta.setState(partitionKey, (stateModel.getCurrentState() == null)
            ? initState : stateModel.getCurrentState());

        return new HelixStateTransitionHandler(stateModelFactory,
                                               stateModel,
                                               message,
                                               context,
                                               currentStateDelta);
    } else
    {
      BatchMessageWrapper wrapper = stateModelFactory.getBatchMessageWrapper(resourceName);
      if (wrapper == null)
      {
        wrapper = stateModelFactory.createAndAddBatchMessageWrapper(resourceName);
      }

    	// get executor-service for the message
    	TaskExecutor executor = (TaskExecutor) context.get(MapKey.TASK_EXECUTOR.toString());
    	if (executor == null)
    	{
    		logger.error("fail to get executor-service for batch message: " + message.getId()
    				+ ". msgType: " + message.getMsgType() + ", resource: " + message.getResourceName());
    		return null;
    	}
    	return new BatchMessageHandler(message, context, this, wrapper, executor);
    }
  }

  @Override
  public String getMessageType()
  {
    return MessageType.STATE_TRANSITION.toString();
  }

  @Override
  public boolean removeStateModelFactory(String stateModelDef,
                                         StateModelFactory<? extends StateModel> factory)
  {
    throw new UnsupportedOperationException("Remove not yet supported");
  }

  @Override
  public boolean removeStateModelFactory(String stateModelDef,
                                         StateModelFactory<? extends StateModel> factory,
                                         String factoryName)
  {
    throw new UnsupportedOperationException("Remove not yet supported");
  }
}
