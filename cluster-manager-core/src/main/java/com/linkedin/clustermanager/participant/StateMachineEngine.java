package com.linkedin.clustermanager.participant;

import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.messaging.handling.CMStateTransitionHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;
import com.linkedin.clustermanager.participant.statemachine.StateModelParser;

public class StateMachineEngine<T extends StateModel> implements
    MessageHandlerFactory
{
  private static Logger logger = Logger.getLogger(StateMachineEngine.class);
  private final StateModelFactory<T> _stateModelFactory;
  StateModelParser _stateModelParser;

  public StateModelFactory<T> getStateModelFactory()
  {
    return _stateModelFactory;
  }

  public StateMachineEngine(StateModelFactory<T> factory)
  {
    this._stateModelFactory = factory;
    _stateModelParser = new StateModelParser();
  }

  public void reset()
  {
    ConcurrentMap<String, T> modelMap = _stateModelFactory.getStateModelMap();
    if (modelMap == null || modelMap.isEmpty())
    {
      return;
    }
    for (String resourceKey : modelMap.keySet())
    {
      StateModel stateModel = modelMap.get(resourceKey);
      stateModel.reset();
      String initialState = _stateModelParser.getInitialState(stateModel
          .getClass());
      stateModel.updateState(initialState);
      // todo probably should update the state on ZK. Shi confirm what needs to
      // be done here.
    }
  }

  @Override
  public MessageHandler createHandler(Message message,
      NotificationContext context)
  {
    String type = message.getMsgType();
    
    if(!type.equals(MessageType.STATE_TRANSITION.toString()))
    {
      throw new ClusterManagerException("Unexpected msg type for message "+message.getMsgId()
          +" type:" + message.getMsgType());
    }
    
    String stateUnitKey = message.getStateUnitKey();
    StateModelFactory<T> stateModelFactory = getStateModelFactory();
    // StateModel stateModel;
    T stateModel = stateModelFactory.getStateModel(stateUnitKey);
    if (stateModel == null)
    {
      stateModel = stateModelFactory.createNewStateModel(stateUnitKey);
      stateModelFactory.addStateModel(stateUnitKey, stateModel);
    }
    return new CMStateTransitionHandler(stateModel);
  }

  @Override
  public String getMessageType()
  {
    return MessageType.STATE_TRANSITION.toString();
  }
}
