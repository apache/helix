package com.linkedin.clustermanager.participant;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.messaging.handling.CMStateTransitionHandler;
import com.linkedin.clustermanager.messaging.handling.CMTaskExecutor;
import com.linkedin.clustermanager.messaging.handling.MessageHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.monitoring.ParticipantMonitor;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;
import com.linkedin.clustermanager.participant.statemachine.StateModelParser;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class StateMachineEngine<T extends StateModel> implements
    MessageHandlerFactory
{
  private static Logger logger = Logger.getLogger(StateMachineEngine.class);
  private final StateModelFactory<T> _stateModelFactory;
  private final CMTaskExecutor _taskExecutor;
  StatusUpdateUtil _statusUpdateUtil;
  StateModelParser _stateModelParser;

  public StateModelFactory<T> getStateModelFactory()
  {
    return _stateModelFactory;
  }

  public StateMachineEngine(StateModelFactory<T> factory)
  {
    this._stateModelFactory = factory;
    _taskExecutor = new CMTaskExecutor();
    _statusUpdateUtil = new StatusUpdateUtil();
    _stateModelParser = new StateModelParser();
  }

  public ParticipantMonitor getTransitionStatMonitor()
  {
    return _taskExecutor.getParticipantMonitor();
  }

  void reset()
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
    MessageType type = message.getMsgType();
    
    if(type != MessageType.STATE_TRANSITION)
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
      stateModel =stateModelFactory.createNewStateModel(stateUnitKey);
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
