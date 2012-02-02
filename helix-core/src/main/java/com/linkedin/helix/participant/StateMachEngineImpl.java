package com.linkedin.helix.participant;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.ClusterManagerException;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.messaging.handling.CMStateTransitionHandler;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.participant.statemachine.StateModelParser;

public class StateMachEngineImpl implements StateMachEngine
{
  private static Logger logger = Logger.getLogger(StateMachEngineImpl.class);
  private final Map<String, StateModelFactory<? extends StateModel>> _stateModelFactoryMap
     = new ConcurrentHashMap<String, StateModelFactory<? extends StateModel>>();
  StateModelParser _stateModelParser;
  final static char SEPARATOR = '^';
  private final ClusterManager _manager;

  public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName)
  {
    return _stateModelFactoryMap.get(stateModelName);
  }

  public StateMachEngineImpl(ClusterManager manager)
  {
    _stateModelParser = new StateModelParser();
    _manager = manager;
  }

  @Override
  public boolean registerStateModelFactory(String stateModelDef, StateModelFactory<? extends StateModel> factory)
  {
    return registerStateModelFactory(stateModelDef, null, factory);
  }

  @Override
  public boolean registerStateModelFactory(String stateModelDef, String resourceGroupName,
      StateModelFactory<? extends StateModel> factory)
  {
    if (_manager.isConnected())
    {
      throw new ClusterManagerException("stateModelFactory cannot be registered after manager is connected");
    }

    if (stateModelDef == null || stateModelDef.contains("" + SEPARATOR))
    {
      throw new ClusterManagerException("stateModelDef cannot be null or contains character "
          + SEPARATOR + " (was " + stateModelDef + ")");
    }

    if (resourceGroupName != null && resourceGroupName.contains("" + SEPARATOR))
    {
      throw new ClusterManagerException("resourceGroupName cannot contain character "
          + SEPARATOR + " (was " + resourceGroupName + ")");
    }

    logger.info("Register state model factory for state model " + stateModelDef
              + " for resource group " + resourceGroupName + " with " +  factory);

    String key = stateModelDef + (resourceGroupName == null? "" : SEPARATOR + resourceGroupName);
    if (_stateModelFactoryMap.containsKey(key))
    {
      logger.warn("StateModelFactory for " + key + " has already been registered.");
      return false;
    }

    _stateModelFactoryMap.put(key, factory);
    return true;
  }

  @Override
  public void reset()
  {
    for (StateModelFactory<? extends StateModel> stateModelFactory : _stateModelFactoryMap.values())
    {
      Map<String, ? extends StateModel> modelMap = stateModelFactory.getStateModelMap();
      if (modelMap == null || modelMap.isEmpty())
      {
        return;
      }
      for (String resourceKey : modelMap.keySet())
      {
        StateModel stateModel = modelMap.get(resourceKey);
        stateModel.reset();
        String initialState = _stateModelParser.getInitialState(stateModel.getClass());
        stateModel.updateState(initialState);
        // todo probably should update the state on ZK. Shi confirm what needs to
        // be done here.
      }
    }
  }

  @Override
  public MessageHandler createHandler(Message message,
      NotificationContext context)
  {
    String type = message.getMsgType();

    if (!type.equals(MessageType.STATE_TRANSITION.toString()))
    {
      throw new ClusterManagerException("Unexpected msg type for message " + message.getMsgId()
          + " type:" + message.getMsgType());
    }

    String stateUnitKey = message.getStateUnitKey();
    String stateModelName = message.getStateModelDef();
    String resourceGroupName = message.getResourceGroupName();
    if (stateModelName == null)
    {
      logger.warn("message does not contain stateModelDef");
      return null;
    }

    String key = stateModelName + SEPARATOR + resourceGroupName;
    StateModelFactory stateModelFactory = getStateModelFactory(key);
    if (stateModelFactory == null)
    {
      stateModelFactory = getStateModelFactory(stateModelName);
      if(stateModelFactory == null)
      {
        logger.warn("Cannot find stateModelFactory for model " + stateModelName
            + " resourceGroup " + resourceGroupName);
        return null;
      }
    }
    StateModel stateModel = stateModelFactory.getStateModel(stateUnitKey);
    if (stateModel == null)
    {
      //stateModelFactory.addStateModel(key,stateModelFactory.createNewStateModel(stateUnitKey));
      stateModelFactory.createAndAddStateModel(stateUnitKey);
      stateModel = stateModelFactory.getStateModel(stateUnitKey);
    }
    return new CMStateTransitionHandler(stateModel, message, context);
  }

  @Override
  public String getMessageType()
  {
    return MessageType.STATE_TRANSITION.toString();
  }
}
