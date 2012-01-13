package com.linkedin.clustermanager.participant;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

public class StateMachineEngine implements
    MessageHandlerFactory
{
  private static Logger logger = Logger.getLogger(StateMachineEngine.class);
  private final Map<String, StateModelFactory<? extends StateModel>> _stateModelFactoryMap
     = new ConcurrentHashMap<String, StateModelFactory<? extends StateModel>>();
  StateModelParser _stateModelParser;
  final static char SEPARATOR = '^'; 

  public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName)
  {
    return _stateModelFactoryMap.get(stateModelName);
  }

  public StateMachineEngine()
  {
    _stateModelParser = new StateModelParser();
  }

  public boolean registerStateModelFactory(String stateModelDef, StateModelFactory<? extends StateModel> factory)
  {
    if(stateModelDef.contains(""+SEPARATOR))
    {
      throw new ClusterManagerException("stateModelName cannot contain character "+SEPARATOR);
    }
    logger.info("Registering state model factory for state model "+ stateModelDef + " with "+ factory);
    if(_stateModelFactoryMap.containsKey(stateModelDef))
    {
      logger.warn("State model " + stateModelDef + " already registered");
      return false;
    }
    _stateModelFactoryMap.put(stateModelDef, factory);
    return true;
  }
  
  public boolean registerStateModelFactory(String stateModelDef, String resourceGroupName, StateModelFactory<? extends StateModel> factory)
  {
    if(stateModelDef.contains(""+SEPARATOR))
    {
      throw new ClusterManagerException("stateModelName cannot contain character "+SEPARATOR);
    }
    if(resourceGroupName.contains(""+SEPARATOR))
    {
      throw new ClusterManagerException("resourceGroupName cannot contain character "+SEPARATOR);
    }
    logger.info("Registering state model factory for state model "+ stateModelDef + " for resource group " + resourceGroupName+ " with "+ factory);
    String key = stateModelDef + SEPARATOR + resourceGroupName;
    if(_stateModelFactoryMap.containsKey(key))
    {
      logger.warn("Statemodel/resourceGroup key " + key + " already registered");
      return false;
    }
    _stateModelFactoryMap.put(key, factory);
    return true;
  }
  
  public void reset()
  {
    for(StateModelFactory<? extends StateModel> stateModelFactory : _stateModelFactoryMap.values())
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
        String initialState = _stateModelParser.getInitialState(stateModel
            .getClass());
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

    if(!type.equals(MessageType.STATE_TRANSITION.toString()))
    {
      throw new ClusterManagerException("Unexpected msg type for message "+message.getMsgId()
          +" type:" + message.getMsgType());
    }

    String stateUnitKey = message.getStateUnitKey();
    String stateModelName = message.getStateModelDef();
    String resourceGroupName = message.getResourceGroupName();
    if(stateModelName == null)
    {
      logger.warn("message does not contain stateModelDef");
      return null;
    }// StateModel stateModel;
    String key = stateModelName + SEPARATOR + resourceGroupName;
    StateModelFactory stateModelFactory = getStateModelFactory(key);
    if(stateModelFactory == null)
    {
      stateModelFactory = getStateModelFactory(stateModelName);
      if(stateModelFactory == null)
      {
        logger.warn("Cannot find stateModelFactory for model "+stateModelName + " resourceGroup "+ resourceGroupName);
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
