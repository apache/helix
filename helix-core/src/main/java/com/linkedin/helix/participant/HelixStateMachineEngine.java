package com.linkedin.helix.participant;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.messaging.handling.HelixStateTransitionHandler;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.participant.statemachine.StateModelParser;

public class HelixStateMachineEngine implements StateMachineEngine
{
  private static Logger logger = Logger.getLogger(HelixStateMachineEngine.class);

  public final static String DEFAULT_FACTORY = "DEFALUT";

  // StateModelName->FactoryName->StateModelFactory
  private final Map<String, Map<String, StateModelFactory<? extends StateModel>>> _stateModelFactoryMap = new ConcurrentHashMap<String, Map<String, StateModelFactory<? extends StateModel>>>();
  StateModelParser _stateModelParser;

  // final static char SEPARATOR = '^';

  // private final HelixManager _manager;

  public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName)
  {
    return getStateModelFactory(stateModelName, DEFAULT_FACTORY);
  }

  public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName,
      String factoryName)
  {
    return _stateModelFactoryMap.get(stateModelName).get(factoryName);
  }

  // public HelixStateMachineEngine(HelixManager manager)
  public HelixStateMachineEngine()
  {
    _stateModelParser = new StateModelParser();
    // _manager = manager;
  }

  @Override
  public boolean registerStateModelFactory(String stateModelDef,
      StateModelFactory<? extends StateModel> factory)
  {
    return registerStateModelFactory(stateModelDef, factory, DEFAULT_FACTORY);
  }

  @Override
  public boolean registerStateModelFactory(String stateModelDef,
      StateModelFactory<? extends StateModel> factory, String factoryName)
  {
    // if (_manager.isConnected())
    // {
    // throw new
    // HelixException("stateModelFactory cannot be registered after manager is connected");
    // }

    // if (stateModelDef == null || stateModelDef.contains("" + SEPARATOR))
    if (stateModelDef == null || factory == null || factoryName == null)
    {
      throw new HelixException("stateModelDef|stateModelFactory|factoryName cannot be null");
    }

    // if (resourceGroupName != null && resourceGroupName.contains("" +
    // SEPARATOR))
    // {
    // throw new HelixException("resourceGroupName cannot contain character " +
    // SEPARATOR + " (was "
    // + resourceGroupName + ")");
    // }

    logger.info("Register state model factory for state model " + stateModelDef
        + " using factory name " + factoryName + " with " + factory);

    // String key = stateModelDef + (resourceGroupName == null ? "" : SEPARATOR
    // + resourceGroupName);
    if (!_stateModelFactoryMap.containsKey(stateModelDef))
    {
      _stateModelFactoryMap.put(stateModelDef,
          new ConcurrentHashMap<String, StateModelFactory<? extends StateModel>>());
    }

    if (_stateModelFactoryMap.get(stateModelDef).containsKey(factoryName))
    {
      logger.warn("stateModelFactory for " + stateModelDef + " using factoryName " + factoryName
          + " has already been registered.");
      return false;
    }

    _stateModelFactoryMap.get(stateModelDef).put(factoryName, factory);
    return true;
  }

  @Override
  public void reset()
  {
    for (Map<String, StateModelFactory<? extends StateModel>> ftyMap : _stateModelFactoryMap
        .values())
    {
      for (StateModelFactory<? extends StateModel> stateModelFactory : ftyMap.values())
      {
        Map<String, ? extends StateModel> modelMap = stateModelFactory.getStateModelMap();
        if (modelMap == null || modelMap.isEmpty())
        {
          continue;
        }

        for (String resourceKey : modelMap.keySet())
        {
          StateModel stateModel = modelMap.get(resourceKey);
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
      throw new HelixException("Unexpected msg type for message " + message.getMsgId() + " type:"
          + message.getMsgType());
    }

    String partitionKey = message.getPartitionName();
    String stateModelName = message.getStateModelDef();
    String resourceName = message.getResourceName();
    if (stateModelName == null)
    {
      logger.error("message does not contain stateModelDef");
      return null;
    }

    // String key = stateModelName + SEPARATOR + resourceGroupName;
    String factoryName = message.getStateModelFactoryName();
    if (factoryName == null)
    {
      factoryName = DEFAULT_FACTORY;
    }

    StateModelFactory stateModelFactory = getStateModelFactory(stateModelName, factoryName);
    if (stateModelFactory == null)
    {
      // stateModelFactory = getStateModelFactory(stateModelName);
      // if (stateModelFactory == null)
      // {
      logger.warn("Cannot find stateModelFactory for model:" + stateModelName
          + " using factoryName:" + factoryName + " for resourceGroup:" + resourceName);
      return null;
      // }
    }

    StateModel stateModel = stateModelFactory.getStateModel(partitionKey);
    if (stateModel == null)
    {
      // stateModelFactory.addStateModel(key,stateModelFactory.createNewStateModel(stateUnitKey));
      stateModelFactory.createAndAddStateModel(partitionKey);
      stateModel = stateModelFactory.getStateModel(partitionKey);

    }
    return new HelixStateTransitionHandler(stateModel, message, context);
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
      StateModelFactory<? extends StateModel> factory, String factoryName)
  {
    throw new UnsupportedOperationException("Remove not yet supported");
  }
}
