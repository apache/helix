package com.linkedin.helix.participant;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
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
  private static Logger logger = Logger
      .getLogger(HelixStateMachineEngine.class);
  private final Map<String, StateModelFactory<? extends StateModel>> _stateModelFactoryMap = new ConcurrentHashMap<String, StateModelFactory<? extends StateModel>>();
  StateModelParser _stateModelParser;
  final static char SEPARATOR = '^';
  private final HelixManager _manager;

  public StateModelFactory<? extends StateModel> getStateModelFactory(
      String stateModelName)
  {
    return _stateModelFactoryMap.get(stateModelName);
  }

  public HelixStateMachineEngine(HelixManager manager)
  {
    _stateModelParser = new StateModelParser();
    _manager = manager;
  }

  @Override
  public boolean registerStateModelFactory(String stateModelDef,
      StateModelFactory<? extends StateModel> factory)
  {
    return registerStateModelFactory(stateModelDef, null, factory);
  }

  @Override
  public boolean registerStateModelFactory(String stateModelDef,
      String resourceName, StateModelFactory<? extends StateModel> factory)
  {
    if (_manager.isConnected())
    {
      throw new HelixException(
          "stateModelFactory cannot be registered after manager is connected");
    }

    if (stateModelDef == null || stateModelDef.contains("" + SEPARATOR))
    {
      throw new HelixException(
          "stateModelDef cannot be null or contains character " + SEPARATOR
              + " (was " + stateModelDef + ")");
    }

    if (resourceName != null && resourceName.contains("" + SEPARATOR))
    {
      throw new HelixException("resourceName cannot contain character "
          + SEPARATOR + " (was " + resourceName + ")");
    }

    logger.info("Register state model factory for state model " + stateModelDef
        + " for resource  " + resourceName + " with " + factory);

    String key = stateModelDef
        + (resourceName == null ? "" : SEPARATOR + resourceName);
    if (_stateModelFactoryMap.containsKey(key))
    {
      logger.warn("StateModelFactory for " + key
          + " has already been registered.");
      return false;
    }

    _stateModelFactoryMap.put(key, factory);
    return true;
  }

  @Override
  public void reset()
  {
    for (StateModelFactory<? extends StateModel> stateModelFactory : _stateModelFactoryMap
        .values())
    {
      Map<String, ? extends StateModel> modelMap = stateModelFactory
          .getStateModelMap();
      if (modelMap == null || modelMap.isEmpty())
      {
        return;
      }
      for (String partitionName : modelMap.keySet())
      {
        StateModel stateModel = modelMap.get(partitionName);
        stateModel.reset();
        String initialState = _stateModelParser.getInitialState(stateModel
            .getClass());
        stateModel.updateState(initialState);
        // todo probably should update the state on ZK. Shi confirm what needs
        // to
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
      throw new HelixException("Unexpected msg type for message "
          + message.getMsgId() + " type:" + message.getMsgType());
    }

    String partitionName = message.getPartitionName();
    String stateModelName = message.getStateModelDef();
    String resourceName = message.getResourceName();
    if (stateModelName == null)
    {
      logger.warn("message does not contain stateModelDef");
      return null;
    }

    String key = stateModelName + SEPARATOR + resourceName;
    StateModelFactory stateModelFactory = getStateModelFactory(key);
    if (stateModelFactory == null)
    {
      stateModelFactory = getStateModelFactory(stateModelName);
      if (stateModelFactory == null)
      {
        logger.warn("Cannot find stateModelFactory for model " + stateModelName
            + " resourceName " + resourceName);
        return null;
      }
    }
    StateModel stateModel = stateModelFactory.getStateModel(partitionName);
    if (stateModel == null)
    {
      // stateModelFactory.addStateModel(key,stateModelFactory.createNewStateModel(stateUnitKey));
      stateModelFactory.createAndAddStateModel(partitionName);
      stateModel = stateModelFactory.getStateModel(partitionName);
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
      String resourceName, StateModelFactory<? extends StateModel> factory)
  {
    throw new UnsupportedOperationException("Remove not yet supported");
  }
}
