package com.linkedin.helix.messaging.handling;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecordDelta;
import com.linkedin.helix.ZNRecordDelta.MERGEOPERATION;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageSubType;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelParser;
import com.linkedin.helix.participant.statemachine.StateTransitionError;
import com.linkedin.helix.util.StatusUpdateUtil;

public class HelixStateTransitionHandler extends MessageHandler
{
  private static Logger logger = Logger.getLogger(HelixStateTransitionHandler.class);
  private final StateModel _stateModel;
  StatusUpdateUtil _statusUpdateUtil;
  private final StateModelParser _transitionMethodFinder;

  public HelixStateTransitionHandler(StateModel stateModel, Message message,
      NotificationContext context)
  {
    super(message, context);
    this._stateModel = stateModel;
    _statusUpdateUtil = new StatusUpdateUtil();
    _transitionMethodFinder = new StateModelParser();
  }

  private void prepareMessageExecution(HelixManager manager, Message message) throws HelixException
  {
    if (!message.isValid())
    {
      String errorMessage = "Invalid Message, ensure that message: " + message
          + " has all the required fields: " + Arrays.toString(Message.Attributes.values());

      _statusUpdateUtil.logError(message, HelixStateTransitionHandler.class, errorMessage,
          manager.getDataAccessor());
      logger.error(errorMessage);
      throw new HelixException(errorMessage);
    }
    DataAccessor accessor = manager.getDataAccessor();
    String partitionKey = message.getStateUnitKey();
    String resourceGroup = message.getStateUnitGroup();
    String instanceName = manager.getInstanceName();

    String fromState = message.getFromState();
    String toState = message.getToState();

    List<StateModelDefinition> stateModelDefs = accessor.getChildValues(StateModelDefinition.class,
        PropertyType.STATEMODELDEFS);

    StateModelDefinition stateModelDef = lookupStateModel(message.getStateModelDef(),
        stateModelDefs);

    if (stateModelDef == null)
    {
      throw new HelixException("No State Model Defined for " + message.getStateModelDef());

    }
    String initStateValue = stateModelDef.getInitialState();
    CurrentState currentState = accessor.getProperty(CurrentState.class,
        PropertyType.CURRENTSTATES, instanceName, manager.getSessionId(), resourceGroup);

    // Set an empty current state record if it is null
    if (currentState == null)
    {
      currentState = new CurrentState(resourceGroup);
      currentState.setSessionId(manager.getSessionId());
      accessor.updateProperty(PropertyType.CURRENTSTATES, currentState, instanceName,
          manager.getSessionId(), resourceGroup);
    }

    /**
     * For resource unit that does not have a state, initialize it to OFFLINE If
     * current state does not have a state model def, set it. Do the two updates
     * together, otherwise controller may view a current state with a NULL state
     * model def
     */

    CurrentState currentStateDelta = new CurrentState(resourceGroup);
    if (currentState.getState(partitionKey) == null)
    {
      currentStateDelta.setState(partitionKey, initStateValue);
      currentState.setState(partitionKey, initStateValue);

      logger.info("Setting initial state for partition: " + partitionKey + " to " + initStateValue);
    }

    // Set the state model def to current state
    if (currentState.getStateModelDefRef() == null)
    {

      if (message.getStateModelDef() != null)
      {
        logger.info("Setting state model def on current state: " + message.getStateModelDef());
        currentStateDelta.setStateModelDefRef(message.getStateModelDef());
      }
    }
    accessor.updateProperty(PropertyType.CURRENTSTATES, currentStateDelta, instanceName,
        manager.getSessionId(), resourceGroup);

    // Verify the fromState and current state of the stateModel
    String state = currentState.getState(partitionKey);
    // if (!fromState.equals("*")
    // && (fromState == null || !fromState.equalsIgnoreCase(state)))
    if (fromState != null && !fromState.equals("*") && !fromState.equalsIgnoreCase(state))
    {
      String errorMessage = "Current state of stateModel does not match the fromState in Message"
          + ", Current State:" + state + ", message expected:" + fromState + ", partition: "
          + partitionKey + ", from: " + message.getMsgSrc() + ", to: " + message.getTgtName();

      _statusUpdateUtil
          .logError(message, HelixStateTransitionHandler.class, errorMessage, accessor);
      logger.error(errorMessage);
      throw new HelixException(errorMessage);
    }
  }

  void postExecutionMessage(HelixManager manager, Message message, NotificationContext context,
      HelixTaskResult taskResult, Exception exception) throws InterruptedException
  {
    DataAccessor accessor = manager.getDataAccessor();
    try
    {
      String partitionKey = message.getStateUnitKey();
      String resourceGroup = message.getStateUnitGroup();
      String instanceName = manager.getInstanceName();

      CurrentState currentState = accessor.getProperty(CurrentState.class,
          PropertyType.CURRENTSTATES, instanceName, manager.getSessionId(), resourceGroup);

      if (currentState == null)
      {
        logger
            .warn("currentState is null. Storage node should be working with static file based cluster manager.");
      }

      // TODO verify that fromState is same as currentState this task
      // was
      // called at.
      // Verify that no one has edited this field
      CurrentState currentStateDelta = new CurrentState(resourceGroup);
      currentStateDelta.setResourceGroup(partitionKey, resourceGroup);

      if (taskResult.isSucess())
      {
        // String fromState = message.getFromState();
        String toState = message.getToState();

        if (toState.equalsIgnoreCase("DROPPED"))
        {
          // for "OnOfflineToDROPPED" message, we need to remove the resource
          // key
          // record from
          // the current state of the instance because the resource key is
          // dropped.
          ZNRecordDelta delta = new ZNRecordDelta(currentStateDelta.getRecord(),
              MERGEOPERATION.SUBTRACT);
          List<ZNRecordDelta> deltaList = new ArrayList<ZNRecordDelta>();
          deltaList.add(delta);
          currentStateDelta.setDeltaList(deltaList);
        } else
        {
          // If a resource key is dropped, it is ok to leave it "offline"
          currentStateDelta.setState(partitionKey, toState);
          _stateModel.updateState(toState);
        }
      } else
      {
        StateTransitionError error = new StateTransitionError(ErrorType.INTERNAL, ErrorCode.ERROR,
            exception);

        _stateModel.rollbackOnError(message, context, error);
        currentStateDelta.setState(partitionKey, "ERROR");
        _stateModel.updateState("ERROR");

      }

      // based on task result update the current state of the node.
      accessor.updateProperty(PropertyType.CURRENTSTATES, currentStateDelta, instanceName,
          manager.getSessionId(), resourceGroup);
    } catch (Exception e)
    {
      logger.error("Error when updating the state ", e);
      StateTransitionError error = new StateTransitionError(ErrorType.FRAMEWORK, ErrorCode.ERROR, e);
      _stateModel.rollbackOnError(message, context, error);
      _statusUpdateUtil.logError(message, HelixStateTransitionHandler.class, e,
          "Error when update the state ", accessor);
    }
  }

  public HelixTaskResult handleMessageInternal(Message message, NotificationContext context)
  {
    synchronized (_stateModel)
    {
      HelixTaskResult taskResult = new HelixTaskResult();
      HelixManager manager = context.getManager();
      DataAccessor accessor = manager.getDataAccessor();
      try
      {
        _statusUpdateUtil.logInfo(message, HelixStateTransitionHandler.class,
            "Message handling task begin execute", accessor);
        message.setExecuteStartTimeStamp(new Date().getTime());

        Exception exception = null;
        try
        {
          prepareMessageExecution(manager, message);
          invoke(accessor, context, taskResult, message);
        } catch (InterruptedException e)
        {
          throw e;
        } catch (Exception e)
        {
          String errorMessage = "Exception while executing a state transition task. ";
          logger.error(errorMessage + ". " + e.getMessage(), e);
          _statusUpdateUtil.logError(message, HelixStateTransitionHandler.class, e, errorMessage,
              accessor);
          taskResult.setSuccess(false);
          taskResult.setMessage(e.toString());
          taskResult.setException(e);

          exception = e;
        }
        postExecutionMessage(manager, message, context, taskResult, exception);
        return taskResult;
      } catch (InterruptedException e)
      {
        _statusUpdateUtil.logError(message, HelixStateTransitionHandler.class, e,
            "State transition interrupted", accessor);
        logger.info("Message " + message.getMsgId() + " is interrupted");

        StateTransitionError error = new StateTransitionError(ErrorType.FRAMEWORK,
            ErrorCode.CANCEL, e);

        _stateModel.rollbackOnError(message, context, error);
        // We have handled the cancel case here, so no need to let outside know
        // taskResult.setInterrupted(true);
        // taskResult.setException(e);
        taskResult.setSuccess(false);
        return taskResult;
      }
    }
  }

  private void invoke(DataAccessor accessor, NotificationContext context,
      HelixTaskResult taskResult, Message message) throws IllegalAccessException,
      InvocationTargetException, InterruptedException
  {
    _statusUpdateUtil.logInfo(message, HelixStateTransitionHandler.class,
        "Message handling invoking", accessor);

    if (message.getMsgSubType() != null
        && message.getMsgSubType().equals(MessageSubType.RESET.toString()))
    {
      _stateModel.reset();
      taskResult.setSuccess(true);
      return;
    }

    // by default, we invoke state transition function in state model
    Method methodToInvoke = null;
    String fromState = message.getFromState();
    String toState = message.getToState();
    methodToInvoke = _transitionMethodFinder.getMethodForTransition(_stateModel.getClass(),
        fromState, toState, new Class[] { Message.class, NotificationContext.class });
    if (methodToInvoke != null)
    {
      methodToInvoke.invoke(_stateModel, new Object[] { message, context });
      taskResult.setSuccess(true);
    } else
    {
      String errorMessage = "Unable to find method for transition from " + fromState + " to "
          + toState + "in " + _stateModel.getClass();
      logger.error(errorMessage);
      taskResult.setSuccess(false);

      System.out.println(errorMessage);
      _statusUpdateUtil
          .logError(message, HelixStateTransitionHandler.class, errorMessage, accessor);
    }
  }

  private StateModelDefinition lookupStateModel(String stateModelDefRef,
      List<StateModelDefinition> stateModelDefs)
  {
    for (StateModelDefinition def : stateModelDefs)
    {
      if (def.getId().equals(stateModelDefRef))
      {
        return def;
      }
    }
    return null;
  }

  @Override
  public HelixTaskResult handleMessage() throws InterruptedException
  {
    return handleMessageInternal(_message, _notificationContext);
  }

  @Override
  public void onError(Exception e, ErrorCode code, ErrorType type)
  {
    HelixManager manager = _notificationContext.getManager();
    DataAccessor accessor = manager.getDataAccessor();
    String instanceName = manager.getInstanceName();
    String stateUnitKey = _message.getStateUnitKey();
    String stateUnitGroup = _message.getStateUnitGroup();
    CurrentState currentStateDelta = new CurrentState(stateUnitGroup);

    StateTransitionError error = new StateTransitionError(type, code, e);
    _stateModel.rollbackOnError(_message, _notificationContext, error);
    // if the transition is not canceled, it should go into error state
    if (code == ErrorCode.ERROR)
    {
      currentStateDelta.setState(stateUnitKey, "ERROR");
      _stateModel.updateState("ERROR");

      currentStateDelta.setResourceGroup(stateUnitKey, _message.getStateUnitGroup());
      accessor.updateProperty(PropertyType.CURRENTSTATES, currentStateDelta, instanceName,
          manager.getSessionId(), stateUnitGroup);
    }
  }
};
