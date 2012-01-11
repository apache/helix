package com.linkedin.clustermanager.messaging.handling;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecordDelta;
import com.linkedin.clustermanager.ZNRecordDelta.MERGEOPERATION;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelParser;
import com.linkedin.clustermanager.participant.statemachine.StateTransitionError;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class CMStateTransitionHandler implements MessageHandler
{
  private static Logger logger = Logger.getLogger(CMStateTransitionHandler.class);
  private final StateModel _stateModel;
  StatusUpdateUtil _statusUpdateUtil;
  private final StateModelParser _transitionMethodFinder;

  public CMStateTransitionHandler(StateModel stateModel)
  {
    this._stateModel = stateModel;
    _statusUpdateUtil = new StatusUpdateUtil();
    _transitionMethodFinder = new StateModelParser();
  }

  // TODO replace with util from espresso or linkedin
  private boolean isNullOrEmpty(String data)
  {
    return data == null || data.length() == 0 || data.trim().length() == 0;
  }

  private boolean validateMessage(Message message)
  {
    boolean isValid =
        isNullOrEmpty(message.getFromState()) || isNullOrEmpty(message.getToState())
            || isNullOrEmpty(message.getToState())
            || isNullOrEmpty(message.getStateUnitKey())
            || isNullOrEmpty(message.getToState())
            || isNullOrEmpty(message.getStateModelDef());
    return !isValid;
  }

  private void prepareMessageExecution(ClusterManager manager, Message message) throws ClusterManagerException
  {
    if (!validateMessage(message))
    {
      String errorMessage =
          "Invalid Message, ensure that message: " + message
              + " has all the required fields: "
              + Arrays.toString(Message.Attributes.values());

      _statusUpdateUtil.logError(message,
                                 CMStateTransitionHandler.class,
                                 errorMessage,
                                 manager.getDataAccessor());
      logger.error(errorMessage);
      throw new ClusterManagerException(errorMessage);
    }
    ClusterDataAccessor accessor = manager.getDataAccessor();
    String stateUnitKey = message.getStateUnitKey();
    String stateUnitGroup = message.getStateUnitGroup();
    String instanceName = manager.getInstanceName();

    String fromState = message.getFromState();
    String toState = message.getToState();

    List<StateModelDefinition> stateModelDefs =
        accessor.getChildValues(StateModelDefinition.class, PropertyType.STATEMODELDEFS);

    StateModelDefinition stateModelDef =
        lookupStateModel(message.getStateModelDef(), stateModelDefs);

    String initStateValue;
    if (stateModelDef == null)
    {
      throw new ClusterManagerException("No State Model Defined for "+ message.getStateModelDef());
    }
    initStateValue = stateModelDef.getInitialState();
    CurrentState currentState =
        accessor.getProperty(CurrentState.class,
                             PropertyType.CURRENTSTATES,
                             instanceName,
                             manager.getSessionId(),
                             stateUnitGroup);

    // Set an empty current state record if it is null
    if (currentState == null)
    {
      currentState = new CurrentState(stateUnitGroup);
      currentState.setSessionId(manager.getSessionId());
      accessor.updateProperty(PropertyType.CURRENTSTATES,
                              currentState,
                              instanceName,
                              manager.getSessionId(),
                              stateUnitGroup);
    }

    /**
     * For resource unit that does not have a state, initialize it to OFFLINE If current
     * state does not have a state model def, set it. Do the two updates together,
     * otherwise controller may view a current state with a NULL state model def
     */

    CurrentState currentStateDelta = new CurrentState(stateUnitGroup);
    if (currentState.getState(stateUnitKey) == null)
    {
      currentStateDelta.setState(stateUnitKey, initStateValue);
      currentState.setState(stateUnitKey, initStateValue);

      logger.info("Setting initial state for partition: " + stateUnitKey + " to "
          + initStateValue);
    }

    // Set the state model def to current state
    if (currentState.getStateModelDefRef() == null)
    {

      if (message.getStateModelDef() != null)
      {
        logger.info("Setting state model def on current state: "
            + message.getStateModelDef());
        currentStateDelta.setStateModelDefRef(message.getStateModelDef());
      }
    }
    accessor.updateProperty(PropertyType.CURRENTSTATES,
                            currentStateDelta,
                            instanceName,
                            manager.getSessionId(),
                            stateUnitGroup);

    // Verify the fromState and current state of the stateModel
    String state = currentState.getState(stateUnitKey);
    if (!fromState.equals("*")
        && (fromState == null || !fromState.equalsIgnoreCase(state)))
    {
      String errorMessage =
          "Current state of stateModel does not match the fromState in Message"
              + ", Current State:" + state + ", message expected:" + fromState
              + ", partition: " + message.getStateUnitKey() + ", from: "
              + message.getMsgSrc() + ", to: " + message.getTgtName();

      _statusUpdateUtil.logError(message,
                                 CMStateTransitionHandler.class,
                                 errorMessage,
                                 accessor);
      logger.error(errorMessage);
      throw new ClusterManagerException(errorMessage);
    }
  }

  void postExecutionMessage(ClusterManager manager,
                            Message message,
                            NotificationContext context,
                            CMTaskResult taskResult,
                            Exception exception) throws InterruptedException
  {
    ClusterDataAccessor accessor = manager.getDataAccessor();
    try
    {
      String stateUnitKey = message.getStateUnitKey();
      String stateUnitGroup = message.getStateUnitGroup();
      String instanceName = manager.getInstanceName();

      String fromState = message.getFromState();
      String toState = message.getToState();
      CurrentState currentState =
          accessor.getProperty(CurrentState.class,
                               PropertyType.CURRENTSTATES,
                               instanceName,
                               manager.getSessionId(),
                               stateUnitGroup);

      if (currentState != null)
      {
        // map = currentState.getMapField(stateUnitKey);
      }
      else
      {
        logger.warn("currentState is null. Storage node should be working with file based cluster manager.");
      }

      // TODO verify that fromState is same as currentState this task
      // was
      // called at.
      // Verify that no one has edited this field
      CurrentState currentStateDelta = new CurrentState(stateUnitGroup);
      if (taskResult.isSucess())
      {
        if (!toState.equalsIgnoreCase("DROPPED"))
        {
          // If a resource key is dropped, it is ok to leave it "offline"
          currentStateDelta.setState(stateUnitKey, toState);
          _stateModel.updateState(toState);
        }
      }
      else
      {
        StateTransitionError error =
            new StateTransitionError(StateTransitionError.ErrorCode.INTERNAL, exception);
        _stateModel.rollbackOnError(message, context, error);
        currentStateDelta.setState(stateUnitKey, "ERROR");
        _stateModel.updateState("ERROR");
      }

      currentStateDelta.setResourceGroup(stateUnitKey, message.getStateUnitGroup());

      if (taskResult.isSucess() && toState.equals("DROPPED"))
      {
        // for "OnOfflineToDROPPED" message, we need to remove the resource key
        // record from
        // the current state of the instance because the resource key is dropped.
        ZNRecordDelta delta =
            new ZNRecordDelta(currentStateDelta.getRecord(), MERGEOPERATION.SUBSTRACT);
        List<ZNRecordDelta> deltaList = new ArrayList<ZNRecordDelta>();
        deltaList.add(delta);
        CurrentState currentStateUpdate = new CurrentState(stateUnitGroup);
        currentStateUpdate.setDeltaList(deltaList);
        accessor.updateProperty(PropertyType.CURRENTSTATES,
                                currentStateUpdate,
                                instanceName,
                                manager.getSessionId(),
                                stateUnitGroup);
      }
      else
      {
        // based on task result update the current state of the node.
        accessor.updateProperty(PropertyType.CURRENTSTATES,
                                currentStateDelta,
                                instanceName,
                                manager.getSessionId(),
                                stateUnitGroup);
      }
    }
    catch (Exception e)
    {
      logger.error("Error when updating the state ", e);
      StateTransitionError error =
          new StateTransitionError(StateTransitionError.ErrorCode.FRAMEWORK, e);
      _stateModel.rollbackOnError(message, context, error);
      _statusUpdateUtil.logError(message,
                                 CMStateTransitionHandler.class,
                                 e,
                                 "Error when update the state ",
                                 accessor);
    }
  }

  // TODO: decide if handleMessage() should return a value CMTaskResult; this
  // part need to integrate with
  // send reply message
  public CMTaskResult handleMessageInternal(Message message, NotificationContext context)
  {
    synchronized (_stateModel)
    {
      CMTaskResult taskResult = new CMTaskResult();
      ClusterManager manager = context.getManager();
      ClusterDataAccessor accessor = manager.getDataAccessor();
      String instanceName = manager.getInstanceName();
      try
      {
        _statusUpdateUtil.logInfo(message,
                                  CMStateTransitionHandler.class,
                                  "Message handling task begin execute",
                                  accessor);
        message.setExecuteStartTimeStamp(new Date().getTime());
        try
        {
          prepareMessageExecution(manager, message);
        }
        catch (ClusterManagerException e)
        {
          taskResult.setSuccess(false);
          taskResult.setMessage(e.getMessage());
          logger.error("prepareMessageExecution failed", e);
          return taskResult;
        }

        Exception exception = null;
        try
        {
          invoke(accessor, context, taskResult, message);
        }
        catch (Exception e)
        {
          String errorMessage = "Exception while executing a state transition task. " + e;

          // Hack: avoid throwing mock exception for testing code
          if (e instanceof InvocationTargetException
              && e.getCause().getMessage().startsWith("IGNORABLE"))
          {
            logger.error(errorMessage + ". Cause:" + e.getCause().getMessage());
          }
          else
          {
            logger.error(errorMessage, e);
          }
          _statusUpdateUtil.logError(message,
                                     CMStateTransitionHandler.class,
                                     e,
                                     errorMessage,
                                     accessor);
          taskResult.setSuccess(false);
          exception = e;

        }

        postExecutionMessage(manager, message, context, taskResult, exception);

        return taskResult;
      }
      catch (InterruptedException e)
      {
        _statusUpdateUtil.logError(message,
                                   CMStateTransitionHandler.class,
                                   e,
                                   "State transition interrupted",
                                   accessor);
        logger.info("Message " + message.getMsgId() + " is interrupted");

        StateTransitionError error =
            new StateTransitionError(StateTransitionError.ErrorCode.FRAMEWORK, e);
        _stateModel.rollbackOnError(message, context, error);
        return taskResult;
      }
    }
  }

  private void invoke(ClusterDataAccessor accessor,
                      NotificationContext context,
                      CMTaskResult taskResult,
                      Message message) throws IllegalAccessException,
      InvocationTargetException,
      InterruptedException
  {
    Method methodToInvoke = null;
    String fromState = message.getFromState();
    String toState = message.getToState();
    methodToInvoke =
        _transitionMethodFinder.getMethodForTransition(_stateModel.getClass(),
                                                       fromState,
                                                       toState,
                                                       new Class[] { Message.class,
                                                           NotificationContext.class });
    _statusUpdateUtil.logInfo(message,
                              CMStateTransitionHandler.class,
                              "Message handling invoking",
                              accessor);
    if (methodToInvoke != null)
    {
      methodToInvoke.invoke(_stateModel, new Object[] { message, context });
      taskResult.setSuccess(true);
    }
    else
    {
      String errorMessage =
          "Unable to find method for transition from " + fromState + " to " + toState
              + "in " + _stateModel.getClass();
      logger.error(errorMessage);
      taskResult.setSuccess(false);

      System.out.println(errorMessage);
      _statusUpdateUtil.logError(message,
                                 CMStateTransitionHandler.class,
                                 errorMessage,
                                 accessor);
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
  public void handleMessage(Message message,
                            NotificationContext context,
                            Map<String, String> resultMap) throws InterruptedException
  {
    handleMessageInternal(message, context);

  }
};
