package com.linkedin.clustermanager.participant.statemachine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.CMConstants.ZNAttribute;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.controller.stages.AttributeName;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class CMTaskHandler implements Callable<CMTaskResult>
{
  private static Logger logger = Logger.getLogger(CMTaskHandler.class);
  private final Message _message;
  private final StateModel _stateModel;
  private final NotificationContext _notificationContext;
  private final ClusterManager _manager;
  StatusUpdateUtil _statusUpdateUtil;
  private StateModelParser _transitionMethodFinder;
  CMTaskExecutor _executor;

  public CMTaskHandler(NotificationContext notificationContext,
      Message message, StateModel stateModel, CMTaskExecutor executor) throws Exception
  {
    this._notificationContext = notificationContext;
    this._message = message;
    this._stateModel = stateModel;
    this._manager = notificationContext.getManager();
    _statusUpdateUtil = new StatusUpdateUtil();
    _transitionMethodFinder = new StateModelParser();
    _executor = executor;
    if (!validateTask())
    {
      String errorMessage = "Invalid Message, ensure that message: " + message
          + " has all the required fields: "
          + Arrays.toString(Message.Attributes.values());

      _statusUpdateUtil.logError(_message, CMTaskHandler.class, errorMessage,
          _manager.getDataAccessor());
      throw new ClusterManagerException(errorMessage);
    }
  }

  // TODO replace with util from espresso or linkedin
  private boolean isNullorEmpty(String data)
  {
    return data == null || data.length() == 0 || data.trim().length() == 0;
  }

  private boolean validateTask()
  {
    boolean isValid = isNullorEmpty(_message.getFromState())
        || isNullorEmpty(_message.getToState())
        || isNullorEmpty(_message.getToState())
        || isNullorEmpty(_message.getStateUnitKey())
        || isNullorEmpty(_message.getToState());
    return !isValid;
  }
  
  private void prepareMessageExecution() throws InterruptedException
  {
    ClusterDataAccessor accessor = _manager.getDataAccessor();
    String stateUnitKey = _message.getStateUnitKey();
    String stateUnitGroup = _message.getStateUnitGroup();
    String instanceName = _manager.getInstanceName();
    
    String fromState = _message.getFromState();
    String toState = _message.getToState();

    ZNRecord currentState = accessor.getInstanceProperty(instanceName,
        InstancePropertyType.CURRENTSTATES, _manager.getSessionId(), stateUnitGroup);
    // Set a empty current state record if it is null
    if (currentState == null)
    {
      currentState = new ZNRecord();
      currentState.setId(stateUnitGroup);
      currentState.setSimpleField(
          CMConstants.ZNAttribute.SESSION_ID.toString(),
          _manager.getSessionId());
      accessor.updateInstanceProperty(instanceName,
          InstancePropertyType.CURRENTSTATES, _manager.getSessionId(), stateUnitGroup, currentState);
    }
    Map<String, String> map;
    
    // For resource unit that does not have state before, init it to offline
    if(!currentState.getMapFields().containsKey(stateUnitKey))
    {
       map = new HashMap<String, String>();
       map.put(ZNAttribute.CURRENT_STATE.toString(), "OFFLINE");
       
       ZNRecord currentStateDelta = new ZNRecord();
       currentStateDelta.setMapField(stateUnitKey, map);
       
       logger.info("Setting initial state for partition: " + stateUnitKey + " to offline");
       
       accessor.updateInstanceProperty(instanceName,
         InstancePropertyType.CURRENTSTATES, _manager.getSessionId(), stateUnitGroup, currentStateDelta);
    }
    
    // Set the state model def to current state
    if(!currentState.getSimpleFields().containsKey(Message.Attributes.STATE_MODEL_DEF.toString()))
    {
      if(_message.getSimpleFields().containsKey(Message.Attributes.STATE_MODEL_DEF.toString()))
      {
        logger.info("Setting state model def on current state: " + _message.getStateModelDef());
        ZNRecord currentStateDelta = new ZNRecord();
        currentStateDelta.setSimpleField(Message.Attributes.STATE_MODEL_DEF.toString(), _message.getStateModelDef());
        
        accessor.updateInstanceProperty(instanceName,
            InstancePropertyType.CURRENTSTATES, _manager.getSessionId(), stateUnitGroup, currentStateDelta);
      }
    }
    // Verify the fromState and current state of the stateModel
    if ( !fromState.equals("*") && 
        (fromState == null
        || !fromState.equalsIgnoreCase(_stateModel.getCurrentState())))
    {
      String errorMessage = "Current state of stateModel does not match the fromState in Message "
          + " Current State:"
          + _stateModel.getCurrentState()
          + ", message expected:" + fromState;
      
      _statusUpdateUtil.logError(_message, CMTaskHandler.class, errorMessage,
          accessor);
      throw new ClusterManagerException(errorMessage);  
    }
  }
  
  public void postExecutionMessage(CMTaskResult taskResult, Exception exception) throws InterruptedException
  {
    ClusterDataAccessor accessor = _manager.getDataAccessor();
    try
    {
      
      String stateUnitKey = _message.getStateUnitKey();
      String stateUnitGroup = _message.getStateUnitGroup();
      String instanceName = _manager.getInstanceName();
      
      String fromState = _message.getFromState();
      String toState = _message.getToState();
      ZNRecord currentState = accessor.getInstanceProperty(instanceName,
          InstancePropertyType.CURRENTSTATES, _manager.getSessionId(), stateUnitGroup);
      
      Map<String, String> map = currentState.getMapField(stateUnitKey);
      map.put(Message.Attributes.STATE_UNIT_GROUP.toString(),
          _message.getStateUnitGroup());
      
      
      // TODO verify that fromState is same as currentState this task
      // was
      // called at.
      // Verify that no one has edited this field
      ZNRecord currentStateDelta = new ZNRecord();
      if (taskResult.isSucess())
      {
        _statusUpdateUtil.logInfo(_message, CMTaskHandler.class,
            "Message handling task completed successfully", accessor);
        
        if(!toState.equalsIgnoreCase("DROPPED"))
        {
          // If a resource key is dropped, it is ok to leave it "offline"
          map.put(ZNAttribute.CURRENT_STATE.toString(), toState); 
          _stateModel.updateState(toState);
        }
        
        currentStateDelta.mapFields.put(stateUnitKey, map);  
      } 
      else
      {
        StateTransitionError error = new StateTransitionError(
            StateTransitionError.ErrorCode.INTERNAL, exception);
        _stateModel.rollbackOnError(_message, _notificationContext, error);
        map.put(ZNAttribute.CURRENT_STATE.toString(), "ERROR");
        _stateModel.updateState("ERROR");
      }
      map.put(Message.Attributes.STATE_UNIT_GROUP.toString(),
          _message.getStateUnitGroup());
      
      if(taskResult.isSucess() && toState.equals("DROPPED"))
      {// for "OnOfflineToDROPPED" message, we need to remove the resource key record from
        // the current state of the instance because the resource key is dropped.
        accessor.substractInstanceProperty(instanceName, 
          InstancePropertyType.CURRENTSTATES, _manager.getSessionId(), stateUnitGroup, currentStateDelta);
      }
      else
      {
      // based on task result update the current state of the node.
        accessor.updateInstanceProperty(instanceName,
          InstancePropertyType.CURRENTSTATES, _manager.getSessionId(), stateUnitGroup, currentStateDelta);
      }
    } 
    catch (Exception e)
    {
      logger.error("Error when updating the state ", e);
      StateTransitionError error = new StateTransitionError(
          StateTransitionError.ErrorCode.FRAMEWORK, e);
      _stateModel.rollbackOnError(_message, _notificationContext, error);
      _statusUpdateUtil.logError(_message, CMTaskHandler.class, e,
          "Error when update the state ", accessor);
    }
  }
  
  @Override
  public CMTaskResult call()
  {
    synchronized (_stateModel)
    {
      CMTaskResult taskResult = new CMTaskResult();
      ClusterDataAccessor accessor = _manager.getDataAccessor();
      String instanceName = _manager.getInstanceName();
      try
      {
        _statusUpdateUtil.logInfo(_message, CMTaskHandler.class,
            "Message handling task begin execute", accessor);
        try
        {
          prepareMessageExecution();
        }
        catch(ClusterManagerException e)
        {
          taskResult.setSuccess(false);
          taskResult.setMessage(e.getMessage());
          return taskResult;
        }
    
        Exception exception = null;
        try
        {
          invoke(accessor, taskResult, _message);
        } 
        catch(InterruptedException e)
        {
          throw e;
        }
        catch (Exception e)
        {
          String errorMessage = "Exception while executing a state transition task"
              + e;
          _statusUpdateUtil.logError(_message, CMTaskHandler.class, e,
              errorMessage, accessor);
          logger.error(errorMessage);
          taskResult.setSuccess(false);
          exception = e;
        }
        
        postExecutionMessage(taskResult, exception);
        
        return taskResult;
      }
      catch(InterruptedException e)
      {
        _statusUpdateUtil.logError(_message, CMTaskHandler.class, e,
            "State transition interrupted", accessor);
        logger.info("Message "+_message.getMsgId() + " is interrupted");
        
        StateTransitionError error = new StateTransitionError(
            StateTransitionError.ErrorCode.FRAMEWORK, e);
        _stateModel.rollbackOnError(_message, _notificationContext, error);
        return taskResult;
      }
      finally
      {
        accessor.removeInstanceProperty(instanceName,
            InstancePropertyType.MESSAGES, _message.getId());
        if(_executor != null)
        {
          _executor.reportCompletion(_message.getMsgId());
        }
      }
    }
  }

  private void invoke(ClusterDataAccessor accessor, CMTaskResult taskResult,
      Message message) throws IllegalAccessException, InvocationTargetException, InterruptedException
  {
    Method methodToInvoke = null;
    String fromState = _message.getFromState();
    String toState = _message.getToState();
    methodToInvoke = _transitionMethodFinder.getMethodForTransition(
        _stateModel.getClass(), fromState, toState, new Class[]
        { Message.class, NotificationContext.class });
    _statusUpdateUtil.logInfo(_message, CMTaskHandler.class,
        "Message handling invoking", accessor);
    if (methodToInvoke != null)
    {
      methodToInvoke.invoke(_stateModel, new Object[]
      { _message, _notificationContext });
      taskResult.setSuccess(true);
    } else
    {
      String errorMessage = "Unable to find method for transition from "
          + fromState + " to " + toState + "in " + _stateModel.getClass();
      logger.error(errorMessage);
      taskResult.setSuccess(false);
      
      System.out.println(errorMessage);
      _statusUpdateUtil.logError(_message, CMTaskHandler.class, errorMessage,
          accessor);
    }
  }
};
