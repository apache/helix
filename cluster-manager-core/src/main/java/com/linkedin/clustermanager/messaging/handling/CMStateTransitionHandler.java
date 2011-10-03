package com.linkedin.clustermanager.messaging.handling;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.CMConstants.ZNAttribute;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.monitoring.StateTransitionContext;
import com.linkedin.clustermanager.monitoring.StateTransitionDataPoint;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelParser;
import com.linkedin.clustermanager.participant.statemachine.StateTransitionError;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class CMStateTransitionHandler implements MessageHandler
{
  private static Logger logger = Logger.getLogger(CMStateTransitionHandler.class);
  private final StateModel _stateModel;
  StatusUpdateUtil _statusUpdateUtil;
  private StateModelParser _transitionMethodFinder;

  public CMStateTransitionHandler(StateModel stateModel)
  {
    this._stateModel = stateModel;
    _statusUpdateUtil = new StatusUpdateUtil();
    _transitionMethodFinder = new StateModelParser();
  }

  // TODO replace with util from espresso or linkedin
  private boolean isNullorEmpty(String data)
  {
    return data == null || data.length() == 0 || data.trim().length() == 0;
  }

  private boolean validateMessage(Message message)
  {
    boolean isValid = isNullorEmpty(message.getFromState())
        || isNullorEmpty(message.getToState())
        || isNullorEmpty(message.getToState())
        || isNullorEmpty(message.getStateUnitKey())
        || isNullorEmpty(message.getToState());
    return !isValid;
  }
  
  private void prepareMessageExecution(ClusterManager manager, Message message) throws ClusterManagerException
  {
    if (!validateMessage(message))
    {
      String errorMessage = "Invalid Message, ensure that message: " + message
          + " has all the required fields: "
          + Arrays.toString(Message.Attributes.values());

      _statusUpdateUtil.logError(message, CMStateTransitionHandler.class, errorMessage,
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

    List<ZNRecord> stateModelDefs = accessor.getClusterPropertyList(ClusterPropertyType.STATEMODELDEFS);
    String initStateValue = "OFFLINE";
    if (stateModelDefs != null)
    {
      StateModelDefinition stateModelDef = lookupStateModel(message.getStateModelDef(), stateModelDefs);
    
      if (stateModelDef != null)
      {
        initStateValue = stateModelDef.getInitialState();
      }
    }
    
    ZNRecord currentState = accessor.getInstanceProperty(instanceName,
        InstancePropertyType.CURRENTSTATES, manager.getSessionId(), stateUnitGroup);
    // Set a empty current state record if it is null
    if (currentState == null)
    {
      currentState = new ZNRecord();
      currentState.setId(stateUnitGroup);
      currentState.setSimpleField(
          CMConstants.ZNAttribute.SESSION_ID.toString(),
          manager.getSessionId());
      accessor.updateInstanceProperty(instanceName,
          InstancePropertyType.CURRENTSTATES, manager.getSessionId(), stateUnitGroup, currentState);
    }
    Map<String, String> map;
    
    // For resource unit that does not have state before, init it to offline
    if(!currentState.getMapFields().containsKey(stateUnitKey))
    {
       map = new HashMap<String, String>();
       map.put(ZNAttribute.CURRENT_STATE.toString(), initStateValue);  // "OFFLINE");
       
       ZNRecord currentStateDelta = new ZNRecord();
       currentStateDelta.setMapField(stateUnitKey, map);
       
       logger.info("Setting initial state for partition: " + stateUnitKey + " to offline");
       
       accessor.updateInstanceProperty(instanceName,
         InstancePropertyType.CURRENTSTATES, manager.getSessionId(), stateUnitGroup, currentStateDelta);
    }
    
    // Set the state model def to current state
    if(!currentState.getSimpleFields().containsKey(Message.Attributes.STATE_MODEL_DEF.toString()))
    {
      if(message.getRecord().getSimpleFields().containsKey(Message.Attributes.STATE_MODEL_DEF.toString()))
      {
        logger.info("Setting state model def on current state: " + message.getStateModelDef());
        ZNRecord currentStateDelta = new ZNRecord();
        currentStateDelta.setSimpleField(Message.Attributes.STATE_MODEL_DEF.toString(), message.getStateModelDef());
        
        accessor.updateInstanceProperty(instanceName,
            InstancePropertyType.CURRENTSTATES, manager.getSessionId(), stateUnitGroup, currentStateDelta);
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
          + ", message expected:" + fromState
          +" partition: " + message.getStateUnitKey();
      
      _statusUpdateUtil.logError(message, CMStateTransitionHandler.class, errorMessage,
          accessor);
      logger.error(errorMessage);
      throw new ClusterManagerException(errorMessage);  
    }
  }
  
  
  
  void postExecutionMessage(ClusterManager manager, Message message, NotificationContext context, CMTaskResult taskResult, Exception exception) throws InterruptedException
  {
    ClusterDataAccessor accessor = manager.getDataAccessor();
    try
    {
      String stateUnitKey = message.getStateUnitKey();
      String stateUnitGroup = message.getStateUnitGroup();
      String instanceName = manager.getInstanceName();
      
      String fromState = message.getFromState();
      String toState = message.getToState();
      ZNRecord currentState = accessor.getInstanceProperty(instanceName,
          InstancePropertyType.CURRENTSTATES, manager.getSessionId(), stateUnitGroup);
      
      Map<String, String> map = new HashMap<String, String>();
      if(currentState != null)
      {
        map = currentState.getMapField(stateUnitKey);
      }
      else
      {
        logger.warn("currentState is null. Storage node should be working with file based clm.");
      }
      map.put(Message.Attributes.STATE_UNIT_GROUP.toString(),
          message.getStateUnitGroup());
      
      
      // TODO verify that fromState is same as currentState this task
      // was
      // called at.
      // Verify that no one has edited this field
      ZNRecord currentStateDelta = new ZNRecord();
      if (taskResult.isSucess())
      { 
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
        _stateModel.rollbackOnError(message, context, error);
        map.put(ZNAttribute.CURRENT_STATE.toString(), "ERROR");
        _stateModel.updateState("ERROR");
      }
      
      map.put(Message.Attributes.STATE_UNIT_GROUP.toString(),
          message.getStateUnitGroup());
      
      if(taskResult.isSucess() && toState.equals("DROPPED"))
      {// for "OnOfflineToDROPPED" message, we need to remove the resource key record from
        // the current state of the instance because the resource key is dropped.
        accessor.substractInstanceProperty(instanceName, 
          InstancePropertyType.CURRENTSTATES, manager.getSessionId(), stateUnitGroup, currentStateDelta);
      }
      else
      {
      // based on task result update the current state of the node.
        accessor.updateInstanceProperty(instanceName,
          InstancePropertyType.CURRENTSTATES, manager.getSessionId(), stateUnitGroup, currentStateDelta);
      }
    } 
    catch (Exception e)
    {
      logger.error("Error when updating the state ", e);
      StateTransitionError error = new StateTransitionError(
          StateTransitionError.ErrorCode.FRAMEWORK, e);
      _stateModel.rollbackOnError(message, context, error);
      _statusUpdateUtil.logError(message, CMStateTransitionHandler.class, e,
          "Error when update the state ", accessor);
    }
  }
  
  // TODO: decide if handleMessage() should return a value CMTaskResult; this part need to integrate with
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
        _statusUpdateUtil.logInfo(message, CMStateTransitionHandler.class,
            "Message handling task begin execute", accessor);
        message.setExecuteStartTimeStamp(new Date().getTime());
        try
        {
          prepareMessageExecution(manager, message);
        }
        catch(ClusterManagerException e)
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
        catch(InterruptedException e)
        {
          throw e;
        }
        catch (Exception e)
        {
          String errorMessage = "Exception while executing a state transition task"
              + e;
          _statusUpdateUtil.logError(message, CMStateTransitionHandler.class, e,
              errorMessage, accessor);
          logger.error(errorMessage);
          taskResult.setSuccess(false);
          exception = e;
        }
        
        postExecutionMessage(manager, message, context, taskResult, exception);
        
        return taskResult;
      }
      catch(InterruptedException e)
      {
        _statusUpdateUtil.logError(message, CMStateTransitionHandler.class, e,
            "State transition interrupted", accessor);
        logger.info("Message "+message.getMsgId() + " is interrupted");
        
        StateTransitionError error = new StateTransitionError(
            StateTransitionError.ErrorCode.FRAMEWORK, e);
        _stateModel.rollbackOnError(message, context, error);
        return taskResult;
      }
    }
  }

  private void invoke(ClusterDataAccessor accessor, NotificationContext context, CMTaskResult taskResult,
      Message message) throws IllegalAccessException, InvocationTargetException, InterruptedException
  {
    Method methodToInvoke = null;
    String fromState = message.getFromState();
    String toState = message.getToState();
    methodToInvoke = _transitionMethodFinder.getMethodForTransition(
        _stateModel.getClass(), fromState, toState, new Class[]
        { Message.class, NotificationContext.class });
    _statusUpdateUtil.logInfo(message, CMStateTransitionHandler.class,
        "Message handling invoking", accessor);
    if (methodToInvoke != null)
    {
      methodToInvoke.invoke(_stateModel, new Object[]
      { message, context });
      taskResult.setSuccess(true);
    } else
    {
      String errorMessage = "Unable to find method for transition from "
          + fromState + " to " + toState + "in " + _stateModel.getClass();
      logger.error(errorMessage);
      taskResult.setSuccess(false);
      
      System.out.println(errorMessage);
      _statusUpdateUtil.logError(message, CMStateTransitionHandler.class, errorMessage,
          accessor);
    }
  }
  
  private StateModelDefinition lookupStateModel(String stateModelDefRef, List<ZNRecord> stateModelDefs)
  {
    for (ZNRecord record : stateModelDefs)
    {
      if (record.getId().equals(stateModelDefRef))
      {
        return new StateModelDefinition(record);
      }
    }
    return null;
  }

  @Override
  public void handleMessage(Message message, NotificationContext context, Map<String, String> resultMap)
      throws InterruptedException
  {
    handleMessageInternal( message,  context);
    
  }
};
