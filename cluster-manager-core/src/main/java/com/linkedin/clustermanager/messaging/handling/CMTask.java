package com.linkedin.clustermanager.messaging.handling;

import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.Criteria;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.monitoring.StateTransitionContext;
import com.linkedin.clustermanager.monitoring.StateTransitionDataPoint;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class CMTask implements Callable<CMTaskResult>
{
  private static Logger logger = Logger.getLogger(CMTask.class);
  private final Message _message;
  private final MessageHandler _handler;
  private final NotificationContext _notificationContext;
  private final ClusterManager _manager;
  StatusUpdateUtil _statusUpdateUtil;
  CMTaskExecutor _executor;

  public CMTask(Message message, NotificationContext notificationContext,
      MessageHandler handler, CMTaskExecutor executor) throws Exception
  {
    this._notificationContext = notificationContext;
    this._message = message;
    this._handler = handler;
    this._manager = notificationContext.getManager();
    _statusUpdateUtil = new StatusUpdateUtil();
    _executor = executor;
  }

  @Override
  public CMTaskResult call()
  {
    CMTaskResult taskResult = new CMTaskResult();
    taskResult.setSuccess(false);
    ClusterDataAccessor accessor = _manager.getDataAccessor();
    String instanceName = _manager.getInstanceName();
    try
    {
      _statusUpdateUtil.logInfo(_message, CMTask.class,
          "Message handling task begin execute", accessor);
      _message.setExecuteStartTimeStamp(new Date().getTime());

      try
      {
        _handler.handleMessage(_message, _notificationContext, taskResult.getTaskResultMap());
        taskResult.setSuccess(true);
        
        // If the message requires reply, send reply message
        if(_message.getCorrelationId() != null)
        {
          logger.info("Sending reply for message "+ _message.getCorrelationId());
          _statusUpdateUtil.logInfo(_message, CMTask.class, "Sending reply", accessor);
          
          Message replyMessage = Message.createReplyMessage(_message, _manager.getInstanceName(), taskResult.getTaskResultMap());
          Criteria recipientCriteria = new Criteria();
          recipientCriteria.setInstanceName(replyMessage.getTgtName());
          _manager.getMessagingService().send(recipientCriteria, replyMessage);
        }
      }
      catch(InterruptedException e)
      {
        throw e;
      }
      catch (Exception e)
      {
        String errorMessage = "Exception while executing a state transition task"
            + e;
        _statusUpdateUtil.logError(_message, CMTask.class, e,
            errorMessage, accessor);
        logger.error(errorMessage);
        taskResult.setSuccess(false);
        taskResult.setMessage(e.getMessage());
      }
    }  
    catch(InterruptedException e)
    {
      _statusUpdateUtil.logError(_message, CMTask.class, e,
          "State transition interrupted", accessor);
      logger.info("Message "+_message.getMsgId() + " is interrupted");
    }
    finally
    {
      reportMessgeStat(_manager, _message, taskResult);
      accessor.removeInstanceProperty(instanceName,
          InstancePropertyType.MESSAGES, _message.getId());
      if(_executor != null)
      {
        _executor.reportCompletion(_message.getMsgId());
      }
      return taskResult;
    } 
  }
  

  private void reportMessgeStat(ClusterManager manager, Message message, CMTaskResult taskResult)
  {
    // report stat
    if(message.getMsgType() != MessageType.STATE_TRANSITION)
    {
      return;
    }
    long now = new Date().getTime();
    long msgReadTime = message.getReadTimeStamp();
    long msgExecutionStartTime = message.getExecuteStartTimeStamp();
    if(msgReadTime != 0 && msgExecutionStartTime != 0)
    {
      long totalDelay = now - msgReadTime;
      long executionDelay = now - msgExecutionStartTime;
      if(totalDelay > 0 && executionDelay > 0)
      {
        String fromState = message.getFromState();
        String toState = message.getToState();
        String transition = fromState + "--" + toState;
        
        StateTransitionContext cxt = new StateTransitionContext(
            manager.getClusterName(),
            manager.getInstanceName(),
            message.getStateUnitGroup(),
          transition
          );
        
        StateTransitionDataPoint data = new StateTransitionDataPoint(totalDelay, executionDelay, taskResult.isSucess());
        _executor.getParticipantMonitor().reportTransitionStat(cxt, data);
      }
    }
    else
    {
      logger.warn("message read time and start execution time not recorded.");
    }
  }

};
