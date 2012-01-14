package com.linkedin.clustermanager.messaging.handling;

import java.nio.channels.ClosedByInterruptException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.Criteria;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.messaging.handling.MessageHandler.ErrorCode;
import com.linkedin.clustermanager.messaging.handling.MessageHandler.ErrorType;
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
  volatile boolean _isTimeout = false;
  
  public class TimeoutCancelTask extends TimerTask
  {
    CMTaskExecutor _executor;
    Message _message;
    NotificationContext _context;
    public TimeoutCancelTask(CMTaskExecutor executor, Message message, NotificationContext context)
    {
      _executor = executor;
      _message = message;
      _context = context;
    }
    @Override
    public void run()
    {
      _isTimeout = true;
      logger.warn("Message time out, canceling. id:" + _message.getMsgId() + " timeout : " + _message.getExecutionTimeout());
      _executor.cancelTask(_message, _context);
    }
    
  }

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
    // Start the timeout TimerTask, if necessary
    Timer timer = null;
    if(_message.getExecutionTimeout() > 0)
    {
      timer = new Timer();
      timer.schedule(new TimeoutCancelTask(_executor, _message, _notificationContext),
            _message.getExecutionTimeout());
      logger.info("Message starts with timeout " + _message.getExecutionTimeout() + " MsgId:"+_message.getMsgId());
    }
    else
    {
      logger.info("Message does not have timeout. MsgId:"+_message.getMsgId());
    }
    
    CMTaskResult taskResult = new CMTaskResult();
    
    Exception exception = null;
    ErrorType type = null;
    ErrorCode code = null;
    
    ClusterDataAccessor accessor = _manager.getDataAccessor();
    _statusUpdateUtil.logInfo(_message, CMTask.class,
        "Message handling task begin execute", accessor);
    _message.setExecuteStartTimeStamp(new Date().getTime());

    // Handle the message
    try
    {
      taskResult = _handler.handleMessage();
      exception = taskResult.getException();
    } 
    catch (InterruptedException e)
    {
      _statusUpdateUtil.logError(_message, CMTask.class, e,
        "State transition interrupted, timeout:" + _isTimeout, accessor);
      logger.info("Message " + _message.getMsgId() + " is interrupted");
      taskResult.setInterrupted(true);
      taskResult.setException(e);
      exception = e;
    } 
    catch (Exception e)
    {
      String errorMessage = "Exception while executing a message. " + e + " msgId: "+_message.getMsgId() + " type: "+_message.getMsgType();
      logger.error(errorMessage, e);
      _statusUpdateUtil.logError(_message, CMTask.class, e, errorMessage, accessor);
      taskResult.setSuccess(false);
      taskResult.setException(e);
      taskResult.setMessage(e.getMessage());
      exception = e;
    }
    
    // Cancel the timer since the handling is done
    // it is fine if the TimerTask for canceling is called already
    if(timer != null)
    {
      timer.cancel();
    }
    
    if(taskResult.isSucess())
    {
      _statusUpdateUtil.logInfo(_message, _handler.getClass(),
        "Message handling task completed successfully", accessor);
      logger.info("Message "+_message.getMsgId()+" completed.");
    }
    else if(taskResult.isInterrupted())
    {
      logger.info("Message "+_message.getMsgId() +" is interrupted");
      code = ErrorCode.CANCEL;
      if(_isTimeout)
      {
        int retryCount = _message.getRetryCount();
        logger.info("Message timeout, retry count: "+ retryCount + " MSGID:" + _message.getMsgId());
        _statusUpdateUtil.logInfo(_message, _handler.getClass(),
            "Message handling task timeout, retryCount:" + retryCount, accessor);
        // Notify the handler that timeout happens, and the number of retries left
        // In case timeout happens (time out and also interrupted)
        // we should retry the execution of the message by re-schedule it in 
        if(retryCount > 0)
        {
          _message.setRetryCount(retryCount - 1);
          _executor.scheduleTask(_message, _handler, _notificationContext);
          return taskResult;
        } 
      }
    }
    else // logging for errors
    {
      String errorMsg = "Message execution failed. msgId: "+_message.getMsgId() + taskResult.getMessage();
      if(exception != null)
      {
        errorMsg += exception;
      }
      logger.error(errorMsg, exception);
      _statusUpdateUtil.logError(_message, _handler.getClass(), errorMsg, accessor);
    }
    
    // Post-processing for the finished task
    try
    {
      _executor.reportCompletion(_message.getMsgId());
      reportMessageStat(_manager, _message, taskResult);
      removeMessageFromZk(accessor, _message);
      sendReply(accessor, _message, taskResult);
    }
    catch(Exception e)
    {
      String errorMessage = "Exception after executing a message, msgId: "+_message.getMsgId() + e;
      logger.error(errorMessage);
      _statusUpdateUtil.logError(_message, CMTask.class, errorMessage, accessor);
      exception = e;
      type = ErrorType.FRAMEWORK;
      code = ErrorCode.ERROR;
    }
    // 
    finally
    {
      // Notify the handler about any error happened in the handling procedure, so that 
      // the handler have chance to finally cleanup
      if(exception != null)
      {
        _handler.onError(exception, code, type);
      }
      return taskResult; 
    }
  }

  private void removeMessageFromZk(ClusterDataAccessor accessor, Message message)
  {
    if (message.getTgtName().equalsIgnoreCase("controller"))
    {
      accessor
          .removeProperty(PropertyType.MESSAGES_CONTROLLER, message.getId());
    } else
    {
      accessor.removeProperty(PropertyType.MESSAGES,
          _manager.getInstanceName(), message.getId());
    }
  }

  private void sendReply(ClusterDataAccessor accessor, Message message,
      CMTaskResult taskResult)
  {
    if (_message.getCorrelationId() != null
        && !message.getMsgType().equals(MessageType.TASK_REPLY.toString()))
    {
      logger.info("Sending reply for message " + message.getCorrelationId());
      _statusUpdateUtil.logInfo(message, CMTask.class, "Sending reply",
          accessor);

      taskResult.getTaskResultMap().put("SUCCESS", "" + taskResult.isSucess());
      taskResult.getTaskResultMap().put("INTERRUPTED", "" + taskResult.isInterrupted());
      if (!taskResult.isSucess())
      {
        taskResult.getTaskResultMap().put("ERRORINFO", taskResult.getMessage());
      }
      Message replyMessage = Message.createReplyMessage(_message,
          _manager.getInstanceName(), taskResult.getTaskResultMap());
      Criteria recipientCriteria = new Criteria();
      recipientCriteria.setInstanceName(replyMessage.getTgtName());

      recipientCriteria.setRecipientInstanceType(message.getMsgSrc()
          .equalsIgnoreCase("controller") ? InstanceType.CONTROLLER
          : InstanceType.PARTICIPANT);
      recipientCriteria.setSessionSpecific(true);
      int nMsgs = _manager.getMessagingService().send(recipientCriteria,
          replyMessage);
      _statusUpdateUtil.logInfo(message, CMTask.class, nMsgs
          + " msgs replied to " + replyMessage.getTgtName(), accessor);
    }
  }

  private void reportMessageStat(ClusterManager manager, Message message,
      CMTaskResult taskResult)
  {
    // report stat
    if (!message.getMsgType().equals(MessageType.STATE_TRANSITION.toString()))
    {
      return;
    }
    long now = new Date().getTime();
    long msgReadTime = message.getReadTimeStamp();
    long msgExecutionStartTime = message.getExecuteStartTimeStamp();
    if (msgReadTime != 0 && msgExecutionStartTime != 0)
    {
      long totalDelay = now - msgReadTime;
      long executionDelay = now - msgExecutionStartTime;
      if (totalDelay > 0 && executionDelay > 0)
      {
        String fromState = message.getFromState();
        String toState = message.getToState();
        String transition = fromState + "--" + toState;

        StateTransitionContext cxt = new StateTransitionContext(
            manager.getClusterName(), manager.getInstanceName(),
            message.getStateUnitGroup(), transition);

        StateTransitionDataPoint data = new StateTransitionDataPoint(
            totalDelay, executionDelay, taskResult.isSucess());
        _executor.getParticipantMonitor().reportTransitionStat(cxt, data);
      }
    } else
    {
      logger.warn("message read time and start execution time not recorded.");
    }
  }

};
