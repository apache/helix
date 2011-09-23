package com.linkedin.clustermanager.messaging.handling;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class CMTaskHandler implements Callable<CMTaskResult>
{
  private static Logger logger = Logger.getLogger(CMTaskHandler.class);
  private final Message _message;
  private final MessageHandler _handler;
  private final NotificationContext _notificationContext;
  private final ClusterManager _manager;
  StatusUpdateUtil _statusUpdateUtil;
  CMTaskExecutor _executor;

  public CMTaskHandler(Message message, NotificationContext notificationContext,
      MessageHandler handler, CMTaskExecutor executor) throws Exception
  {
    this._notificationContext = notificationContext;
    this._message = message;
    this._handler = handler;
    this._manager = notificationContext.getManager();
    _statusUpdateUtil = new StatusUpdateUtil();
    _executor = executor;
    if (!validateTask())
    {
      String errorMessage = "Invalid Message, ensure that message: " + message
          + " has all the required fields: "
          + Arrays.toString(Message.Attributes.values());

      _statusUpdateUtil.logError(_message, CMTaskHandler.class, errorMessage,
          _manager.getDataAccessor());
      logger.error(errorMessage);
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
  
  
  @Override
  public CMTaskResult call()
  {
    CMTaskResult taskResult = new CMTaskResult();
    taskResult.setSuccess(false);
    ClusterDataAccessor accessor = _manager.getDataAccessor();
    String instanceName = _manager.getInstanceName();
    try
    {
      _statusUpdateUtil.logInfo(_message, CMTaskHandler.class,
          "Message handling task begin execute", accessor);
      _message.setExecuteStartTimeStamp(new Date().getTime());

      try
      {
        _handler.handleMessage(_message, _notificationContext);
        taskResult.setSuccess(true);
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
        taskResult.setMessage(e.getMessage());
      }
    }  
    catch(InterruptedException e)
    {
      _statusUpdateUtil.logError(_message, CMTaskHandler.class, e,
          "State transition interrupted", accessor);
      logger.info("Message "+_message.getMsgId() + " is interrupted");
    }
    finally
    {
      accessor.removeInstanceProperty(instanceName,
          InstancePropertyType.MESSAGES, _message.getId());
      if(_executor != null)
      {
        _executor.reportCompletion(_message.getMsgId());
      }
      return taskResult;
    } 
  }
  
};
