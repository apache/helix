package com.linkedin.clustermanager.agent.zk;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.messaging.handling.AsyncCallbackService;
import com.linkedin.clustermanager.messaging.handling.CMTaskResult;
import com.linkedin.clustermanager.messaging.handling.MessageHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandler.ErrorCode;
import com.linkedin.clustermanager.messaging.handling.MessageHandler.ErrorType;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;

public class DefaultControllerMessageHandlerFactory implements
    MessageHandlerFactory
{
  private static Logger _logger = Logger.getLogger(DefaultControllerMessageHandlerFactory.class);
  @Override
  public MessageHandler createHandler(Message message,
      NotificationContext context)
  {
    String type = message.getMsgType();
    
    if(!type.equals(getMessageType()))
    {
      throw new ClusterManagerException("Unexpected msg type for message "+message.getMsgId()
          +" type:" + message.getMsgType());
    }
    
    return new DefaultControllerMessageHandler(message, context);
  }

  @Override
  public String getMessageType()
  {
    return MessageType.CONTROLLER_MSG.toString();
  }

  @Override
  public void reset()
  {

  }
  
  public static class DefaultControllerMessageHandler extends MessageHandler
  {
    public DefaultControllerMessageHandler(Message message,
        NotificationContext context)
    {
      super(message, context);
      // TODO Auto-generated constructor stub
    }

    @Override
    public CMTaskResult handleMessage() throws InterruptedException
    {
      String type = _message.getMsgType();
      CMTaskResult result = new CMTaskResult();
      if(!type.equals(MessageType.CONTROLLER_MSG.toString()))
      {
        throw new ClusterManagerException("Unexpected msg type for message "+_message.getMsgId()
            +" type:" + _message.getMsgType());
      }
      result.getTaskResultMap().put("ControllerResult", "msg "+ _message.getMsgId() + " from "+_message.getMsgSrc() + " processed");
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type)
    {
      _logger.error("Message handling pipeline get an exception. MsgId:" + _message.getMsgId(), e);
    }
  }
}
