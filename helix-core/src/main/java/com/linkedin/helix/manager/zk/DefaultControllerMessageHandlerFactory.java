package com.linkedin.helix.manager.zk;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.messaging.handling.AsyncCallbackService;
import com.linkedin.helix.messaging.handling.HelixTaskResult;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorCode;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorType;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;

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
      throw new HelixException("Unexpected msg type for message "+message.getMsgId()
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
    public HelixTaskResult handleMessage() throws InterruptedException
    {
      String type = _message.getMsgType();
      HelixTaskResult result = new HelixTaskResult();
      if(!type.equals(MessageType.CONTROLLER_MSG.toString()))
      {
        throw new HelixException("Unexpected msg type for message "+_message.getMsgId()
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
