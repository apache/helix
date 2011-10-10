package com.linkedin.clustermanager.agent.zk;

import java.util.Map;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.messaging.handling.MessageHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;

public class DefaultControllerMessageHandlerFactory implements
    MessageHandlerFactory
{

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
    
    return new DefaultControllerMessageHandler();
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
  
  public static class DefaultControllerMessageHandler implements MessageHandler
  {

    @Override
    public void handleMessage(Message message, NotificationContext context,
        Map<String, String> resultMap) throws InterruptedException
    {
      String type = message.getMsgType();
      
      if(!type.equals(MessageType.CONTROLLER_MSG.toString()))
      {
        throw new ClusterManagerException("Unexpected msg type for message "+message.getMsgId()
            +" type:" + message.getMsgType());
      }
      
      resultMap.put("ControllerResult", "msg "+ message.getMsgId() + " from "+message.getMsgSrc() + " processed");
    }
    
  }
}
