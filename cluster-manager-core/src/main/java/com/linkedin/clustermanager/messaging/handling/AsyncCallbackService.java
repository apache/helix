package com.linkedin.clustermanager.messaging.handling;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.messaging.AsyncCallback;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;

public class AsyncCallbackService implements MessageHandlerFactory
{
  private final ConcurrentHashMap<String, AsyncCallback> _callbackMap 
      = new ConcurrentHashMap<String, AsyncCallback>();
  private static Logger _logger = Logger.getLogger(StateMachineEngine.class);
  
  public AsyncCallbackService()
  {
  }
  
  public void registerAsyncCallback( String correlationId,AsyncCallback callback)
  {
    if(_callbackMap.containsKey(correlationId))
    {
      _logger.warn("correlation id "+ correlationId +" already registered");
    }
    _logger.info("registering correlation id "+ correlationId);
    _callbackMap.put(correlationId, callback);
  }
  
  void verifyMessage(Message message)
  {
    if(!message.getMsgType().toString().equalsIgnoreCase(MessageType.TASK_REPLY.toString()))
    {
      String errorMsg = "Unexpected msg type for message "+message.getMsgId()
        +" type:" + message.getMsgType() + " Expected : " + MessageType.TASK_REPLY;
      _logger.error(errorMsg);
      throw new ClusterManagerException(errorMsg);
    }
    String correlationId = message.getCorrelationId();
    if(correlationId == null)
    {
      String errorMsg = "Message "+message.getMsgId()
        +" does not have correlation id";
      _logger.error(errorMsg);
      throw new ClusterManagerException(errorMsg);
    }
    
    if(!_callbackMap.containsKey(correlationId))
    {
      String errorMsg = "Message "+message.getMsgId()
        +" does not have correponding callback. Probably timed out already. Correlation id: " + correlationId;
      _logger.error(errorMsg);
      throw new ClusterManagerException(errorMsg);
    }
    _logger.info("Verified reply message " + message.getMsgId()+ " correlation:"+correlationId);
  }
  
  @Override
  public MessageHandler createHandler(Message message,
      NotificationContext context)
  {
    verifyMessage(message);
    return new AsyncCallbackMessageHandler(message.getCorrelationId());
  }

  @Override
  public String getMessageType()
  {
    return MessageType.TASK_REPLY.toString();
  }

  @Override
  public void reset()
  {
    
  }
  
  
  public class AsyncCallbackMessageHandler implements MessageHandler
  {
    private final String _correlationId;
    public AsyncCallbackMessageHandler(String correlationId)
    {
      _correlationId = correlationId;
    }
    
    @Override
    public void handleMessage(Message message, NotificationContext context, Map<String, String> resultMap)
        throws InterruptedException
    {
      verifyMessage(message);
      assert(_correlationId.equalsIgnoreCase(message.getCorrelationId()));
      _logger.info("invoking reply message " + message.getMsgId() + ", correlationid:"+ _correlationId);
      
      AsyncCallback callback = _callbackMap.get(_correlationId);
      synchronized(callback)
      {
        callback.onReply(message);
        if(callback.isDone())
        {
          _logger.info("Removing finished callback, correlationid:"+ _correlationId);
          _callbackMap.remove(_correlationId);
        }
      }
      
    }
  }
}
