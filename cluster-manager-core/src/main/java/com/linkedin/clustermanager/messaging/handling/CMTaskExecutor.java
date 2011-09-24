package com.linkedin.clustermanager.messaging.handling;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.monitoring.ParticipantMonitor;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class CMTaskExecutor implements MessageListener
{
  // TODO: we need to further design how to throttle this.
  // From storage point of view, only bootstrap case is expensive 
  // and we need to throttle, which is mostly IO / network bounded.
  private static final int MAX_PARALLEL_TASKS = 4;
  // TODO: create per-task type threadpool with customizable pool size 
  private final ExecutorService _pool;
  protected final Map<String, Future<CMTaskResult>> _taskMap;
  private final Object _lock;
  StatusUpdateUtil _statusUpdateUtil;
  ParticipantMonitor _monitor;
  
  private final ConcurrentHashMap<String, MessageHandlerFactory> _handlerFactoryMap 
  = new ConcurrentHashMap<String, MessageHandlerFactory>();

  private static Logger logger = Logger.getLogger(CMTaskExecutor.class);

  public CMTaskExecutor()
  {
    _taskMap = new HashMap<String, Future<CMTaskResult>>();
    _lock = new Object();
    _statusUpdateUtil = new StatusUpdateUtil();
    _pool = Executors.newFixedThreadPool(MAX_PARALLEL_TASKS);
    _monitor = ParticipantMonitor.getInstance();
    startMonitorThread();
  }
  
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory)
  {
    if(!_handlerFactoryMap.containsKey(type))
    {
      _handlerFactoryMap.put(type, factory);
      logger.warn("adding msg factory for type " + type);
    }
    else
    {
      logger.warn("Ignoring duplicate msg factory for type " + type);
    }
  }
  
  public void registerExternalMessageHandlerFactory(String type, MessageHandlerFactory factory)
  {
    registerMessageHandlerFactory(MessageType.USER_DEFINE_MSG+"."+type, factory);
  }
  
  public ParticipantMonitor getParticipantMonitor()
  {
    return _monitor;
  }
  
  private void startMonitorThread()
  {
    // start a thread which monitors the completions of task
  }

  public void scheduleTask(Message message, MessageHandler handler,
      NotificationContext notificationContext)
  {
    assert(handler != null);
    synchronized (_lock)
    {
      try
      {
        logger.info("message.getMsgId() = " + message.getMsgId());
        _statusUpdateUtil.logInfo(message, CMTaskExecutor.class,
            "Message handling task scheduled", notificationContext.getManager()
                .getDataAccessor());
        
        CMTaskHandler task = new CMTaskHandler(message, notificationContext, 
            handler, this);
        if (!_taskMap.containsKey(message.getMsgId()))
        {
          Future<CMTaskResult> future = _pool.submit(task);
          _taskMap.put(message.getMsgId(), future);
        } 
        else
        {
          _statusUpdateUtil.logWarning(message, CMTaskExecutor.class,
              "Message handling task already sheduled for " + message.getMsgId(),
              notificationContext.getManager().getDataAccessor());
        }
      } 
      catch (Exception e)
      {
        String errorMessage = "Error while executing task" + e;
        logger.error("Error while executing task." + message, e);

        _statusUpdateUtil.logError(message, CMTaskExecutor.class, e, errorMessage,
            notificationContext.getManager().getDataAccessor());
        // TODO add retry or update errors node
      }
    }
  }
  
  public void cancelTask(Message message, 
      NotificationContext notificationContext)
  {
    synchronized (_lock)
    {
      if (_taskMap.containsKey(message.getMsgId()))
      {
        _statusUpdateUtil.logInfo(message, CMTaskExecutor.class,
            "Trying to cancel the future for " + message.getMsgId(),
            notificationContext.getManager().getDataAccessor());
        Future<CMTaskResult> future = _taskMap.get(message.getMsgId());
        
        // If the thread is still running it will be interrupted if cancel(true)
        // is called. So state transition callbacks should implement logic to return 
        // if it is interrupted.
        if(future.cancel(true))
        {
          _statusUpdateUtil.logInfo(message, CMTaskExecutor.class,
              "Canceled " + message.getMsgId(),
              notificationContext.getManager().getDataAccessor());
          _taskMap.remove(message.getMsgId());
        }
        else
        {
          _statusUpdateUtil.logInfo(message, CMTaskExecutor.class,
              "false when trying to cancel the message " + message.getMsgId(),
              notificationContext.getManager().getDataAccessor());
        }
      } 
      else
      {
        _statusUpdateUtil.logWarning(message, CMTaskExecutor.class,
            "Future not found when trying to cancel " + message.getMsgId(),
            notificationContext.getManager().getDataAccessor());
      }
    }
  }

  protected void reportCompletion(String msgId)
  {
    synchronized (_lock)
    {
      logger.info("message " + msgId + " finished");
      if(_taskMap.containsKey(msgId))
      {
        _taskMap.remove(msgId);
      }
      else
      {
        logger.warn("message " + msgId + "not found in task map");
      }
    }
  }

  public static void main(String[] args) throws Exception
  {
    ExecutorService pool = Executors.newFixedThreadPool(MAX_PARALLEL_TASKS);
    Future<CMTaskResult> future;
    // pool.shutdown();
    // pool.awaitTermination(5, TimeUnit.SECONDS);
    future = pool.submit(new Callable<CMTaskResult>()
    {

      @Override
      public CMTaskResult call() throws Exception
      {
        System.out
            .println("CMTaskExecutor.main(...).new Callable() {...}.call()");
        return null;
      }

    });
    future = pool.submit(new CMTaskHandler(null, null, null, null));
    Thread.currentThread().join();
    System.out.println(future.isDone());
  }

  @Override
  public void onMessage(String instanceName, List<ZNRecord> messages,
      NotificationContext changeContext)
  {
    ClusterManager manager = changeContext.getManager();
    ClusterDataAccessor client = manager.getDataAccessor();
    if (messages == null || messages.size() == 0)
    {
      logger.info("No Messages to process");
      return;
    }
    //TODO: sort message based on timestamp
    for (ZNRecord record : messages)
    {
      Message message = new Message(record);
      if (message.getId() == null)
      {
        message.setId(message.getMsgId());
      }
      
      String sessionId = manager.getSessionId();
      String tgtSessionId = ((Message) message).getTgtSessionId();
      if (sessionId.equals(tgtSessionId) || tgtSessionId.equals("*"))
      {
        MessageHandler handler = null;
        if ("new".equals(message.getMsgState()))
        {
          try
          {
            logger.info("Creating handler for message "+ message.getMsgId());
            handler = createMessageHandler(message, changeContext);
            
            _statusUpdateUtil.logInfo(message, StateMachineEngine.class,
              "New Message", client);
            // update msgState to read
            message.setMsgState("read");
            message.setReadTimeStamp(new Date().getTime());
  
            client.updateInstanceProperty(instanceName,
                InstancePropertyType.MESSAGES, message.getId(),
                message.getRecord());
            scheduleTask(message, handler, changeContext);
          }
          catch(Exception e)
          {
            String error = "Failed to create message handler for " 
              + message.getMsgId() + " exception: " + e;
            
            _statusUpdateUtil.logError(message, StateMachineEngine.class, e,
                error, client);
            
            client.removeInstanceProperty(instanceName,
                InstancePropertyType.MESSAGES, message.getId());
            continue;
          }
        } 
        else
        {
          // This will happen because we dont delete the message as soon as we
          // read it.
          // We keep it until the current state is changed.
          // We will read the message again if there is a new message but we
          // check for the status and ignore if its already read
          logger.trace("Message already read" + message.getMsgId());
          // _statusUpdateUtil.logInfo(message, StateMachineEngine.class,
          // "Message already read", client);
        }

      } 
      else
      {
        String warningMessage = "Session Id does not match.  current session id  Expected: "
            + sessionId + " sessionId from Message: " + tgtSessionId;
        logger.warn(warningMessage);
        client.removeInstanceProperty(instanceName,
            InstancePropertyType.MESSAGES, message.getId());
        _statusUpdateUtil.logWarning(message, StateMachineEngine.class,
            warningMessage, client);
      }
    }
    
  }

  private MessageHandler createMessageHandler(Message message,
      NotificationContext changeContext)
  {
    String msgType = message.getMsgType().toString();
    if(msgType.equalsIgnoreCase(MessageType.USER_DEFINE_MSG.toString()))
    {
      msgType = msgType + "."+message.getMsgSubType();
    }
    
    MessageHandlerFactory handlerFactory = _handlerFactoryMap.get(msgType);
    
    if(handlerFactory == null)
    {
      throw new ClusterManagerException("Cannot find handler factory for msg type " + msgType
          +" message:" + message.getMsgId());
    }
    
    return handlerFactory.createHandler(message, changeContext);
    
  }
}
