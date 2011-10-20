package com.linkedin.clustermanager.messaging.handling;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.NotificationContext.Type;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.monitoring.ParticipantMonitor;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class CMTaskExecutor implements MessageListener
{
  // TODO: we need to further design how to throttle this.
  // From storage point of view, only bootstrap case is expensive
  // and we need to throttle, which is mostly IO / network bounded.
  private static final int MAX_PARALLEL_TASKS = 4;
  // TODO: create per-task type threadpool with customizable pool size
  protected final Map<String, Future<CMTaskResult>> _taskMap;
  private final Object _lock;
  private final StatusUpdateUtil _statusUpdateUtil;
  private final ParticipantMonitor _monitor;

  private final ConcurrentHashMap<String, MessageHandlerFactory> _handlerFactoryMap = new ConcurrentHashMap<String, MessageHandlerFactory>();

  private final ConcurrentHashMap<String, ExecutorService> _threadpoolMap = new ConcurrentHashMap<String, ExecutorService>();

  private static Logger logger = Logger.getLogger(CMTaskExecutor.class);

  public CMTaskExecutor()
  {
    _taskMap = new HashMap<String, Future<CMTaskResult>>();
    _lock = new Object();
    _statusUpdateUtil = new StatusUpdateUtil();
    _monitor = ParticipantMonitor.getInstance();
    startMonitorThread();
  }

  public void registerMessageHandlerFactory(String type,
      MessageHandlerFactory factory)
  {
    if (!_handlerFactoryMap.containsKey(type))
    {
      if (!type.equalsIgnoreCase(factory.getMessageType()))
      {
        throw new ClusterManagerException(
            "Message factory type mismatch. Type: " + type + " factory : "
                + factory.getMessageType());

      }
      _handlerFactoryMap.put(type, factory);
      _threadpoolMap
          .put(type, Executors.newFixedThreadPool(MAX_PARALLEL_TASKS));
      logger.info("adding msg factory for type " + type);
    } else
    {
      logger.warn("Ignoring duplicate msg factory for type " + type);
    }
  }

  public MessageHandlerFactory getMessageHanderFactory(String type)
  {
    return _handlerFactoryMap.get(type);
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
    assert (handler != null);
    synchronized (_lock)
    {
      try
      {
        logger.info("message.getMsgId() = " + message.getMsgId());
        _statusUpdateUtil.logInfo(message, CMTaskExecutor.class,
            "Message handling task scheduled", notificationContext.getManager()
                .getDataAccessor());

        CMTask task = new CMTask(message, notificationContext, handler, this);
        if (!_taskMap.containsKey(message.getMsgId()))
        {
          Future<CMTaskResult> future = _threadpoolMap
              .get(message.getMsgType()).submit(task);
          _taskMap.put(message.getMsgId(), future);
        } else
        {
          _statusUpdateUtil.logWarning(
              message,
              CMTaskExecutor.class,
              "Message handling task already sheduled for "
                  + message.getMsgId(), notificationContext.getManager()
                  .getDataAccessor());
        }
      } catch (Exception e)
      {
        String errorMessage = "Error while executing task" + e;
        logger.error("Error while executing task." + message, e);

        _statusUpdateUtil.logError(message, CMTaskExecutor.class, e,
            errorMessage, notificationContext.getManager().getDataAccessor());
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
        // is called. So state transition callbacks should implement logic to
        // return
        // if it is interrupted.
        if (future.cancel(true))
        {
          _statusUpdateUtil.logInfo(message, CMTaskExecutor.class, "Canceled "
              + message.getMsgId(), notificationContext.getManager()
              .getDataAccessor());
          _taskMap.remove(message.getMsgId());
        } else
        {
          _statusUpdateUtil.logInfo(message, CMTaskExecutor.class,
              "false when trying to cancel the message " + message.getMsgId(),
              notificationContext.getManager().getDataAccessor());
        }
      } else
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
      if (_taskMap.containsKey(msgId))
      {
        _taskMap.remove(msgId);
      } else
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
    future = pool.submit(new CMTask(null, null, null, null));
    Thread.currentThread().join();
    System.out.println(future.isDone());
  }

  @Override
  public void onMessage(String instanceName, List<ZNRecord> messages,
      NotificationContext changeContext)
  {
    ClusterManager manager = changeContext.getManager();
    ClusterDataAccessor client = manager.getDataAccessor();

    // If FINALIZE notification comes, reset all handler factories
    // TODO: see if we should have a separate notification call for resetting
    if (changeContext.getType() == Type.FINALIZE)
    {
      for (MessageHandlerFactory factory : _handlerFactoryMap.values())
      {
        factory.reset();
      }
    }
    if (messages == null || messages.size() == 0)
    {
      logger.info("No Messages to process");
      return;
    }
    // TODO: sort message based on timestamp
    for (ZNRecord record : messages)
    {
      Message message = new Message(record);
      // NO_OP messages are removed with nothing done. It is used to trigger the
      // onMessage() call if needed.
      if (message.getMsgType().equalsIgnoreCase(MessageType.NO_OP.toString()))
      {
        logger.info("Dropping NO-OP msg from " + message.getMsgSrc());
        client.removeProperty(PropertyType.MESSAGES, instanceName,
            message.getId());
        continue;
      }
      String sessionId = manager.getSessionId();
      String tgtSessionId = message.getTgtSessionId();
      if (sessionId.equals(tgtSessionId) || tgtSessionId.equals("*"))
      {
        MessageHandler handler = null;
        if ("new".equals(message.getMsgState()))
        {
          try
          {
            logger.info("Creating handler for message " + message.getMsgId());
            handler = createMessageHandler(message, changeContext);

            if (handler == null)
            {
              logger.warn("Message handler factory not found for message type "
                  + message.getMsgType());
              continue;
            }

            _statusUpdateUtil.logInfo(message, StateMachineEngine.class,
                "New Message", client);
            // update msgState to read
            message.setMsgState("read");
            message.setReadTimeStamp(new Date().getTime());

            client.updateProperty(PropertyType.MESSAGES, message.getRecord(),
                instanceName, message.getId());
            scheduleTask(message, handler, changeContext);
          } catch (Exception e)
          {
            String error = "Failed to create message handler for "
                + message.getMsgId() + " exception: " + e;

            _statusUpdateUtil.logError(message, StateMachineEngine.class, e,
                error, client);

            client.removeProperty(PropertyType.MESSAGES, instanceName,
                message.getId());
            continue;
          }
        } else
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
      } else
      {
        String warningMessage = "Session Id does not match.  current session id  Expected: "
            + sessionId + " sessionId from Message: " + tgtSessionId;
        logger.warn(warningMessage);
        client.removeProperty(PropertyType.MESSAGES, instanceName,
            message.getId());
        _statusUpdateUtil.logWarning(message, StateMachineEngine.class,
            warningMessage, client);
      }
    }

  }

  private MessageHandler createMessageHandler(Message message,
      NotificationContext changeContext)
  {
    String msgType = message.getMsgType().toString();

    MessageHandlerFactory handlerFactory = _handlerFactoryMap.get(msgType);

    if (handlerFactory == null)
    {
      logger.warn("Cannot find handler factory for msg type " + msgType
          + " message:" + message.getMsgId());
      return null;
    }

    return handlerFactory.createHandler(message, changeContext);

  }
}
