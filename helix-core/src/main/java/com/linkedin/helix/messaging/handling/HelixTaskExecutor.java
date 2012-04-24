/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.messaging.handling;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.Attributes;
import com.linkedin.helix.model.Message.MessageState;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.monitoring.ParticipantMonitor;
import com.linkedin.helix.participant.HelixStateMachineEngine;
import com.linkedin.helix.util.StatusUpdateUtil;

public class HelixTaskExecutor implements MessageListener
{
  // TODO: we need to further design how to throttle this.
  // From storage point of view, only bootstrap case is expensive
  // and we need to throttle, which is mostly IO / network bounded.
  private static final int MAX_PARALLEL_TASKS = 4;
  // TODO: create per-task type threadpool with customizable pool size
  protected final Map<String, Future<HelixTaskResult>> _taskMap;
  private final Object _lock;
  private final StatusUpdateUtil _statusUpdateUtil;
  private final ParticipantMonitor _monitor;

  final ConcurrentHashMap<String, MessageHandlerFactory> _handlerFactoryMap = new ConcurrentHashMap<String, MessageHandlerFactory>();

  final ConcurrentHashMap<String, ExecutorService> _threadpoolMap = new ConcurrentHashMap<String, ExecutorService>();

  private static Logger logger = Logger.getLogger(HelixTaskExecutor.class);

  public HelixTaskExecutor()
  {
    _taskMap = new HashMap<String, Future<HelixTaskResult>>();
    _lock = new Object();
    _statusUpdateUtil = new StatusUpdateUtil();
    _monitor = new ParticipantMonitor();
    startMonitorThread();
  }

  public void registerMessageHandlerFactory(String type,
      MessageHandlerFactory factory)
  {
    if (!_handlerFactoryMap.containsKey(type))
    {
      if (!type.equalsIgnoreCase(factory.getMessageType()))
      {
        throw new HelixException(
            "Message factory type mismatch. Type: " + type + " factory : "
                + factory.getMessageType());

      }
      _handlerFactoryMap.put(type, factory);
      _threadpoolMap
          .put(type, Executors.newFixedThreadPool(MAX_PARALLEL_TASKS));
      logger.info("adding msg factory for type " + type);
    }
    else
    {
      logger.error("Ignoring duplicate msg handler factory for type " + type);
    }
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
        _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class,
            "Message handling task scheduled", notificationContext.getManager()
                .getDataAccessor());

        HelixTask task = new HelixTask(message, notificationContext, handler, this);
        if (!_taskMap.containsKey(message.getMsgId()))
        {
          Future<HelixTaskResult> future = _threadpoolMap
              .get(message.getMsgType()).submit(task);
          _taskMap.put(message.getMsgId(), future);
        } else
        {
          _statusUpdateUtil.logWarning(
              message,
              HelixTaskExecutor.class,
              "Message handling task already sheduled for "
                  + message.getMsgId(), notificationContext.getManager()
                  .getDataAccessor());
        }
      } catch (Exception e)
      {
        String errorMessage = "Error while executing task" + e;
        logger.error("Error while executing task." + message, e);

        _statusUpdateUtil.logError(message, HelixTaskExecutor.class, e,
            errorMessage, notificationContext.getManager().getDataAccessor());
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
        _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class,
            "Trying to cancel the future for " + message.getMsgId(),
            notificationContext.getManager().getDataAccessor());
        Future<HelixTaskResult> future = _taskMap.get(message.getMsgId());

        // If the thread is still running it will be interrupted if cancel(true)
        // is called. So state transition callbacks should implement logic to
        // return
        // if it is interrupted.
        if (future.cancel(true))
        {
          _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class, "Canceled "
              + message.getMsgId(), notificationContext.getManager()
              .getDataAccessor());
          _taskMap.remove(message.getMsgId());
        } else
        {
          _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class,
              "false when trying to cancel the message " + message.getMsgId(),
              notificationContext.getManager().getDataAccessor());
        }
      } else
      {
        _statusUpdateUtil.logWarning(message, HelixTaskExecutor.class,
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
    Future<HelixTaskResult> future;
    future = pool.submit(new Callable<HelixTaskResult>()
    {

      @Override
      public HelixTaskResult call() throws Exception
      {
        System.out
            .println("CMTaskExecutor.main(...).new Callable() {...}.call()");
        return null;
      }

    });
    future = pool.submit(new HelixTask(null, null, null, null));
    Thread.currentThread().join();
    System.out.println(future.isDone());
  }

  @Override
  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext)
  {
    // If FINALIZE notification comes, reset all handler factories
    // and terminate all the thread pools
    // TODO: see if we should have a separate notification call for resetting
    if (changeContext.getType() == Type.FINALIZE)
    {
      logger.info("Get FINALIZE notification");
      for (MessageHandlerFactory factory : _handlerFactoryMap.values())
      {
        factory.reset();
      }
      return;
    }

    HelixManager manager = changeContext.getManager();
    DataAccessor accessor = manager.getDataAccessor();

    if (messages == null || messages.size() == 0)
    {
      logger.info("No Messages to process");
      return;
    }

    // Sort message based on creation timestamp
    class MessageTimeComparator implements Comparator<Message>
    {
      @Override
      public int compare(Message message1, Message message2)
      {
        long messageCreateTime1 = 0, messageCreateTime2 = 0;
        String time1 = message1.getRecord().getSimpleField(Attributes.CREATE_TIMESTAMP.toString());
        String time2 = message2.getRecord().getSimpleField(Attributes.CREATE_TIMESTAMP.toString());
        try
        {
          messageCreateTime1 = Long.parseLong(time1);
        }
        catch(Exception e)
        {
          logger.warn("failed to parse creation time for msg "+ message1.getId());
        }
        try
        {
          messageCreateTime2 = Long.parseLong(time2);
        }
        catch(Exception e)
        {
          logger.warn("failed to parse creation time for msg "+ message2.getId());
        }

        if(messageCreateTime1 > messageCreateTime2)
        {
          return 1;
        }
        else if (messageCreateTime1 < messageCreateTime2)
        {
          return -1;
        }
        return 0;
      }
    }
    Collections.sort(messages, new MessageTimeComparator());

    for (Message message : messages)
    {
      // NO_OP messages are removed with nothing done. It is used to trigger the
      // onMessage() call if needed.
      if (message.getMsgType().equalsIgnoreCase(MessageType.NO_OP.toString()))
      {
        logger.info("Dropping NO-OP msg from " + message.getMsgSrc());
        if(message.getTgtName().equalsIgnoreCase("controller"))
        {
          accessor.removeProperty(PropertyType.MESSAGES_CONTROLLER,
            message.getId());
        }
        else
        {
          accessor.removeProperty(PropertyType.MESSAGES, instanceName,
            message.getId());
        }
        continue;
      }

      String sessionId = manager.getSessionId();
      String tgtSessionId = message.getTgtSessionId();
      if (sessionId.equals(tgtSessionId) || tgtSessionId.equals("*"))
      {
        MessageHandler handler = null;
        if (MessageState.NEW == message.getMsgState())
        {
          try
          {
            logger.info("Creating handler for message " + message.getMsgId());
            handler = createMessageHandler(message, changeContext);

            // We did not find a MessageHandlerFactory for the message; 
            // we will keep the message and we may be able to handler it when 
            // the corresponding MessageHandlerFactory factory is registered.
            if (handler == null)
            {
              logger.warn("Message handler factory not found for message type:"
                  + message.getMsgType() + ", message:" + message);
              continue;
            }

            // update msgState to read
            message.setMsgState(MessageState.READ);
            message.setReadTimeStamp(new Date().getTime());
            message.setExecuteSessionId(changeContext.getManager().getSessionId());

            _statusUpdateUtil.logInfo(message, HelixStateMachineEngine.class,
                "New Message", accessor);
            if(message.getTgtName().equalsIgnoreCase("controller"))
            {
              accessor.updateProperty(PropertyType.MESSAGES_CONTROLLER,
                                      message,
                                      message.getId());
            }
            else
            {
              accessor.updateProperty(PropertyType.MESSAGES,
                                      message,
                                      instanceName,
                                      message.getId());

            }
            scheduleTask(message, handler, changeContext);
          }
          catch (Exception e)
          {
            logger.error("Failed to create message handler for "
                + message.getMsgId(), e);
            String error = "Failed to create message handler for "
                + message.getMsgId() + " exception: " + e;

            _statusUpdateUtil.logError(message, HelixStateMachineEngine.class, e,
                error, accessor);
            // Mark the message as UNPROCESSABLE if we hit a exception while creating
            // handler for it. The message will stay on ZK and not be processed.
            message.setMsgState(MessageState.UNPROCESSABLE);
            if(message.getTgtName().equalsIgnoreCase("controller"))
            {
              accessor.updateProperty(PropertyType.MESSAGES_CONTROLLER,
                                      message,
                                      message.getId());
            }
            else
            {
              accessor.updateProperty(PropertyType.MESSAGES,
                                      message,
                                      instanceName,
                                      message.getId());
            }
            continue;
          }
        } 
        else
        {
          // This will happen because we don't delete the message as soon as we
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
        String warningMessage = "Session Id does not match. Expected sessionId: "
            + sessionId + ", sessionId from Message: " + tgtSessionId;
        logger.error(warningMessage);
        accessor.removeProperty(PropertyType.MESSAGES, instanceName,
            message.getId());
        _statusUpdateUtil.logWarning(message, HelixStateMachineEngine.class,
            warningMessage, accessor);
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

  public void shutDown()
  {
    logger.info("shutting down TaskExecutor");
    synchronized(_lock)
    {
      for(String msgType : _threadpoolMap.keySet())
      {
        List<Runnable> tasksLeft = _threadpoolMap.get(msgType).shutdownNow();
        logger.info(tasksLeft.size() + " tasks are still in the threadpool for msgType " + msgType);
      }
      for(String msgType : _threadpoolMap.keySet())
      {
        try
        {
          if(!_threadpoolMap.get(msgType).awaitTermination(200, TimeUnit.MILLISECONDS))
          {
            logger.warn(msgType + " is not fully termimated in 200 MS");
            System.out.println(msgType + " is not fully termimated in 200 MS");
          }
        }
        catch (InterruptedException e)
        {
          logger.error("Interrupted", e);
        }
      }
    }
    _monitor.shutDown();
    logger.info("shutdown finished");
  }
}
