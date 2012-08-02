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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigAccessor;
import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixConstants;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Message;
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
  public static final int                               DEFAULT_PARALLEL_TASKS = 4;
  // TODO: create per-task type threadpool with customizable pool size
  protected final Map<String, Future<HelixTaskResult>>   _taskMap;
  private final Object                                   _lock;
  private final StatusUpdateUtil                         _statusUpdateUtil;
  private final ParticipantMonitor                       _monitor;
  public static final String MAX_THREADS = "maxThreads";

  final ConcurrentHashMap<String, MessageHandlerFactory> _handlerFactoryMap =
                                                                                new ConcurrentHashMap<String, MessageHandlerFactory>();

  final ConcurrentHashMap<String, ExecutorService>       _threadpoolMap     =
                                                                                new ConcurrentHashMap<String, ExecutorService>();

  private static Logger                                  logger             =
                                                                                Logger.getLogger(HelixTaskExecutor.class);
  
  Map<String, Integer> _resourceThreadpoolSizeMap = new ConcurrentHashMap<String, Integer>();
  
  public HelixTaskExecutor()
  {
    _taskMap = new ConcurrentHashMap<String, Future<HelixTaskResult>>();
    _lock = new Object();
    _statusUpdateUtil = new StatusUpdateUtil();
    _monitor = new ParticipantMonitor();
    startMonitorThread();
  }
  
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory)
  {
    registerMessageHandlerFactory(type, factory, DEFAULT_PARALLEL_TASKS);
  }
  
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory, int threadpoolSize)
  {
    if (!_handlerFactoryMap.containsKey(type))
    {
      if (!type.equalsIgnoreCase(factory.getMessageType()))
      {
        throw new HelixException("Message factory type mismatch. Type: " + type
            + " factory : " + factory.getMessageType());

      }
      _handlerFactoryMap.put(type, factory);
      _threadpoolMap.put(type, Executors.newFixedThreadPool(threadpoolSize));
      logger.info("Adding msg factory for type " + type +" threadpool size " + threadpoolSize);
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
  
  void checkResourceConfig(String resourceName, HelixManager manager)
  {
    if(!_resourceThreadpoolSizeMap.containsKey(resourceName))
    {
      int threadpoolSize = -1;
      ConfigAccessor configAccessor = manager.getConfigAccessor();
      if(configAccessor != null)
      {
        ConfigScope scope =
          new ConfigScopeBuilder().forCluster(manager.getClusterName()).forResource(resourceName).build();
      
        String threadpoolSizeStr = configAccessor.get(scope, MAX_THREADS);
        try
        {
          if(threadpoolSizeStr != null)
          {
            threadpoolSize = Integer.parseInt(threadpoolSizeStr);
          } 
        }
        catch(Exception e)
        {
          logger.error("", e);
        }
      }
      if(threadpoolSize > 0)
      {
        String key = MessageType.STATE_TRANSITION.toString() + "." + resourceName;
        _threadpoolMap.put(key, Executors.newFixedThreadPool(threadpoolSize));
      }
      _resourceThreadpoolSizeMap.put(resourceName, threadpoolSize);
    }
  }
  
  /**
   * Find the executor service for the message. A message can have a per-statemodelfactory
   * executor service, or per-message type executor service.
   * 
   **/
  ExecutorService findExecutorServiceForMsg(Message message)
  {
    ExecutorService executorService = _threadpoolMap.get(message.getMsgType());
    if(message.getMsgType().equals(MessageType.STATE_TRANSITION.toString()))
    {
      String resourceName = message.getResourceName();
      if (resourceName != null)
      {
        String key = message.getMsgType() + "." + resourceName;
        if(_threadpoolMap.containsKey(key))
        {
          logger.info("Find per-resource thread pool with key " + key);
          executorService = _threadpoolMap.get(key);
        }
      }
    }
    return executorService;
  }
  
  public void scheduleTask(Message message,
                           MessageHandler handler,
                           NotificationContext notificationContext)
  {
    assert (handler != null);
    synchronized (_lock)
    {
      try
      {
        if(message.getMsgType().equals(MessageType.STATE_TRANSITION.toString()))
        {
          checkResourceConfig(message.getResourceName(), notificationContext.getManager());
        }
        logger.info("message.getMsgId() = " + message.getMsgId());
        _statusUpdateUtil.logInfo(message,
                                  HelixTaskExecutor.class,
                                  "Message handling task scheduled",
                                  notificationContext.getManager().getHelixDataAccessor());

        HelixTask task = new HelixTask(message, notificationContext, handler, this);
        if (!_taskMap.containsKey(message.getMsgId()))
        {
          logger.info("msg:" + message.getMsgId() + " handling task scheduled");
          Future<HelixTaskResult> future =
              findExecutorServiceForMsg(message).submit(task);
          _taskMap.put(message.getMsgId(), future);
        }
        else
        {
          _statusUpdateUtil.logWarning(message,
                                       HelixTaskExecutor.class,
                                       "Message handling task already sheduled for "
                                           + message.getMsgId(),
                                       notificationContext.getManager()
                                                          .getHelixDataAccessor());
        }
      }
      catch (Exception e)
      {
        String errorMessage = "Error while executing task" + e;
        logger.error("Error while executing task." + message, e);

        _statusUpdateUtil.logError(message,
                                   HelixTaskExecutor.class,
                                   e,
                                   errorMessage,
                                   notificationContext.getManager()
                                                      .getHelixDataAccessor());
      }
    }
  }

  public void cancelTask(Message message, NotificationContext notificationContext)
  {
   synchronized (_lock)
    {
      if (_taskMap.containsKey(message.getMsgId()))
      {
        _statusUpdateUtil.logInfo(message,
                                  HelixTaskExecutor.class,
                                  "Trying to cancel the future for " + message.getMsgId(),
                                  notificationContext.getManager().getHelixDataAccessor());
        Future<HelixTaskResult> future = _taskMap.get(message.getMsgId());

        // If the thread is still running it will be interrupted if cancel(true)
        // is called. So state transition callbacks should implement logic to
        // return
        // if it is interrupted.
        if (future.cancel(true))
        {
          _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class, "Canceled "
              + message.getMsgId(), notificationContext.getManager()
                                                       .getHelixDataAccessor());
          _taskMap.remove(message.getMsgId());
        }
        else
        {
          _statusUpdateUtil.logInfo(message,
                                    HelixTaskExecutor.class,
                                    "false when trying to cancel the message "
                                        + message.getMsgId(),
                                    notificationContext.getManager()
                                                       .getHelixDataAccessor());
        }
      }
      else
      {
        _statusUpdateUtil.logWarning(message,
                                     HelixTaskExecutor.class,
                                     "Future not found when trying to cancel "
                                         + message.getMsgId(),
                                     notificationContext.getManager()
                                                        .getHelixDataAccessor());
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
      }
      else
      {
        logger.warn("message " + msgId + "not found in task map");
      }
    }
  }

  @Override
  public void onMessage(String instanceName,
                        List<Message> messages,
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
      // Cancel all scheduled future
     // synchronized (_lock)
      {
        for(Future<HelixTaskResult> f : _taskMap.values())
        {
          f.cancel(true);
        }
        _taskMap.clear();
      }
      return;
    }

    HelixManager manager = changeContext.getManager();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    if (messages == null || messages.size() == 0)
    {
      logger.info("No Messages to process");
      return;
    }

    // Sort message based on creation timestamp
    Collections.sort(messages, new Comparator<Message>()
    {
      @Override
      public int compare(Message m1, Message m2)
      {
        return (int) (m1.getCreateTimeStamp() - m2.getCreateTimeStamp());
      }
    });

    List<Message> readMsgs = new ArrayList<Message>();
    List<PropertyKey> readMsgKeys = new ArrayList<PropertyKey>();
    List<MessageHandler> readMsgHandlers = new ArrayList<MessageHandler>();

    String sessionId = manager.getSessionId();
    List<String> curResourceNames = accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId));
    List<PropertyKey> createCurStateKeys = new ArrayList<PropertyKey>();
    List<CurrentState> metaCurStates = new ArrayList<CurrentState>(); 
    Set<String> createCurStateNames = new HashSet<String>();

    for (Message message : messages)
    {
      // NO_OP messages are removed with nothing done. It is used to trigger the
      // onMessage() call if needed.
      if (message.getMsgType().equalsIgnoreCase(MessageType.NO_OP.toString()))
      {
        logger.info("Dropping NO-OP msg from " + message.getMsgSrc());
        if (message.getTgtName().equalsIgnoreCase("controller"))
        {
          accessor.removeProperty(keyBuilder.controllerMessage(message.getId()));
        }
        else
        {
          accessor.removeProperty(keyBuilder.message(instanceName, message.getId()));
        }
        continue;
      }

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

            _statusUpdateUtil.logInfo(message,
                                      HelixStateMachineEngine.class,
                                      "New Message",
                                      accessor);

            // batch all messages
            readMsgs.add(message);
            if (message.getTgtName().equalsIgnoreCase("controller"))
            {
              readMsgKeys.add(keyBuilder.controllerMessage(message.getMsgId()));
            }
            else
            {
              // batch all creation of current state meta data
              // do it for state transition messages only
              if (message.getMsgType().equals(Message.MessageType.STATE_TRANSITION.toString()))
              {
                String resourceName = message.getResourceName();
                if (!curResourceNames.contains(resourceName) && !createCurStateNames.contains(resourceName))
                {
                  createCurStateNames.add(resourceName);
                  createCurStateKeys.add(keyBuilder.currentState(instanceName, sessionId, resourceName));
                  
                  CurrentState metaCurState = new CurrentState(resourceName);
                  metaCurState.setBucketSize(message.getBucketSize());
                  metaCurState.setStateModelDefRef(message.getStateModelDef());
                  metaCurState.setSessionId(sessionId);
                  String ftyName = message.getStateModelFactoryName(); 
                  if (ftyName != null)
                  {
                    metaCurState.setStateModelFactoryName(ftyName);
                  } else
                  {
                    metaCurState.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
                  }
                  
                  metaCurStates.add(metaCurState);
                }
              }
              
              readMsgKeys.add(keyBuilder.message(instanceName, message.getMsgId()));
            }
            readMsgHandlers.add(handler);
          }
          catch (Exception e)
          {
            logger.error("Failed to create message handler for " + message.getMsgId(), e);
            String error =
                "Failed to create message handler for " + message.getMsgId()
                    + " exception: " + e;

            _statusUpdateUtil.logError(message,
                                       HelixStateMachineEngine.class,
                                       e,
                                       error,
                                       accessor);
            // Mark the message as UNPROCESSABLE if we hit a exception while creating
            // handler for it. The message will stay on ZK and not be processed.
            message.setMsgState(MessageState.UNPROCESSABLE);
            if (message.getTgtName().equalsIgnoreCase("controller"))
            {
              accessor.updateProperty(keyBuilder.controllerMessage(message.getId()),
                                      message);
            }
            else
            {
              accessor.updateProperty(keyBuilder.message(instanceName, message.getId()),
                                      message);
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
        String warningMessage =
            "Session Id does not match. Expected sessionId: " + sessionId
                + ", sessionId from Message: " + tgtSessionId + ". MessageId: "
                + message.getMsgId();
        logger.error(warningMessage);
        accessor.removeProperty(keyBuilder.message(instanceName, message.getId()));
        _statusUpdateUtil.logWarning(message,
                                     HelixStateMachineEngine.class,
                                     warningMessage,
                                     accessor);
      }
    }

    // batch create curState meta
    if (createCurStateKeys.size() > 0)
    {
      try
      {
        accessor.createChildren(createCurStateKeys, metaCurStates);
      } catch (Exception e)
      {
        System.out.println(e);
      }
    }
    
    // update messages in batch and schedule all read messages
    if (readMsgs.size() > 0)
    {
      // System.out.println("Scheduling task in batch of "+ readMsgs.size());
      accessor.setChildren(readMsgKeys, readMsgs);

      for (int i = 0; i < readMsgs.size(); i++)
      {
        Message message = readMsgs.get(i);
        MessageHandler handler = readMsgHandlers.get(i);
        scheduleTask(message, handler, changeContext);
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
      logger.warn("Cannot find handler factory for msg type " + msgType + " message:"
          + message.getMsgId());
      return null;
    }

    return handlerFactory.createHandler(message, changeContext);
  }

  public void shutDown()
  {
    logger.info("shutting down TaskExecutor");
   synchronized (_lock)
    {
      for (String msgType : _threadpoolMap.keySet())
      {
        List<Runnable> tasksLeft = _threadpoolMap.get(msgType).shutdownNow();
        logger.info(tasksLeft.size() + " tasks are still in the threadpool for msgType "
            + msgType);
      }
      for (String msgType : _threadpoolMap.keySet())
      {
        try
        {
          if (!_threadpoolMap.get(msgType).awaitTermination(200, TimeUnit.MILLISECONDS))
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

  // TODO: remove this
  public static void main(String[] args) throws Exception
  {
    ExecutorService pool = Executors.newFixedThreadPool(DEFAULT_PARALLEL_TASKS);
    Future<HelixTaskResult> future;
    future = pool.submit(new Callable<HelixTaskResult>()
    {

      @Override
      public HelixTaskResult call() throws Exception
      {
        System.out.println("CMTaskExecutor.main(...).new Callable() {...}.call()");
        return null;
      }

    });
    future = pool.submit(new HelixTask(null, null, null, null));
    Thread.currentThread().join();
    System.out.println(future.isDone());
  }
}
