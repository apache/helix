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
package org.apache.helix.messaging.handling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.MessageListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.monitoring.ParticipantMonitor;
import org.apache.helix.participant.HelixStateMachineEngine;
import org.apache.helix.util.StatusUpdateUtil;
import org.apache.log4j.Logger;


public class HelixTaskExecutor implements MessageListener
{
  // TODO: we need to further design how to throttle this.
  // From storage point of view, only bootstrap case is expensive
  // and we need to throttle, which is mostly IO / network bounded.
  public static final int                                DEFAULT_PARALLEL_TASKS     = 40;
  // TODO: create per-task type threadpool with customizable pool size
  protected final Map<String, Future<HelixTaskResult>>   _taskMap;
  private final Object                                   _lock;
  private final StatusUpdateUtil                         _statusUpdateUtil;
  private final ParticipantMonitor                       _monitor;
  public static final String                             MAX_THREADS                =
                                                                                        "maxThreads";

  final ConcurrentHashMap<String, MessageHandlerFactory> _handlerFactoryMap         =
                                                                                        new ConcurrentHashMap<String, MessageHandlerFactory>();

  final ConcurrentHashMap<String, ExecutorService>       _threadpoolMap             =
                                                                                        new ConcurrentHashMap<String, ExecutorService>();

  private static Logger                                  LOG                        =
                                                                                        Logger.getLogger(HelixTaskExecutor.class);

  Map<String, Integer>                                   _resourceThreadpoolSizeMap =
                                                                                        new ConcurrentHashMap<String, Integer>();

  final GroupMessageHandler                              _groupMsgHandler;

  public HelixTaskExecutor()
  {
    _taskMap = new ConcurrentHashMap<String, Future<HelixTaskResult>>();
    _groupMsgHandler = new GroupMessageHandler();

    _lock = new Object();
    _statusUpdateUtil = new StatusUpdateUtil();
    _monitor = new ParticipantMonitor();
    startMonitorThread();
  }

  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory)
  {
    registerMessageHandlerFactory(type, factory, DEFAULT_PARALLEL_TASKS);
  }

  public void registerMessageHandlerFactory(String type,
                                            MessageHandlerFactory factory,
                                            int threadpoolSize)
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
      LOG.info("Adding msg factory for type " + type + " threadpool size "
          + threadpoolSize);
    }
    else
    {
      LOG.error("Ignoring duplicate msg handler factory for type " + type);
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
    if (!_resourceThreadpoolSizeMap.containsKey(resourceName))
    {
      int threadpoolSize = -1;
      ConfigAccessor configAccessor = manager.getConfigAccessor();
      if (configAccessor != null)
      {
        ConfigScope scope =
            new ConfigScopeBuilder().forCluster(manager.getClusterName())
                                    .forResource(resourceName)
                                    .build();

        String threadpoolSizeStr = configAccessor.get(scope, MAX_THREADS);
        try
        {
          if (threadpoolSizeStr != null)
          {
            threadpoolSize = Integer.parseInt(threadpoolSizeStr);
          }
        }
        catch (Exception e)
        {
          LOG.error("", e);
        }
      }
      if (threadpoolSize > 0)
      {
        String key = MessageType.STATE_TRANSITION.toString() + "." + resourceName;
        _threadpoolMap.put(key, Executors.newFixedThreadPool(threadpoolSize));
        LOG.info("Adding per resource threadpool for resource " + resourceName
            + " with size " + threadpoolSize);
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
    if (message.getMsgType().equals(MessageType.STATE_TRANSITION.toString()))
    {
      String resourceName = message.getResourceName();
      if (resourceName != null)
      {
        String key = message.getMsgType() + "." + resourceName;
        if (_threadpoolMap.containsKey(key))
        {
          LOG.info("Find per-resource thread pool with key " + key);
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
        String taskId = message.getMsgId() + "/" + message.getPartitionName();

        if (message.getMsgType().equals(MessageType.STATE_TRANSITION.toString()))
        {
          checkResourceConfig(message.getResourceName(), notificationContext.getManager());
        }
        LOG.info("Scheduling message: " + taskId);
        // System.out.println("sched msg: " + message.getPartitionName() + "-"
        // + message.getTgtName() + "-" + message.getFromState() + "-"
        // + message.getToState());

        _statusUpdateUtil.logInfo(message,
                                  HelixTaskExecutor.class,
                                  "Message handling task scheduled",
                                  notificationContext.getManager().getHelixDataAccessor());

        HelixTask task = new HelixTask(message, notificationContext, handler, this);
        if (!_taskMap.containsKey(taskId))
        {
          LOG.info("Message:" + taskId + " handling task scheduled");
          Future<HelixTaskResult> future =
              findExecutorServiceForMsg(message).submit(task);
          _taskMap.put(taskId, future);
        }
        else
        {
          _statusUpdateUtil.logWarning(message,
                                       HelixTaskExecutor.class,
                                       "Message handling task already sheduled for "
                                           + taskId,
                                       notificationContext.getManager()
                                                          .getHelixDataAccessor());
        }
      }
      catch (Exception e)
      {
        LOG.error("Error while executing task." + message, e);

        _statusUpdateUtil.logError(message,
                                   HelixTaskExecutor.class,
                                   e,
                                   "Error while executing task " + e,
                                   notificationContext.getManager()
                                                      .getHelixDataAccessor());
      }
    }
  }

  public void cancelTask(Message message, NotificationContext notificationContext)
  {
    synchronized (_lock)
    {
      String taskId = message.getMsgId() + "/" + message.getPartitionName();

      if (_taskMap.containsKey(taskId))
      {
        _statusUpdateUtil.logInfo(message,
                                  HelixTaskExecutor.class,
                                  "Trying to cancel the future for " + taskId,
                                  notificationContext.getManager().getHelixDataAccessor());
        Future<HelixTaskResult> future = _taskMap.get(taskId);

        // If the thread is still running it will be interrupted if cancel(true)
        // is called. So state transition callbacks should implement logic to
        // return
        // if it is interrupted.
        if (future.cancel(true))
        {
          _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class, "Canceled "
              + taskId, notificationContext.getManager().getHelixDataAccessor());
          _taskMap.remove(taskId);
        }
        else
        {
          _statusUpdateUtil.logInfo(message,
                                    HelixTaskExecutor.class,
                                    "false when trying to cancel the message " + taskId,
                                    notificationContext.getManager()
                                                       .getHelixDataAccessor());
        }
      }
      else
      {
        _statusUpdateUtil.logWarning(message,
                                     HelixTaskExecutor.class,
                                     "Future not found when trying to cancel " + taskId,
                                     notificationContext.getManager()
                                                        .getHelixDataAccessor());
      }
    }
  }

  protected void reportCompletion(Message message) // String msgId)
  {
    synchronized (_lock)
    {
      String taskId = message.getMsgId() + "/" + message.getPartitionName();
      LOG.info("message finished: " + taskId + ", took "
          + (new Date().getTime() - message.getExecuteStartTimeStamp()));
      if (_taskMap.containsKey(taskId))
      {
        _taskMap.remove(taskId);
      }
      else
      {
        LOG.warn("message " + taskId + "not found in task map");
      }
    }
  }

  private void updateMessageState(List<Message> readMsgs,
                                  HelixDataAccessor accessor,
                                  String instanceName)
  {
    Builder keyBuilder = accessor.keyBuilder();
    List<PropertyKey> readMsgKeys = new ArrayList<PropertyKey>();
    for (Message msg : readMsgs)
    {
      readMsgKeys.add(msg.getKey(keyBuilder, instanceName));
    }
    accessor.setChildren(readMsgKeys, readMsgs);
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
      LOG.info("Get FINALIZE notification");
      for (MessageHandlerFactory factory : _handlerFactoryMap.values())
      {
        factory.reset();
      }
      // Cancel all scheduled future
      // synchronized (_lock)
      {
        for (Future<HelixTaskResult> f : _taskMap.values())
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
      LOG.info("No Messages to process");
      return;
    }

    // sort message by creation timestamp, so message created earlier is processed first
    Collections.sort(messages, Message.CREATE_TIME_COMPARATOR);

    // message handlers created
    List<MessageHandler> handlers = new ArrayList<MessageHandler>();

    // message read
    List<Message> readMsgs = new ArrayList<Message>();

    String sessionId = manager.getSessionId();
    List<String> curResourceNames =
        accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId));
    List<PropertyKey> createCurStateKeys = new ArrayList<PropertyKey>();
    List<CurrentState> metaCurStates = new ArrayList<CurrentState>();
    Set<String> createCurStateNames = new HashSet<String>();

    changeContext.add(NotificationContext.TASK_EXECUTOR_KEY, this);
    for (Message message : messages)
    {
      // nop messages are simply removed. It is used to trigger onMessage() in
      // situations such as register a new message handler factory
      if (message.getMsgType().equalsIgnoreCase(MessageType.NO_OP.toString()))
      {
        LOG.info("Dropping NO-OP message. mid: " + message.getId() + ", from: "
            + message.getMsgSrc());
        accessor.removeProperty(message.getKey(keyBuilder, instanceName));
        continue;
      }

      String tgtSessionId = message.getTgtSessionId();

      // if sessionId not match, remove it
      if (!sessionId.equals(tgtSessionId) && !tgtSessionId.equals("*"))
      {
        String warningMessage =
            "SessionId does NOT match. expected sessionId: " + sessionId
                + ", tgtSessionId in message: " + tgtSessionId + ", messageId: "
                + message.getMsgId();
        LOG.warn(warningMessage);
        accessor.removeProperty(message.getKey(keyBuilder, instanceName));
        _statusUpdateUtil.logWarning(message,
                                     HelixStateMachineEngine.class,
                                     warningMessage,
                                     accessor);
        continue;
      }

      // don't process message that is of READ or UNPROCESSABLE state
      if (MessageState.NEW != message.getMsgState())
      {
        // It happens because we don't delete message right after
        // read. Instead we keep it until the current state is updated.
        // We will read the message again if there is a new message but we
        // check for the status and ignore if its already read
        LOG.trace("Message already read. mid: " + message.getMsgId());
        continue;
      }

      // create message handlers, if handlers not found, leave its state as NEW
      try
      {
        List<MessageHandler> createHandlers =
            createMessageHandlers(message, changeContext);
        if (createHandlers.isEmpty())
        {
          continue;
        }
        handlers.addAll(createHandlers);
      }
      catch (Exception e)
      {
        LOG.error("Failed to create message handler for " + message.getMsgId(), e);
        String error =
            "Failed to create message handler for " + message.getMsgId()
                + ", exception: " + e;

        _statusUpdateUtil.logError(message,
                                   HelixStateMachineEngine.class,
                                   e,
                                   error,
                                   accessor);

        // Mark message state UNPROCESSABLE if we hit an exception in creating
        // message handler. The message will stay on zookeeper but will not be processed
        message.setMsgState(MessageState.UNPROCESSABLE);
        accessor.updateProperty(message.getKey(keyBuilder, instanceName), message);
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

      readMsgs.add(message);

      // batch creation of all current state meta data
      // do it for non-controller and state transition messages only
      if (!message.isControlerMsg()
          && message.getMsgType().equals(Message.MessageType.STATE_TRANSITION.toString()))
      {
        String resourceName = message.getResourceName();
        if (!curResourceNames.contains(resourceName)
            && !createCurStateNames.contains(resourceName))
        {
          createCurStateNames.add(resourceName);
          createCurStateKeys.add(keyBuilder.currentState(instanceName,
                                                         sessionId,
                                                         resourceName));

          CurrentState metaCurState = new CurrentState(resourceName);
          metaCurState.setBucketSize(message.getBucketSize());
          metaCurState.setStateModelDefRef(message.getStateModelDef());
          metaCurState.setSessionId(sessionId);
          metaCurState.setGroupMessageMode(message.getGroupMessageMode());
          String ftyName = message.getStateModelFactoryName();
          if (ftyName != null)
          {
            metaCurState.setStateModelFactoryName(ftyName);
          }
          else
          {
            metaCurState.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
          }

          metaCurStates.add(metaCurState);
        }
      }
    }

    // batch create curState meta
    if (createCurStateKeys.size() > 0)
    {
      try
      {
        accessor.createChildren(createCurStateKeys, metaCurStates);
      }
      catch (Exception e)
      {
        LOG.error(e);
      }
    }

    // update message state to READ in batch and schedule all read messages
    if (readMsgs.size() > 0)
    {
      updateMessageState(readMsgs, accessor, instanceName);

      for (MessageHandler handler : handlers)
      {
        scheduleTask(handler._message, handler, changeContext);
      }
    }
  }

  private MessageHandler createMessageHandler(Message message,
                                              NotificationContext changeContext)
  {
    String msgType = message.getMsgType().toString();

    MessageHandlerFactory handlerFactory = _handlerFactoryMap.get(msgType);

    // Fail to find a MessageHandlerFactory for the message
    // we will keep the message and the message will be handled when
    // the corresponding MessageHandlerFactory is registered
    if (handlerFactory == null)
    {
      LOG.warn("Fail to find message handler factory for type: " + msgType + " mid:"
          + message.getMsgId());
      return null;
    }

    return handlerFactory.createHandler(message, changeContext);
  }

  private List<MessageHandler> createMessageHandlers(Message message,
                                                     NotificationContext changeContext)
  {
    List<MessageHandler> handlers = new ArrayList<MessageHandler>();
    if (!message.getGroupMessageMode())
    {
      LOG.info("Creating handler for message " + message.getMsgId() + "/"
          + message.getPartitionName());

      MessageHandler handler = createMessageHandler(message, changeContext);

      if (handler != null)
      {
        handlers.add(handler);
      }
    }
    else
    {
      _groupMsgHandler.put(message);

      List<String> partitionNames = message.getPartitionNames();
      for (String partitionName : partitionNames)
      {
        Message subMsg = new Message(message.getRecord());
        subMsg.setPartitionName(partitionName);
        subMsg.setAttribute(Attributes.PARENT_MSG_ID, message.getId());

        LOG.info("Creating handler for group message " + subMsg.getMsgId() + "/"
            + partitionName);
        MessageHandler handler = createMessageHandler(subMsg, changeContext);
        if (handler != null)
        {
          handlers.add(handler);
        }
      }
    }

    return handlers;
  }

  public void shutDown()
  {
    LOG.info("shutting down TaskExecutor");
    synchronized (_lock)
    {
      for (String msgType : _threadpoolMap.keySet())
      {
        List<Runnable> tasksLeft = _threadpoolMap.get(msgType).shutdownNow();
        LOG.info(tasksLeft.size() + " tasks are still in the threadpool for msgType "
            + msgType);
      }
      for (String msgType : _threadpoolMap.keySet())
      {
        try
        {
          if (!_threadpoolMap.get(msgType).awaitTermination(200, TimeUnit.MILLISECONDS))
          {
            LOG.warn(msgType + " is not fully termimated in 200 MS");
            System.out.println(msgType + " is not fully termimated in 200 MS");
          }
        }
        catch (InterruptedException e)
        {
          LOG.error("Interrupted", e);
        }
      }
    }
    _monitor.shutDown();
    LOG.info("shutdown finished");
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
