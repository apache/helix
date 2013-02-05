package org.apache.helix.messaging.handling;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
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
import org.apache.helix.NotificationContext.MapKey;
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


public class HelixTaskExecutor implements MessageListener, TaskExecutor
{
  // TODO: we need to further design how to throttle this.
  // From storage point of view, only bootstrap case is expensive
  // and we need to throttle, which is mostly IO / network bounded.
  public static final int                                DEFAULT_PARALLEL_TASKS     = 40;
  // TODO: create per-task type threadpool with customizable pool size
  protected final Map<String, MessageTaskInfo>   _taskMap;
  private final Object                                   _lock;
  private final StatusUpdateUtil                         _statusUpdateUtil;
  private final ParticipantMonitor                       _monitor;
  public static final String                             MAX_THREADS                =
                                                                                        "maxThreads";

  final ConcurrentHashMap<String, MessageHandlerFactory> _handlerFactoryMap         =
                                                                                        new ConcurrentHashMap<String, MessageHandlerFactory>();

  final ConcurrentHashMap<String, ExecutorService>       _executorMap;

  private static Logger                                  LOG                        =
                                                                                        Logger.getLogger(HelixTaskExecutor.class);

  Map<String, Integer>                                   _resourceThreadpoolSizeMap =
                                                                                        new ConcurrentHashMap<String, Integer>();

  // timer for schedule timeout tasks
  final Timer _timer;


  public HelixTaskExecutor()
  {
    _taskMap = new ConcurrentHashMap<String, MessageTaskInfo>();
    _executorMap = new ConcurrentHashMap<String, ExecutorService>();

    _lock = new Object();
    _statusUpdateUtil = new StatusUpdateUtil();
    _monitor = new ParticipantMonitor();
    
    _timer = new Timer(true);	// created as a daemon timer thread to handle task timeout

    startMonitorThread();
  }

  @Override
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory)
  {
    registerMessageHandlerFactory(type, factory, DEFAULT_PARALLEL_TASKS);
  }

  @Override
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
      ExecutorService executorSvc = Executors.newFixedThreadPool(threadpoolSize);
      _executorMap.put(type, executorSvc);
      
      LOG.info("Added msg-factory for type: " + type + ", threadpool size "
          + threadpoolSize);
    }
    else
    {
      LOG.warn("Fail to register msg-handler-factory for type: " + type 
    		  + ", pool-size: " + threadpoolSize + ", factory: " + factory);
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
        _executorMap.put(key, Executors.newFixedThreadPool(threadpoolSize));
        LOG.info("Added per resource threadpool for resource: " + resourceName
            + " with size: " + threadpoolSize);
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
    ExecutorService executorService = _executorMap.get(message.getMsgType());
    if (message.getMsgType().equals(MessageType.STATE_TRANSITION.toString()))
    {
      String resourceName = message.getResourceName();
      if (resourceName != null)
      {
        String key = message.getMsgType() + "." + resourceName;
        if (_executorMap.containsKey(key))
        {
          LOG.info("Find per-resource thread pool with key: " + key);
          executorService = _executorMap.get(key);
        }
      }
    }
    return executorService;
  }

  // ExecutorService impl's in JDK are thread-safe
  @Override
  public List<Future<HelixTaskResult>> invokeAllTasks(List<MessageTask> tasks, long timeout, TimeUnit unit) throws InterruptedException
  {
	  if (tasks == null || tasks.size() == 0) {
		  return null;
	  }
	  
	  // check all tasks use the same executor-service
	  ExecutorService exeSvc = findExecutorServiceForMsg(tasks.get(0).getMessage());
	  for (int i = 1; i < tasks.size(); i++) {
		  MessageTask task = tasks.get(i);
		  ExecutorService curExeSvc = findExecutorServiceForMsg(task.getMessage());
		  if (curExeSvc != exeSvc) {
			  LOG.error("Fail to invoke all tasks because they are not using the same executor-service");
			  return null;
		  }
	  }
	  
	  // TODO: check if any of the task has already been scheduled
	  	  
	  // this is a blocking call
      List<Future<HelixTaskResult>> futures = exeSvc.invokeAll(tasks, timeout, unit);
      
      return futures;
  }

  @Override
  public boolean cancelTimeoutTask(MessageTask task)
  {
	  synchronized(_lock) {
		  String taskId = task.getTaskId();
          if (_taskMap.containsKey(taskId)) {
        	  MessageTaskInfo info = _taskMap.get(taskId);
        	  if (info._timerTask != null) {
        		  info._timerTask.cancel();
        	  }
        	  return true;
          }
          return false;
	  }
  }
  
  @Override 
  public boolean scheduleTask(MessageTask task)
  {
    String taskId = task.getTaskId();
    Message message = task.getMessage();
    NotificationContext notificationContext = task.getNotificationContext();

    
    try 
    {
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
        
        // this sync guarantees that ExecutorService.submit() task and put taskInfo into map are sync'ed
        synchronized (_lock)
        {
          if (!_taskMap.containsKey(taskId))
          {
          	ExecutorService exeSvc = findExecutorServiceForMsg(message);
        	Future<HelixTaskResult> future = exeSvc.submit(task);
        	
            TimerTask timerTask = null;
            if (message.getExecutionTimeout() > 0)
            {
              timerTask = new MessageTimeoutTask(this, task);
              _timer.schedule(timerTask, message.getExecutionTimeout());
              LOG.info("Message starts with timeout " + message.getExecutionTimeout()
                  + " MsgId: " + task.getTaskId());
            }
            else
            {
              LOG.debug("Message does not have timeout. MsgId: " + task.getTaskId());
            }

        	_taskMap.put(taskId, new MessageTaskInfo(task, future, timerTask));

            LOG.info("Message: " + taskId + " handling task scheduled");

            return true;
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
      }
      catch (Exception e)
      {
        LOG.error("Error while executing task. " + message, e);

        _statusUpdateUtil.logError(message,
                                   HelixTaskExecutor.class,
                                   e,
                                   "Error while executing task " + e,
                                   notificationContext.getManager()
                                                      .getHelixDataAccessor());
      }
      return false;
  }

  @Override
  public boolean cancelTask(MessageTask task)
  {
      Message message = task.getMessage();
      NotificationContext notificationContext = task.getNotificationContext();
      String taskId = task.getTaskId();

      synchronized(_lock) 
      {
        if (_taskMap.containsKey(taskId))
        {
      	  MessageTaskInfo taskInfo = _taskMap.get(taskId);
    	  // cancel timeout task
          if (taskInfo._timerTask != null) {
        	  taskInfo._timerTask.cancel();
          }

    	  // cancel task
          Future<HelixTaskResult> future = taskInfo.getFuture();

          _statusUpdateUtil.logInfo(message,
                                  HelixTaskExecutor.class,
                                  "Canceling task: " + taskId,
                                  notificationContext.getManager().getHelixDataAccessor());

          // If the thread is still running it will be interrupted if cancel(true)
          // is called. So state transition callbacks should implement logic to
          // return
          // if it is interrupted.
          if (future.cancel(true))
          {
            _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class, "Canceled task: "
              + taskId, notificationContext.getManager().getHelixDataAccessor());
            _taskMap.remove(taskId);
            return true;
          }
          else
          {
            _statusUpdateUtil.logInfo(message,
                                    HelixTaskExecutor.class,
                                    "fail to cancel task: " + taskId,
                                    notificationContext.getManager()
                                                       .getHelixDataAccessor());
          }
        }
        else
        {
          _statusUpdateUtil.logWarning(message,
                                     HelixTaskExecutor.class,
                                     "fail to cancel task: " + taskId + ", future not found",
                                     notificationContext.getManager()
                                                        .getHelixDataAccessor());
        }
      }
      return false;
  }

  @Override
  public void finishTask(MessageTask task)
  {
	Message message = task.getMessage();
    String taskId = task.getTaskId();
    LOG.info("message finished: " + taskId + ", took "
            + (new Date().getTime() - message.getExecuteStartTimeStamp()));

    synchronized (_lock)
    {
      if (_taskMap.containsKey(taskId))
      {
          MessageTaskInfo info = _taskMap.remove(taskId);
          if (info._timerTask != null) {
        	  // ok to cancel multiple times
        	  info._timerTask.cancel();
          }
      }
      else
      {
        LOG.warn("message " + taskId + " not found in task map");
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
      // Cancel all scheduled tasks
      synchronized (_lock)
      {
          for (MessageTaskInfo info : _taskMap.values())
          {
            cancelTask(info._task);
          }
        _taskMap.clear();
      }
      return;
    }

    if (messages == null || messages.size() == 0)
    {
      LOG.info("No Messages to process");
      return;
    }

    // sort message by creation timestamp, so message created earlier is processed first
    Collections.sort(messages, Message.CREATE_TIME_COMPARATOR);

    HelixManager manager = changeContext.getManager();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

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

      // sessionId mismatch normally means message comes from expired session, just remove it
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
    	if (LOG.isTraceEnabled()) {
    		LOG.trace("Message already read. msgId: " + message.getMsgId());
    	}
        continue;
      }

      // create message handlers, if handlers not found, leave its state as NEW
      try
      {
        MessageHandler createHandler = createMessageHandler(message, changeContext);
        if (createHandler == null)
        {
          continue;
        }
        handlers.add(createHandler);
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

        message.setMsgState(MessageState.UNPROCESSABLE);
        accessor.removeProperty(message.getKey(keyBuilder, instanceName));
        LOG.error("Message cannot be proessed: " + message.getRecord(), e);

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
          metaCurState.setBatchMessageMode(message.getBatchMessageMode());
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
        LOG.error("fail to create cur-state znodes for messages: " + readMsgs, e);
      }
    }

    // update message state to READ in batch and schedule all read messages
    if (readMsgs.size() > 0)
    {
      updateMessageState(readMsgs, accessor, instanceName);

      for (MessageHandler handler : handlers)
      {
          HelixTask task = new HelixTask(handler._message, changeContext, handler, this);
          scheduleTask(task);
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
      LOG.warn("Fail to find message handler factory for type: " + msgType + " msgId: "
          + message.getMsgId());
      return null;
    }

    // pass the executor to msg-handler since batch-msg-handler needs task-executor to schedule sub-msgs
    changeContext.add(MapKey.TASK_EXECUTOR.toString(), this);
    return handlerFactory.createHandler(message, changeContext);
  }

  @Override
  public void shutdown()
  {
    LOG.info("shutting down TaskExecutor");
    _timer.cancel();
    
    synchronized (_lock)
    {
      for (String msgType : _executorMap.keySet())
      {
        List<Runnable> tasksLeft = _executorMap.get(msgType).shutdownNow();
        LOG.info(tasksLeft.size() + " tasks are still in the threadpool for msgType "
            + msgType);
      }
      for (String msgType : _executorMap.keySet())
      {
        try
        {
          if (!_executorMap.get(msgType).awaitTermination(200, TimeUnit.MILLISECONDS))
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
}
