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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.Criteria;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.MapKey;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.monitoring.mbeans.MessageQueueMonitor;
import org.apache.helix.monitoring.mbeans.ParticipantMessageMonitor;
import org.apache.helix.monitoring.mbeans.ParticipantMessageMonitor.ProcessedMessageState;
import org.apache.helix.monitoring.mbeans.ParticipantStatusMonitor;
import org.apache.helix.participant.HelixStateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.StatusUpdateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixTaskExecutor implements MessageListener, TaskExecutor {
  /**
   * Put together all registration information about a message handler factory
   */
  class MsgHandlerFactoryRegistryItem {
    private final MessageHandlerFactory _factory;
    private final int _threadPoolSize;

    public MsgHandlerFactoryRegistryItem(MessageHandlerFactory factory, int threadPoolSize) {
      if (factory == null) {
        throw new NullPointerException("Message handler factory is null");
      }

      if (threadPoolSize <= 0) {
        throw new IllegalArgumentException("Illegal thread pool size: " + threadPoolSize);
      }

      _factory = factory;
      _threadPoolSize = threadPoolSize;
    }

    int threadPoolSize() {
      return _threadPoolSize;
    }

    MessageHandlerFactory factory() {
      return _factory;
    }
  }

  private static Logger LOG = LoggerFactory.getLogger(HelixTaskExecutor.class);

  // TODO: we need to further design how to throttle this.
  // From storage point of view, only bootstrap case is expensive
  // and we need to throttle, which is mostly IO / network bounded.
  public static final int DEFAULT_PARALLEL_TASKS = 40;
  // TODO: create per-task type threadpool with customizable pool size
  protected final Map<String, MessageTaskInfo> _taskMap;
  private final Object _lock;
  private final StatusUpdateUtil _statusUpdateUtil;
  private final ParticipantStatusMonitor _monitor;
  public static final String MAX_THREADS = "maxThreads";

  private MessageQueueMonitor _messageQueueMonitor;
  private GenericHelixController _controller;
  private Long _lastSessionSyncTime;
  private static final int SESSION_SYNC_INTERVAL = 2000; // 2 seconds
  private static final String SESSION_SYNC = "SESSION-SYNC";
  /**
   * Map of MsgType->MsgHandlerFactoryRegistryItem
   */
  final ConcurrentHashMap<String, MsgHandlerFactoryRegistryItem> _hdlrFtyRegistry;

  final ConcurrentHashMap<String, ExecutorService> _executorMap;

  final ExecutorService _batchMessageExecutorService;

  final ConcurrentHashMap<String, String> _messageTaskMap;

  final Set<String> _knownMessageIds;

  /* Resources whose configuration for dedicate thread pool has been checked.*/
  final Set<String> _resourcesThreadpoolChecked;
  final Set<String> _transitionTypeThreadpoolChecked;

  // timer for schedule timeout tasks
  final Timer _timer;

  private boolean _isShuttingDown;

  public HelixTaskExecutor() {
    this(new ParticipantStatusMonitor(false, null), null);
  }

  public HelixTaskExecutor(ParticipantStatusMonitor participantStatusMonitor) {
    this(participantStatusMonitor, null);
  }

  public HelixTaskExecutor(ParticipantStatusMonitor participantStatusMonitor,
      MessageQueueMonitor messageQueueMonitor) {
    _monitor = participantStatusMonitor;
    _messageQueueMonitor = messageQueueMonitor;

    _taskMap = new ConcurrentHashMap<>();

    _hdlrFtyRegistry = new ConcurrentHashMap<>();
    _executorMap = new ConcurrentHashMap<>();
    _messageTaskMap = new ConcurrentHashMap<>();
    _knownMessageIds = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    _batchMessageExecutorService = Executors.newCachedThreadPool();
    _monitor.createExecutorMonitor("BatchMessageExecutor", _batchMessageExecutorService);

    _resourcesThreadpoolChecked =
        Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    _transitionTypeThreadpoolChecked =
        Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    _lock = new Object();
    _statusUpdateUtil = new StatusUpdateUtil();

    _timer = new Timer(true); // created as a daemon timer thread to handle task timeout

    _isShuttingDown = false;

    startMonitorThread();
  }

  @Override
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory) {
    registerMessageHandlerFactory(type, factory, DEFAULT_PARALLEL_TASKS);
  }

  @Override
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory,
      int threadpoolSize) {
    if (factory instanceof  MultiTypeMessageHandlerFactory) {
      if (!((MultiTypeMessageHandlerFactory) factory).getMessageTypes().contains(type)) {
        throw new HelixException("Message factory type mismatch. Type: " + type + ", factory: "
            + ((MultiTypeMessageHandlerFactory) factory).getMessageTypes());
      }
    } else {
      if (!factory.getMessageType().equals(type)) {
        throw new HelixException(
            "Message factory type mismatch. Type: " + type + ", factory: " + factory.getMessageType());
      }
    }

    _isShuttingDown = false;

    MsgHandlerFactoryRegistryItem newItem = new MsgHandlerFactoryRegistryItem(factory, threadpoolSize);
    MsgHandlerFactoryRegistryItem prevItem = _hdlrFtyRegistry.putIfAbsent(type, newItem);
    if (prevItem == null) {
      ExecutorService newPool = Executors.newFixedThreadPool(threadpoolSize, new ThreadFactory() {
        @Override public Thread newThread(Runnable r) {
          return new Thread(r, "HelixTaskExecutor-message_handle_thread");
        }
      });
      ExecutorService prevExecutor = _executorMap.putIfAbsent(type, newPool);
      if (prevExecutor != null) {
        LOG.warn("Skip creating a new thread pool for type: " + type + ", already existing pool: "
            + prevExecutor + ", isShutdown: " + prevExecutor.isShutdown());
        newPool.shutdown();
        newPool = null;
      } else {
        _monitor.createExecutorMonitor(type, newPool);
      }
      LOG.info("Registered message handler factory for type: " + type + ", poolSize: "
          + threadpoolSize + ", factory: " + factory + ", pool: " + _executorMap.get(type));
    } else {
      LOG.info("Skip register message handler factory for type: " + type + ", poolSize: "
          + threadpoolSize + ", factory: " + factory + ", already existing factory: "
          + prevItem.factory());
      newItem = null;
    }
  }

  public void setController(GenericHelixController controller) {
    _controller = controller;
  }

  public ParticipantStatusMonitor getParticipantMonitor() {
    return _monitor;
  }

  private void startMonitorThread() {
    // start a thread which monitors the completions of task
  }

  /** Dedicated Thread pool can be provided in configuration or by client.
   *  This method is to check it and update the thread pool if necessary.
   */
  private void updateStateTransitionMessageThreadPool(Message message, HelixManager manager) {
    if (!message.getMsgType().equals(MessageType.STATE_TRANSITION.name())) {
      return;
    }

    String resourceName = message.getResourceName();
    String factoryName = message.getStateModelFactoryName();
    String stateModelName = message.getStateModelDef();

    if (factoryName == null) {
      factoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY;
    }
    StateModelFactory<? extends StateModel> stateModelFactory =
        manager.getStateMachineEngine().getStateModelFactory(stateModelName, factoryName);

    String perStateTransitionTypeKey =
        getStateTransitionType(getPerResourceStateTransitionPoolName(resourceName),
            message.getFromState(), message.getToState());
    if (perStateTransitionTypeKey != null && stateModelFactory != null
        && !_transitionTypeThreadpoolChecked.contains(perStateTransitionTypeKey)) {
      ExecutorService perStateTransitionTypeExecutor = stateModelFactory
          .getExecutorService(resourceName, message.getFromState(), message.getToState());
      _transitionTypeThreadpoolChecked.add(perStateTransitionTypeKey);

      if (perStateTransitionTypeExecutor != null) {
        _executorMap.put(perStateTransitionTypeKey, perStateTransitionTypeExecutor);
        LOG.info(String
            .format("Added client specified dedicate threadpool for resource %s from %s to %s",
                getPerResourceStateTransitionPoolName(resourceName), message.getFromState(),
                message.getToState()));
        return;
      }
    }

    if (!_resourcesThreadpoolChecked.contains(resourceName)) {
      int threadpoolSize = -1;
      ConfigAccessor configAccessor = manager.getConfigAccessor();
      // Changes to this configuration on thread pool size will only take effect after the participant get restarted.
      if (configAccessor != null) {
        HelixConfigScope scope =
            new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE)
                .forCluster(manager.getClusterName()).forResource(resourceName).build();

        String threadpoolSizeStr = configAccessor.get(scope, MAX_THREADS);
        try {
          if (threadpoolSizeStr != null) {
            threadpoolSize = Integer.parseInt(threadpoolSizeStr);
          }
        } catch (Exception e) {
          LOG.error(
              "Failed to parse ThreadPoolSize from resourceConfig for resource" + resourceName, e);
        }
      }
      final String key = getPerResourceStateTransitionPoolName(resourceName);
      if (threadpoolSize > 0) {
        _executorMap.put(key, Executors.newFixedThreadPool(threadpoolSize, new ThreadFactory() {
          @Override public Thread newThread(Runnable r) {
            return new Thread(r, "GerenricHelixController-message_handle_" + key);
          }
        }));
        LOG.info("Added dedicate threadpool for resource: " + resourceName + " with size: "
            + threadpoolSize);
      } else {
        // if threadpool is not configured
        // check whether client specifies customized threadpool.
        if (stateModelFactory != null) {
          ExecutorService executor = stateModelFactory.getExecutorService(resourceName);
          if (executor != null) {
            _executorMap.put(key, executor);
            LOG.info("Added client specified dedicate threadpool for resource: " + key);
          }
        } else {
          LOG.error(String.format(
              "Fail to get dedicate threadpool defined in stateModelFactory %s: using factoryName: %s for resource %s. No stateModelFactory was found!",
              stateModelName, factoryName, resourceName));
        }
      }
      _resourcesThreadpoolChecked.add(resourceName);
    }
  }

  /**
   * Find the executor service for the message. A message can have a per-statemodelfactory
   * executor service, or per-message type executor service.
   */
  ExecutorService findExecutorServiceForMsg(Message message) {
    ExecutorService executorService = _executorMap.get(message.getMsgType());
    if (message.getMsgType().equals(MessageType.STATE_TRANSITION.name())) {
      if(message.getBatchMessageMode() == true) {
        executorService = _batchMessageExecutorService;
      } else {
        String resourceName = message.getResourceName();
        if (resourceName != null) {
          String key = getPerResourceStateTransitionPoolName(resourceName);
          String perStateTransitionTypeKey =
              getStateTransitionType(key, message.getFromState(), message.getToState());
          if (perStateTransitionTypeKey != null && _executorMap.containsKey(perStateTransitionTypeKey)) {
            LOG.info(String.format("Find per state transition type thread pool for resource %s from %s to %s",
                message.getResourceName(), message.getFromState(), message.getToState()));
            executorService = _executorMap.get(perStateTransitionTypeKey);
          } else if (_executorMap.containsKey(key)) {
            LOG.info("Find per-resource thread pool with key: " + key);
            executorService = _executorMap.get(key);
          }
        }
      }
    }
    return executorService;
  }

  // ExecutorService impl's in JDK are thread-safe
  @Override
  public List<Future<HelixTaskResult>> invokeAllTasks(List<MessageTask> tasks, long timeout,
      TimeUnit unit) throws InterruptedException {
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
  public boolean cancelTimeoutTask(MessageTask task) {
    synchronized (_lock) {
      String taskId = task.getTaskId();
      if (_taskMap.containsKey(taskId)) {
        MessageTaskInfo info = _taskMap.get(taskId);
        removeMessageFromTaskAndFutureMap(task.getMessage());
        if (info._timerTask != null) {
          info._timerTask.cancel();
        }
        return true;
      }
      return false;
    }
  }

  @Override
  public boolean scheduleTask(MessageTask task) {
    String taskId = task.getTaskId();
    Message message = task.getMessage();
    NotificationContext notificationContext = task.getNotificationContext();
    HelixManager manager = notificationContext.getManager();

    try {
      // Check to see if dedicate thread pool for handling state transition messages is configured or provided.
      updateStateTransitionMessageThreadPool(message, manager);

      LOG.info("Scheduling message {}: {}:{}, {}->{}", taskId, message.getResourceName(),
          message.getPartitionName(), message.getFromState(), message.getToState());

      _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class,
          "Message handling task scheduled", manager);

      // this sync guarantees that ExecutorService.submit() task and put taskInfo into map are
      // sync'ed
      synchronized (_lock) {
        if (!_taskMap.containsKey(taskId)) {
          ExecutorService exeSvc = findExecutorServiceForMsg(message);

          if (exeSvc == null) {
            LOG.warn(String
                .format("Threadpool is null for type %s of message %s", message.getMsgType(),
                    message.getMsgId()));
            return false;
          }

          LOG.info("Submit task: " + taskId + " to pool: " + exeSvc);
          Future<HelixTaskResult> future = exeSvc.submit(task);

          _messageTaskMap
              .putIfAbsent(getMessageTarget(message.getResourceName(), message.getPartitionName()),
                  taskId);

          TimerTask timerTask = null;
          if (message.getExecutionTimeout() > 0) {
            timerTask = new MessageTimeoutTask(this, task);
            _timer.schedule(timerTask, message.getExecutionTimeout());
            LOG.info("Message starts with timeout " + message.getExecutionTimeout() + " MsgId: "
                + task.getTaskId());
          } else {
            LOG.debug("Message does not have timeout. MsgId: " + task.getTaskId());
          }
          _taskMap.put(taskId, new MessageTaskInfo(task, future, timerTask));

          LOG.info("Message: " + taskId + " handling task scheduled");
          return true;
        } else {
          _statusUpdateUtil.logWarning(message, HelixTaskExecutor.class,
              "Message handling task already sheduled for " + taskId, manager);
        }
      }
    } catch (Exception e) {
      LOG.error("Error while executing task. " + message, e);
      _statusUpdateUtil.logError(message, HelixTaskExecutor.class, e, "Error while executing task "
          + e, manager);
    }
    return false;
  }

  @Override
  public boolean cancelTask(MessageTask task) {
    Message message = task.getMessage();
    NotificationContext notificationContext = task.getNotificationContext();
    String taskId = task.getTaskId();

    synchronized (_lock) {
      if (_taskMap.containsKey(taskId)) {
        MessageTaskInfo taskInfo = _taskMap.get(taskId);
        // cancel timeout task
        if (taskInfo._timerTask != null) {
          taskInfo._timerTask.cancel();
        }

        // cancel task
        Future<HelixTaskResult> future = taskInfo.getFuture();
        removeMessageFromTaskAndFutureMap(message);
        _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class, "Canceling task: " + taskId,
            notificationContext.getManager());

        // If the thread is still running it will be interrupted if cancel(true)
        // is called. So state transition callbacks should implement logic to
        // return if it is interrupted.
        if (future.cancel(true)) {
          _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class, "Canceled task: " + taskId,
              notificationContext.getManager());
          _taskMap.remove(taskId);
          return true;
        } else {
          _statusUpdateUtil.logInfo(message, HelixTaskExecutor.class,
              "fail to cancel task: " + taskId, notificationContext.getManager());
        }
      } else {
        _statusUpdateUtil.logWarning(message, HelixTaskExecutor.class,
            "fail to cancel task: " + taskId + ", future not found",
            notificationContext.getManager());
      }
    }
    return false;
  }

  @Override
  public void finishTask(MessageTask task) {
    Message message = task.getMessage();
    String taskId = task.getTaskId();
    LOG.info("message finished: " + taskId + ", took " + (new Date().getTime() - message
        .getExecuteStartTimeStamp()));

    synchronized (_lock) {
      if (_taskMap.containsKey(taskId)) {
        MessageTaskInfo info = _taskMap.remove(taskId);
        removeMessageFromTaskAndFutureMap(message);
        if (info._timerTask != null) {
          // ok to cancel multiple times
          info._timerTask.cancel();
        }
      } else {
        LOG.warn("message " + taskId + " not found in task map");
      }
    }
  }

  private void updateMessageState(List<Message> readMsgs, HelixDataAccessor accessor,
      String instanceName) {
    Builder keyBuilder = accessor.keyBuilder();
    List<PropertyKey> readMsgKeys = new ArrayList<>();
    for (Message msg : readMsgs) {
      readMsgKeys.add(msg.getKey(keyBuilder, instanceName));
      _knownMessageIds.add(msg.getId());
    }
    accessor.setChildren(readMsgKeys, readMsgs);
  }

  private void shutdownAndAwaitTermination(ExecutorService pool) {
    LOG.info("Shutting down pool: " + pool);
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(200, TimeUnit.MILLISECONDS)) {
        List<Runnable> waitingTasks = pool.shutdownNow(); // Cancel currently executing tasks
        LOG.info("Tasks that never commenced execution: " + waitingTasks);
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(200, TimeUnit.MILLISECONDS)) {
          LOG.error("Pool did not fully terminate in 200ms. pool: " + pool);
        }
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      LOG.error("Interruped when waiting for shutdown pool: " + pool, ie);
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  /**
   * remove message-handler factory from map, shutdown the associated executor
   * @param type
   */
  void unregisterMessageHandlerFactory(String type) {
    MsgHandlerFactoryRegistryItem item = _hdlrFtyRegistry.remove(type);
    ExecutorService pool = _executorMap.remove(type);
    _monitor.removeExecutorMonitor(type);

    LOG.info(
        "Unregistering message handler factory for type: " + type + ", factory: " + item.factory()
            + ", pool: " + pool);

    if (pool != null) {
      shutdownAndAwaitTermination(pool);
    }

    // reset state-model
    if (item != null) {
      item.factory().reset();
    }

    LOG.info(
        "Unregistered message handler factory for type: " + type + ", factory: " + item.factory()
            + ", pool: " + pool);
  }

  void reset() {
    LOG.info("Reset HelixTaskExecutor");

    if (_messageQueueMonitor != null) {
      _messageQueueMonitor.reset();
    }

    for (String msgType : _hdlrFtyRegistry.keySet()) {
      // don't un-register factories, just shutdown all executors
      ExecutorService pool = _executorMap.remove(msgType);
      _monitor.removeExecutorMonitor(msgType);
      if (pool != null) {
        LOG.info("Reset exectuor for msgType: " + msgType + ", pool: " + pool);
        shutdownAndAwaitTermination(pool);
      }

      MsgHandlerFactoryRegistryItem item = _hdlrFtyRegistry.get(msgType);
      if (item.factory() != null) {
        item.factory().reset();
      }
    }

    StringBuilder sb = new StringBuilder();
    // Log all tasks that fail to terminate
    for (String taskId : _taskMap.keySet()) {
      MessageTaskInfo info = _taskMap.get(taskId);
      sb.append("Task: " + taskId + " fails to terminate. Message: " + info._task.getMessage() + "\n");
    }

    LOG.info(sb.toString());
    _taskMap.clear();

    _messageTaskMap.clear();

    _knownMessageIds.clear();

    _lastSessionSyncTime = null;
  }

  void init() {
    LOG.info("Init HelixTaskExecutor");

    if (_messageQueueMonitor != null) {
      _messageQueueMonitor.init();
    }

    _isShuttingDown = false;

    // Re-init all existing factories
    for (final String msgType : _hdlrFtyRegistry.keySet()) {
      MsgHandlerFactoryRegistryItem item = _hdlrFtyRegistry.get(msgType);
      ExecutorService newPool =
          Executors.newFixedThreadPool(item.threadPoolSize(), new ThreadFactory() {
            @Override public Thread newThread(Runnable r) {
              return new Thread(r, "HelixTaskExecutor-message_handle_" + msgType);
            }
          });
      ExecutorService prevPool = _executorMap.putIfAbsent(msgType, newPool);
      if (prevPool != null) {
        // Will happen if we register and call init
        LOG.info("Skip init a new thread pool for type: " + msgType + ", already existing pool: "
            + prevPool + ", isShutdown: " + prevPool.isShutdown());
        newPool.shutdown();
      } else {
        _monitor.createExecutorMonitor(msgType, newPool);
      }
    }
  }

  private void syncSessionToController(HelixManager manager) {
    if (_lastSessionSyncTime == null ||
            System.currentTimeMillis() - _lastSessionSyncTime > SESSION_SYNC_INTERVAL) { // > delay since last sync
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      PropertyKey key = new Builder(manager.getClusterName()).controllerMessage(SESSION_SYNC);
      if (accessor.getProperty(key) == null) {
        LOG.info(String.format("Participant %s syncs session with controller", manager.getInstanceName()));
        Message msg = new Message(MessageType.PARTICIPANT_SESSION_CHANGE, SESSION_SYNC);
        msg.setSrcName(manager.getInstanceName());
        msg.setTgtSessionId("*");
        msg.setMsgState(MessageState.NEW);
        msg.setMsgId(SESSION_SYNC);

        Criteria cr = new Criteria();
        cr.setRecipientInstanceType(InstanceType.CONTROLLER);
        cr.setSessionSpecific(false);

        manager.getMessagingService().send(cr, msg);
        _lastSessionSyncTime = System.currentTimeMillis();
      }
    }
  }

  private List<Message> readNewMessagesFromZK(HelixManager manager, String instanceName,
      HelixConstants.ChangeType changeType) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    Set<String> messageIds = new HashSet<>();
    if (changeType.equals(HelixConstants.ChangeType.MESSAGE)) {
      messageIds.addAll(accessor.getChildNames(keyBuilder.messages(instanceName)));
    } else if (changeType.equals(HelixConstants.ChangeType.MESSAGES_CONTROLLER)) {
      messageIds.addAll(accessor.getChildNames(keyBuilder.controllerMessages()));
    } else {
      LOG.warn("Unexpected ChangeType for Message Change CallbackHandler: " + changeType);
      return Collections.emptyList();
    }

    // In case the cache contains any deleted message Id, clean up
    _knownMessageIds.retainAll(messageIds);

    messageIds.removeAll(_knownMessageIds);
    List<PropertyKey> keys = new ArrayList<>();
    for (String messageId : messageIds) {
      if (changeType.equals(HelixConstants.ChangeType.MESSAGE)) {
        keys.add(keyBuilder.message(instanceName, messageId));
      } else if (changeType.equals(HelixConstants.ChangeType.MESSAGES_CONTROLLER)) {
        keys.add(keyBuilder.controllerMessage(messageId));
      }
    }

    List<Message> newMessages = accessor.getProperty(keys);
    // Message may be removed before get read, clean up null messages.
    Iterator<Message> messageIterator = newMessages.iterator();
    while(messageIterator.hasNext()) {
      if (messageIterator.next() == null) {
        messageIterator.remove();
      }
    }
    return newMessages;
  }

  @Override
  @PreFetch(enabled = false)
  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext) {
    HelixManager manager = changeContext.getManager();

    // If FINALIZE notification comes, reset all handler factories
    // and terminate all the thread pools
    // TODO: see if we should have a separate notification call for resetting
    if (changeContext.getType() == Type.FINALIZE) {
      reset();
      return;
    }

    if (changeContext.getType() == Type.INIT) {
      init();
      // continue to process messages
    }

    // if prefetch is disabled in MessageListenerCallback, we need to read all new messages from zk.
    if (messages == null || messages.isEmpty()) {
      // If no messages are given, check and read all new messages.
      messages = readNewMessagesFromZK(manager, instanceName, changeContext.getChangeType());
    }

    if (_isShuttingDown) {
      StringBuilder sb = new StringBuilder();
      for (Message message : messages) {
        sb.append(message.getMsgId() + ",");
      }
      LOG.info("Helix task executor is shutting down, discard unprocessed messages : " + sb.toString());
      return;
    }

    // Update message count
    if (_messageQueueMonitor != null) {
      _messageQueueMonitor.setMessageQueueBacklog(messages.size());
    }

    if (messages.isEmpty()) {
      LOG.info("No Messages to process");
      return;
    }

    // sort message by creation timestamp, so message created earlier is processed first
    Collections.sort(messages, Message.CREATE_TIME_COMPARATOR);

    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    // message handlers and corresponding contexts created
    Map<String, MessageHandler> stateTransitionHandlers = new HashMap<>();
    Map<String, NotificationContext> stateTransitionContexts = new HashMap<>();

    List<MessageHandler> nonStateTransitionHandlers = new ArrayList<>();
    List<NotificationContext> nonStateTransitionContexts = new ArrayList<>();

    // message read
    List<Message> readMsgs = new ArrayList<>();

    String sessionId = manager.getSessionId();
    List<String> curResourceNames =
        accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId));
    List<PropertyKey> createCurStateKeys = new ArrayList<>();
    List<CurrentState> metaCurStates = new ArrayList<>();
    Set<String> createCurStateNames = new HashSet<>();

    for (Message message : messages) {
      // nop messages are simply removed. It is used to trigger onMessage() in
      // situations such as register a new message handler factory
      if (message.getMsgType().equalsIgnoreCase(MessageType.NO_OP.toString())) {
        LOG.info(
            "Dropping NO-OP message. mid: " + message.getId() + ", from: " + message.getMsgSrc());
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.DISCARDED);
        continue;
      }

      String tgtSessionId = message.getTgtSessionId();
      // sessionId mismatch normally means message comes from expired session, just remove it
      if (!sessionId.equals(tgtSessionId) && !tgtSessionId.equals("*")) {
        String warningMessage =
            "SessionId does NOT match. expected sessionId: " + sessionId
                + ", tgtSessionId in message: " + tgtSessionId + ", messageId: "
                + message.getMsgId();
        LOG.warn(warningMessage);
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.DISCARDED);
        _statusUpdateUtil.logWarning(message, HelixStateMachineEngine.class, warningMessage, manager);

        // Proactively send a session sync message from participant to controller
        // upon session mismatch after a new session is established
        if (manager.getInstanceType() == InstanceType.PARTICIPANT
            || manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
          if (message.getCreateTimeStamp() > manager.getSessionStartTime()) {
            syncSessionToController(manager);
          }
        }
        continue;
      }

      if ((manager.getInstanceType() == InstanceType.CONTROLLER
          || manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT)
          && MessageType.PARTICIPANT_SESSION_CHANGE.name().equals(message.getMsgType())) {
        LOG.info(String.format("Controller received PARTICIPANT_SESSION_CHANGE msg from src: %s",
            message.getMsgSrc()));
        PropertyKey key = new Builder(manager.getClusterName()).liveInstances();
        List<LiveInstance> liveInstances = manager.getHelixDataAccessor().getChildValues(key);
        _controller.onLiveInstanceChange(liveInstances, changeContext);
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.COMPLETED);
        continue;
      }

      // don't process message that is of READ or UNPROCESSABLE state
      if (MessageState.NEW != message.getMsgState()) {
        // It happens because we don't delete message right after
        // read. Instead we keep it until the current state is updated.
        // We will read the message again if there is a new message but we
        // check for the status and ignore if its already read
        if (LOG.isTraceEnabled()) {
          LOG.trace("Message already read. msgId: " + message.getMsgId());
        }
        continue;
      }

      if (message.isExpired()) {
        LOG.info(
            "Dropping expired message. mid: " + message.getId() + ", from: " + message.getMsgSrc()
                + " relayed from: " + message.getRelaySrcHost());
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.DISCARDED);
        continue;
      }

      // State Transition Cancellation
      if (message.getMsgType().equals(MessageType.STATE_TRANSITION_CANCELLATION.name())) {
        boolean success = cancelNotStartedStateTransition(message, stateTransitionHandlers, accessor, instanceName);
        if (success) {
          continue;
        }
      }

      _monitor.reportReceivedMessage(message);

      // create message handlers, if handlers not found, leave its state as NEW
      NotificationContext msgWorkingContext = changeContext.clone();
      try {
        MessageHandler createHandler = createMessageHandler(message, msgWorkingContext);
        if (createHandler == null) {
          continue;
        }
        if (message.getMsgType().equals(MessageType.STATE_TRANSITION.name()) || message.getMsgType()
            .equals(MessageType.STATE_TRANSITION_CANCELLATION.name())) {
          String messageTarget =
              getMessageTarget(message.getResourceName(), message.getPartitionName());
          if (stateTransitionHandlers.containsKey(messageTarget)) {
            // If there are 2 messages in same batch about same partition's state transition,
            // the later one is discarded
            Message duplicatedMessage = stateTransitionHandlers.get(messageTarget)._message;
            throw new HelixException(String.format(
                "Duplicated state transition message: %s. Existing: %s->%s; New (Discarded): %s->%s",
                message.getMsgId(), duplicatedMessage.getFromState(),
                duplicatedMessage.getToState(), message.getFromState(), message.getToState()));
          } else if (message.getMsgType().equals(MessageType.STATE_TRANSITION.name())
              && isStateTransitionInProgress(messageTarget)) {

            String taskId = _messageTaskMap.get(messageTarget);
            Message msg = _taskMap.get(taskId).getTask().getMessage();

            // If there is another state transition for same partition is going on,
            // discard the message. Controller will resend if this is a valid message
            throw new HelixException(String.format(
                "Another state transition for %s:%s is in progress with msg: %s, p2p: %s, read: %d, current:%d. Discarding %s->%s message",
                message.getResourceName(), message.getPartitionName(), msg.getMsgId(), String.valueOf(msg.isRelayMessage()),
                msg.getReadTimeStamp(), System.currentTimeMillis(), message.getFromState(),
                message.getToState()));
          }

          stateTransitionHandlers
              .put(getMessageTarget(message.getResourceName(), message.getPartitionName()),
                  createHandler);
          stateTransitionContexts
              .put(getMessageTarget(message.getResourceName(), message.getPartitionName()),
                  msgWorkingContext);
        } else {
          nonStateTransitionHandlers.add(createHandler);
          nonStateTransitionContexts.add(msgWorkingContext);
        }
      } catch (Exception e) {
        LOG.error("Failed to create message handler for " + message.getMsgId(), e);
        String error =
            "Failed to create message handler for " + message.getMsgId() + ", exception: " + e;

        _statusUpdateUtil.logError(message, HelixStateMachineEngine.class, e, error, manager);

        message.setMsgState(MessageState.UNPROCESSABLE);
        removeMessageFromZK(accessor, message, instanceName);
        LOG.error("Message cannot be processed: " + message.getRecord(), e);
        _monitor.reportProcessedMessage(message, ParticipantMessageMonitor.ProcessedMessageState.DISCARDED);
        continue;
      }

      markReadMessage(message, msgWorkingContext, manager);
      readMsgs.add(message);

      // batch creation of all current state meta data
      // do it for non-controller and state transition messages only
      if (!message.isControlerMsg()
          && message.getMsgType().equals(Message.MessageType.STATE_TRANSITION.name())) {
        String resourceName = message.getResourceName();
        if (!curResourceNames.contains(resourceName) && !createCurStateNames.contains(resourceName)) {
          createCurStateNames.add(resourceName);
          createCurStateKeys.add(keyBuilder.currentState(instanceName, sessionId, resourceName));

          CurrentState metaCurState = new CurrentState(resourceName);
          metaCurState.setBucketSize(message.getBucketSize());
          metaCurState.setStateModelDefRef(message.getStateModelDef());
          metaCurState.setSessionId(sessionId);
          metaCurState.setBatchMessageMode(message.getBatchMessageMode());
          String ftyName = message.getStateModelFactoryName();
          if (ftyName != null) {
            metaCurState.setStateModelFactoryName(ftyName);
          } else {
            metaCurState.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
          }

          metaCurStates.add(metaCurState);
        }
      }
    }

    // batch create curState meta
    if (createCurStateKeys.size() > 0) {
      try {
        accessor.createChildren(createCurStateKeys, metaCurStates);
      } catch (Exception e) {
        LOG.error("fail to create cur-state znodes for messages: " + readMsgs, e);
      }
    }

    // update message state to READ in batch and schedule all read messages
    if (readMsgs.size() > 0) {
      updateMessageState(readMsgs, accessor, instanceName);

      for (Map.Entry<String, MessageHandler> handlerEntry : stateTransitionHandlers.entrySet()) {
        MessageHandler handler = handlerEntry.getValue();
        NotificationContext context = stateTransitionContexts.get(handlerEntry.getKey());
        Message msg = handler._message;
        scheduleTask(
            new HelixTask(msg, context, handler, this)
        );
      }

      for (int i = 0; i < nonStateTransitionHandlers.size(); i++) {
        MessageHandler handler = nonStateTransitionHandlers.get(i);
        NotificationContext context = nonStateTransitionContexts.get(i);
        Message msg = handler._message;
        scheduleTask(
            new HelixTask(msg, context, handler, this)
        );
      }
    }
  }

  /**
   * Check if a state transition of the given message target is in progress. This function
   * assumes the given message target corresponds to a state transition task
   *
   * @param messageTarget message target generated by getMessageTarget()
   * @return true if there is a task going on with same message target else false
   */
  private boolean isStateTransitionInProgress(String messageTarget) {
    synchronized (_lock) {
      if (_messageTaskMap.containsKey(messageTarget)) {
        String taskId = _messageTaskMap.get(messageTarget);
        return !_taskMap.get(taskId).getFuture().isDone();
      }
      return false;
    }
  }

  // Try to cancel this state transition that has not been started yet.
  // Three Types of Cancellation: 1. Message arrived with previous state transition
  //                              2. Message handled but task not started
  //                              3. Message handled and task already started
  // This method tries to handle the first two cases, it returns true if no further cancellation is needed,
  // false if not been able to cancel the state transition (i.e, further cancellation is needed).
  private boolean cancelNotStartedStateTransition(Message message, Map<String, MessageHandler> stateTransitionHandlers,
      HelixDataAccessor accessor, String instanceName) {
    String targetMessageName = getMessageTarget(message.getResourceName(), message.getPartitionName());
    ProcessedMessageState messageState;
    Message targetStateTransitionMessage;

    // State transition message and cancel message are in same batch
    if (stateTransitionHandlers.containsKey(targetMessageName)) {
      targetStateTransitionMessage = stateTransitionHandlers.get(targetMessageName).getMessage();
      if (isCancelingSameStateTransition(targetStateTransitionMessage, message)) {
        stateTransitionHandlers.remove(targetMessageName);
        messageState = ProcessedMessageState.COMPLETED;
      } else {
        messageState = ProcessedMessageState.DISCARDED;
      }
    } else if (_messageTaskMap.containsKey(targetMessageName)) {
      // Cancel the from future without interrupt ->  Cancel the task future without
      // interruptting the state transition that is already started.  If the state transition
      // is already started, we should call cancel in the state model.
      String taskId = _messageTaskMap.get(targetMessageName);
      HelixTask task = (HelixTask) _taskMap.get(taskId).getTask();
      Future<HelixTaskResult> future = _taskMap.get(taskId).getFuture();
      targetStateTransitionMessage = task.getMessage();

      if (isCancelingSameStateTransition(task.getMessage(), message)) {
        boolean success = task.cancel();
        if (!success) {
          // the state transition is already started, need further cancellation.
          return false;
        }

        future.cancel(false);
        _messageTaskMap.remove(targetMessageName);
        _taskMap.remove(taskId);
        messageState = ProcessedMessageState.COMPLETED;
      } else {
        messageState = ProcessedMessageState.DISCARDED;
      }
    } else {
      return false;
    }

    // remove the original state-transition message been cancelled.
    removeMessageFromZK(accessor, targetStateTransitionMessage, instanceName);
    _monitor.reportProcessedMessage(targetStateTransitionMessage,
        ParticipantMessageMonitor.ProcessedMessageState.DISCARDED);

    // remove the state transition cancellation message
    reportAndRemoveMessage(message, accessor, instanceName, messageState);

    return true;
  }

  private void reportAndRemoveMessage(Message message, HelixDataAccessor accessor,
      String instanceName, ProcessedMessageState messageProcessState) {
    _monitor.reportReceivedMessage(message);
    _monitor.reportProcessedMessage(message, messageProcessState);
    removeMessageFromZK(accessor, message, instanceName);
  }

  private void markReadMessage(Message message, NotificationContext context,
      HelixManager manager) {
    message.setMsgState(MessageState.READ);
    message.setReadTimeStamp(new Date().getTime());
    message.setExecuteSessionId(context.getManager().getSessionId());

    _statusUpdateUtil.logInfo(message, HelixStateMachineEngine.class, "New Message", manager);
  }

  public MessageHandler createMessageHandler(Message message, NotificationContext changeContext) {
    String msgType = message.getMsgType().toString();

    MsgHandlerFactoryRegistryItem item = _hdlrFtyRegistry.get(msgType);

    // Fail to find a MessageHandlerFactory for the message
    // we will keep the message and the message will be handled when
    // the corresponding MessageHandlerFactory is registered
    if (item == null) {
      LOG.warn("Fail to find message handler factory for type: " + msgType + " msgId: "
          + message.getMsgId());
      return null;
    }
    MessageHandlerFactory handlerFactory = item.factory();

    // pass the executor to msg-handler since batch-msg-handler needs task-executor to schedule
    // sub-msgs
    changeContext.add(MapKey.TASK_EXECUTOR.toString(), this);
    return handlerFactory.createHandler(message, changeContext);
  }

  private void removeMessageFromTaskAndFutureMap(Message message) {
    _knownMessageIds.remove(message.getId());
    String messageTarget = getMessageTarget(message.getResourceName(), message.getPartitionName());
    if (_messageTaskMap.containsKey(messageTarget)) {
      _messageTaskMap.remove(messageTarget);
    }
  }

  private boolean isCancelingSameStateTransition(Message stateTranstionMessage,
      Message cancellationMessage) {
    return stateTranstionMessage.getFromState().equalsIgnoreCase(cancellationMessage.getFromState())
        && stateTranstionMessage.getToState().equalsIgnoreCase(cancellationMessage.getToState());
  }

  String getMessageTarget(String resourceName, String partitionName) {
    return String.format("%s_%s", resourceName, partitionName);
  }

  private String getStateTransitionType(String prefix, String fromState, String toState){
    if (prefix == null || fromState == null || toState == null) {
      return null;
    }
    return String.format("%s.%s.%s", prefix, fromState, toState);
  }

  private String getPerResourceStateTransitionPoolName(String resourceName) {
    return MessageType.STATE_TRANSITION.name() + "." + resourceName;
  }

  private void removeMessageFromZK(HelixDataAccessor accessor, Message message,
      String instanceName) {
    if (HelixUtil.removeMessageFromZK(accessor, message, instanceName)) {
      LOG.info("Successfully removed message {} from ZK.", message.getMsgId());
    } else {
      LOG.warn("Failed to remove message {} from ZK.", message.getMsgId());
    }
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down HelixTaskExecutor");
    _isShuttingDown = true;
    _timer.cancel();

    reset();
    _monitor.shutDown();
    LOG.info("Shutdown HelixTaskExecutor finished");
  }
}
