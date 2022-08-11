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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.helix.AccessOption;
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
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.manager.zk.ParticipantManager;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.LiveInstance.LiveInstanceStatus;
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
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.StatusUpdateUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelixTaskExecutor implements MessageListener, TaskExecutor {
  /**
   * Put together all registration information about a message handler factory
   */
  class MsgHandlerFactoryRegistryItem {
    private final MessageHandlerFactory _factory;
    private final int _threadPoolSize;
    private final int _resetTimeout;

    public MsgHandlerFactoryRegistryItem(MessageHandlerFactory factory, int threadPoolSize, int resetTimeout) {
      if (factory == null) {
        throw new NullPointerException("Message handler factory is null");
      }

      if (threadPoolSize <= 0) {
        throw new IllegalArgumentException("Illegal thread pool size: " + threadPoolSize);
      }

      if (resetTimeout < 0) {
        throw new IllegalArgumentException("Illegal reset timeout: " + resetTimeout);
      }

      _factory = factory;
      _threadPoolSize = threadPoolSize;
      _resetTimeout = resetTimeout;
    }

    int threadPoolSize() {
      return _threadPoolSize;
    }

    int getResetTimeout() {
      return _resetTimeout;
    }

    MessageHandlerFactory factory() {
      return _factory;
    }
  }

  private static Logger LOG = LoggerFactory.getLogger(HelixTaskExecutor.class);

  private static AtomicLong thread_uid = new AtomicLong(0);
  // TODO: we need to further design how to throttle this.
  // From storage point of view, only bootstrap case is expensive
  // and we need to throttle, which is mostly IO / network bounded.
  public static final int DEFAULT_PARALLEL_TASKS = TaskExecutor.DEFAULT_PARALLEL_TASKS;
  // TODO: create per-task type threadpool with customizable pool size
  protected final Map<String, MessageTaskInfo> _taskMap;
  private final Object _lock;
  private final StatusUpdateUtil _statusUpdateUtil;
  private final ParticipantStatusMonitor _monitor;
  public static final String MAX_THREADS = "maxThreads";

  // true if all partition state are "clean" as same after reset()
  private volatile boolean _isCleanState = true;
  private MessageQueueMonitor _messageQueueMonitor;
  private GenericHelixController _controller;
  private Long _lastSessionSyncTime;
  private String _freezeSessionId;
  private LiveInstanceStatus _liveInstanceStatus;
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
    _knownMessageIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    _batchMessageExecutorService = Executors.newCachedThreadPool();
    _monitor.createExecutorMonitor("BatchMessageExecutor", _batchMessageExecutorService);

    _resourcesThreadpoolChecked = Collections.newSetFromMap(new ConcurrentHashMap<>());
    _transitionTypeThreadpoolChecked = Collections.newSetFromMap(new ConcurrentHashMap<>());

    _lock = new Object();
    _statusUpdateUtil = new StatusUpdateUtil();

    // created as a daemon timer thread to handle task timeout
    _timer = new Timer("HelixTaskExecutor_Timer", true);

    _isShuttingDown = false;
    _liveInstanceStatus = LiveInstanceStatus.NORMAL;

    startMonitorThread();
  }

  @Override
  public void registerMessageHandlerFactory(MultiTypeMessageHandlerFactory factory,
      int threadPoolSize, int resetTimeoutMs) {
    for (String type : factory.getMessageTypes()) {
      registerMessageHandlerFactory(type, factory, threadPoolSize, resetTimeoutMs);
    }
  }

  @Override
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory) {
    registerMessageHandlerFactory(type, factory, DEFAULT_PARALLEL_TASKS);
  }

  @Override
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory, int threadpoolSize) {
    registerMessageHandlerFactory(type, factory, threadpoolSize, DEFAULT_MSG_HANDLER_RESET_TIMEOUT_MS);
  }

  private void registerMessageHandlerFactory(String type, MessageHandlerFactory factory, int threadpoolSize,
      int resetTimeoutMs) {
    if (factory instanceof MultiTypeMessageHandlerFactory) {
      if (!((MultiTypeMessageHandlerFactory) factory).getMessageTypes().contains(type)) {
        throw new HelixException("Message factory type mismatch. Type: " + type + ", factory: "
            + ((MultiTypeMessageHandlerFactory) factory).getMessageTypes());
      }
    } else {
      if (!factory.getMessageType().equals(type)) {
        throw new HelixException(
            "Message factory type mismatch. Type: " + type + ", factory: " + factory
                .getMessageType());
      }
    }

    _isShuttingDown = false;

    MsgHandlerFactoryRegistryItem newItem = new MsgHandlerFactoryRegistryItem(factory, threadpoolSize, resetTimeoutMs);
    MsgHandlerFactoryRegistryItem prevItem = _hdlrFtyRegistry.putIfAbsent(type, newItem);
    if (prevItem == null) {
      _executorMap.computeIfAbsent(type, msgType -> {
        ExecutorService newPool = Executors.newFixedThreadPool(threadpoolSize, r -> new Thread(r,
            "HelixTaskExecutor-message_handle_thread_" + thread_uid.getAndIncrement()));
        _monitor.createExecutorMonitor(type, newPool);
        return newPool;
      });
      LOG.info("Registered message handler factory for type: {}, poolSize: {}, factory: {}, pool: {}", type,
          threadpoolSize, factory, _executorMap.get(type));
    } else {
      LOG.info(
          "Skip register message handler factory for type: {}, poolSize: {}, factory: {}, already existing factory: {}",
          type, threadpoolSize, factory, prevItem.factory());
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
        HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE)
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
        _executorMap.put(key, Executors.newFixedThreadPool(threadpoolSize,
            r -> new Thread(r, "GerenricHelixController-message_handle_" + key)));
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
      if (message.getBatchMessageMode() == true) {
        executorService = _batchMessageExecutorService;
      } else {
        String resourceName = message.getResourceName();
        if (resourceName != null) {
          String key = getPerResourceStateTransitionPoolName(resourceName);
          String perStateTransitionTypeKey =
              getStateTransitionType(key, message.getFromState(), message.getToState());
          if (perStateTransitionTypeKey != null && _executorMap
              .containsKey(perStateTransitionTypeKey)) {
            LOG.info(String
                .format("Find per state transition type thread pool for resource %s from %s to %s",
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

      _statusUpdateUtil
          .logInfo(message, HelixTaskExecutor.class, "Message handling task scheduled", manager);

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
            LOG.info(
                "Message starts with timeout " + message.getExecutionTimeout() + " MsgId: " + task
                    .getTaskId());
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
      _statusUpdateUtil
          .logError(message, HelixTaskExecutor.class, e, "Error while executing task " + e,
              manager);
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
          _statusUpdateUtil
              .logInfo(message, HelixTaskExecutor.class, "fail to cancel task: " + taskId,
                  notificationContext.getManager());
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

  private void updateMessageState(Collection<Message> msgsToBeUpdated, HelixDataAccessor accessor,
      String instanceName) {
    if (msgsToBeUpdated.isEmpty()) {
      return;
    }

    Builder keyBuilder = accessor.keyBuilder();
    List<Message> updateMsgs = new ArrayList<>();
    List<String> updateMsgPaths = new ArrayList<>();
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<>();
    for (Message msg : msgsToBeUpdated) {
      updateMsgs.add(msg);
      updateMsgPaths.add(msg.getKey(keyBuilder, instanceName).getPath());
      /**
       * We use the updater to avoid race condition between writing message to zk as READ state and removing message after ST is done
       * If there is no message at this path, meaning the message is removed so we do not write the message
       */
      updaters.add(currentData -> {
        if (currentData == null) {
          LOG.warn(
              "Message {} targets at {} has already been removed before it is set as READ on instance {}",
              msg.getId(), msg.getTgtName(), instanceName);
          return null;
        }
        return msg.getRecord();
      });
    }
    boolean[] updateResults =
        accessor.updateChildren(updateMsgPaths, updaters, AccessOption.PERSISTENT);

    boolean isMessageUpdatedAsNew = false;
    // Note that only cache the known message Ids after the update to ZK is successfully done.
    // This is to avoid inconsistent cache.
    for (int i = 0; i < updateMsgs.size(); i++) {
      Message msg = updateMsgs.get(i);
      if (msg.getMsgState().equals(MessageState.NEW)) {
        // If a message is updated as NEW state, then we might need to process it again soon.
        isMessageUpdatedAsNew = true;
        // And it shall not be treated as a known messages.
      } else {
        _knownMessageIds.add(msg.getId());
        if (!updateResults[i]) {
          // TODO: If the message update fails, maybe we shall not treat the message as a known
          // TODO: message. We shall apply more strict check and retry the update.
          LOG.error("Failed to update the message {}.", msg.getMsgId());
        }
      }
    }

    if (isMessageUpdatedAsNew) {
      // Sending a NO-OP message to trigger another message callback to re-process the messages
      // that are updated as NEW state.
      sendNopMessage(accessor, instanceName);
    }
  }

  private void shutdownAndAwaitTermination(ExecutorService pool, MsgHandlerFactoryRegistryItem handlerItem) {
    LOG.info("Shutting down pool: " + pool);

    int timeout = handlerItem == null? DEFAULT_MSG_HANDLER_RESET_TIMEOUT_MS : handlerItem.getResetTimeout();

    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
        List<Runnable> waitingTasks = pool.shutdownNow(); // Cancel currently executing tasks
        LOG.info("Tasks that never commenced execution after {}: {}", timeout,
            waitingTasks);
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
          LOG.error("Pool did not fully terminate in {} ms. pool: {}", timeout, pool);
        }
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      LOG.error("Interrupted when waiting for shutdown pool: " + pool, ie);
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
      shutdownAndAwaitTermination(pool, item);
    }

    // reset state-model
    if (item != null) {
      item.factory().reset();
    }

    LOG.info(
        "Unregistered message handler factory for type: " + type + ", factory: " + item.factory()
            + ", pool: " + pool);
  }

  private void syncFactoryState() {
    LOG.info("Start to sync factory state");
    // Lock on the registry to avoid race condition when concurrently calling sync() and reset()
    synchronized (_hdlrFtyRegistry) {
      for (Map.Entry<String, MsgHandlerFactoryRegistryItem> entry : _hdlrFtyRegistry.entrySet()) {
        MsgHandlerFactoryRegistryItem item = entry.getValue();
        if (item.factory() != null) {
          try {
            item.factory().sync();
          } catch (Exception ex) {
            LOG.error("Failed to syncState the factory {} of message type {}.", item.factory(),
                entry.getKey(), ex);
          }
        }
      }
    }
  }

  /**
   * Shutdown the registered thread pool executors. This method will be no-op if called repeatedly.
   */
  private void shutdownExecutors() {
    synchronized (_hdlrFtyRegistry) {
      for (String msgType : _hdlrFtyRegistry.keySet()) {
        // don't un-register factories, just shutdown all executors
        MsgHandlerFactoryRegistryItem item = _hdlrFtyRegistry.get(msgType);
        ExecutorService pool = _executorMap.remove(msgType);
        _monitor.removeExecutorMonitor(msgType);
        if (pool != null) {
          LOG.info("Reset executor for msgType: " + msgType + ", pool: " + pool);
          shutdownAndAwaitTermination(pool, item);
        }
      }
    }
  }

  synchronized void reset() {
    if (_isCleanState) {
      LOG.info("HelixTaskExecutor is in clean state, no need to reset again");
      return;
    }
    LOG.info("Reset HelixTaskExecutor");

    if (_messageQueueMonitor != null) {
      _messageQueueMonitor.reset();
    }

    shutdownExecutors();

    synchronized (_hdlrFtyRegistry) {
      _hdlrFtyRegistry.values()
          .stream()
          .map(MsgHandlerFactoryRegistryItem::factory)
          .distinct()
          .filter(Objects::nonNull)
          .forEach(factory -> {
            try {
              factory.reset();
            } catch (Exception ex) {
              LOG.error("Failed to reset the factory {}.", factory.toString(), ex);
            }
          });
    }
    // threads pool specific to STATE_TRANSITION.Key specific pool are not shut down.
    // this is a potential area to improve. https://github.com/apache/helix/issues/1245

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
    _isCleanState = true;
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
      ExecutorService pool = _executorMap.computeIfAbsent(msgType, type -> {
        ExecutorService newPool = Executors.newFixedThreadPool(item.threadPoolSize(),
            r -> new Thread(r, "HelixTaskExecutor-message_handle_" + type));
        _monitor.createExecutorMonitor(type, newPool);
        return newPool;
      });
      LOG.info("Setup the thread pool for type: {}, isShutdown: {}", msgType, pool.isShutdown());
    }
  }

  private void syncSessionToController(HelixManager manager) {
    if (_lastSessionSyncTime == null || System.currentTimeMillis() - _lastSessionSyncTime
        > SESSION_SYNC_INTERVAL) { // > delay since last sync
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      PropertyKey key = new Builder(manager.getClusterName()).controllerMessage(SESSION_SYNC);
      if (accessor.getProperty(key) == null) {
        LOG.info(String
            .format("Participant %s syncs session with controller", manager.getInstanceName()));
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

    // Avoid reading the already known messages.
    messageIds.removeAll(_knownMessageIds);
    List<PropertyKey> keys = new ArrayList<>();
    for (String messageId : messageIds) {
      if (changeType.equals(HelixConstants.ChangeType.MESSAGE)) {
        keys.add(keyBuilder.message(instanceName, messageId));
      } else if (changeType.equals(HelixConstants.ChangeType.MESSAGES_CONTROLLER)) {
        keys.add(keyBuilder.controllerMessage(messageId));
      }
    }

    /**
     * Do not throw exception on partial message read.
     * 1. There is no way to resolve the error on the participant side. And once it fails here, we
     * are running the risk of ignoring the message change event. And the participant might be stuck.
     * 2. Even this is a partial read, we have another chance to retry in the business logic since
     * as long as the participant processes messages, it will touch the message folder and triggers
     * another message event.
     */
    List<Message> newMessages = accessor.getProperty(keys, false);
    // Message may be removed before get read, clean up null messages.
    Iterator<Message> messageIterator = newMessages.iterator();
    while (messageIterator.hasNext()) {
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
    _isCleanState = false;

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
      LOG.info(
          "Helix task executor is shutting down, ignore unprocessed messages : " + sb.toString());
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

    // message to be updated in ZK
    Map<String, Message> msgsToBeUpdated = new HashMap<>();

    String sessionId = manager.getSessionId();
    List<String> curResourceNames =
        accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId));
    List<String> taskCurResourceNames =
        accessor.getChildNames(keyBuilder.taskCurrentStates(instanceName, sessionId));
    List<PropertyKey> createCurStateKeys = new ArrayList<>();
    List<CurrentState> metaCurStates = new ArrayList<>();
    Set<String> createCurStateNames = new HashSet<>();

    for (Message message : messages) {
      if (checkAndProcessNoOpMessage(message, instanceName, changeContext, manager, sessionId,
          stateTransitionHandlers)) {
        // skip the following operations for the no-op messages.
        continue;
      }
      NotificationContext msgWorkingContext = changeContext.clone();
      MessageHandler msgHandler = null;
      try {
        // create message handlers, if handlers not found but no exception, leave its state as NEW
        msgHandler = createMessageHandler(message, msgWorkingContext);
      } catch (Exception ex) {
        // Failed to create message handler and there is an Exception.
        int remainingRetryCount = message.getRetryCount();
        LOG.error(
            "Exception happens when creating Message Handler for message {}. Current remaining retry count is {}.",
            message.getMsgId(), remainingRetryCount);
        // Reduce the message retry count to avoid infinite retrying.
        message.setRetryCount(remainingRetryCount - 1);
        message.setExecuteSessionId(sessionId);
        // Note that we are re-using the retry count of Message that was original designed to control
        // timeout retries. So it is not checked before the first try in order to ensure consistent
        // behavior. It is possible that we introduce a new behavior for this method. But it requires
        // us to split the configuration item so as to avoid confusion.
        if (message.getRetryCount() <= 0) {
          // If no more retry count remains, then mark the message to be UNPROCESSABLE.
          String errorMsg = String.format("No available message Handler found!"
                  + " Stop processing message %s since it has zero or negative remaining retry count %d!",
              message.getMsgId(), message.getRetryCount());
          updateUnprocessableMessage(message, null, errorMsg, manager);
        }
        msgsToBeUpdated.put(message.getId(), message);
        // continue processing in the next section where handler object is double-checked.
      }

      if (msgHandler == null) {
        // Skip processing this message in this callback. The same message process will be retried
        // in the next round if retry count > 0.
        LOG.warn("There is no existing handler for message {}."
            + " Skip processing it for now. Will retry on the next callback.", message.getMsgId());
        continue;
      }

      if (message.getMsgType().equals(MessageType.STATE_TRANSITION.name()) || message.getMsgType()
          .equals(MessageType.STATE_TRANSITION_CANCELLATION.name())) {
        if (validateAndProcessStateTransitionMessage(message, manager, stateTransitionHandlers,
            msgHandler)) {
          // Need future process by triggering state transition
          String msgTarget =
              getMessageTarget(message.getResourceName(), message.getPartitionName());
          stateTransitionHandlers.put(msgTarget, msgHandler);
          stateTransitionContexts.put(msgTarget, msgWorkingContext);
        } else {
          // Skip the following operations for the invalid/expired state transition messages.
          // Also remove the message since it might block the other state transition messages.
          removeMessageFromZK(accessor, message, instanceName);
          continue;
        }
      } else {
        // Need future process non state transition messages by triggering the handler
        nonStateTransitionHandlers.add(msgHandler);
        nonStateTransitionContexts.add(msgWorkingContext);
      }

      // Update the normally processed messages
      Message markedMsg = markReadMessage(message, msgWorkingContext, manager);
      msgsToBeUpdated.put(markedMsg.getId(), markedMsg);

      // batch creation of all current state meta data
      // do it for non-controller and state transition messages only
      if (!message.isControlerMsg() && message.getMsgType()
          .equals(Message.MessageType.STATE_TRANSITION.name())) {
        String resourceName = message.getResourceName();
        if (!curResourceNames.contains(resourceName) && !taskCurResourceNames.contains(resourceName)
            && !createCurStateNames.contains(resourceName)) {
          createCurStateNames.add(resourceName);
          PropertyKey curStateKey = keyBuilder.currentState(instanceName, sessionId, resourceName);
          if (TaskConstants.STATE_MODEL_NAME.equals(message.getStateModelDef()) && !Boolean
              .getBoolean(SystemPropertyKeys.TASK_CURRENT_STATE_PATH_DISABLED)) {
            curStateKey = keyBuilder.taskCurrentState(instanceName, sessionId, resourceName);
          }
          createCurStateKeys.add(curStateKey);

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
        LOG.error("fail to create cur-state znodes for messages: " + msgsToBeUpdated, e);
      }
    }

    // update message state in batch and schedule tasks for all read messages
    updateMessageState(msgsToBeUpdated.values(), accessor, instanceName);

    for (Map.Entry<String, MessageHandler> handlerEntry : stateTransitionHandlers.entrySet()) {
      MessageHandler handler = handlerEntry.getValue();
      NotificationContext context = stateTransitionContexts.get(handlerEntry.getKey());
      if (!scheduleTaskForMessage(instanceName, accessor, handler, context) && !_isShuttingDown) {
        /**
         * TODO: Checking _isShuttingDown is a workaround to avoid unnecessary ERROR partition.
         * TODO: We shall improve the shutdown process of the participant to clean up the workflow
         * TODO: completely. In detail, there isa race condition between TaskExecutor thread
         * TODO: pool shutting down and Message handler stops listening. In this gap, the message
         * TODO: will still be processed but schedule will fail. If we mark partition into ERROR
         * TODO: state, then the controller side logic might be confused.
         */
        try {
          // Record error state to the message handler.
          handler.onError(new HelixException(String
                  .format("Failed to schedule the task for executing message handler for %s.",
                      handler._message.getMsgId())), MessageHandler.ErrorCode.ERROR,
              MessageHandler.ErrorType.FRAMEWORK);
        } catch (Exception ex) {
          LOG.error("Failed to trigger onError method of the message handler for {}",
              handler._message.getMsgId(), ex);
        }
      }
    }

    for (int i = 0; i < nonStateTransitionHandlers.size(); i++) {
      MessageHandler handler = nonStateTransitionHandlers.get(i);
      NotificationContext context = nonStateTransitionContexts.get(i);
      scheduleTaskForMessage(instanceName, accessor, handler, context);
    }
  }

  /**
   * Inspect the message. Report and remove it if no operation needs to be done.
   * @param message
   * @param instanceName
   * @param changeContext
   * @param manager
   * @param sessionId
   * @param stateTransitionHandlers
   * @return True if the message is no-op message and no other process step is required.
   */
  private boolean checkAndProcessNoOpMessage(Message message, String instanceName,
      NotificationContext changeContext, HelixManager manager, String sessionId,
      Map<String, MessageHandler> stateTransitionHandlers) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    try {
      // nop messages are simply removed. It is used to trigger onMessage() in
      // situations such as register a new message handler factory
      if (message.getMsgType().equalsIgnoreCase(MessageType.NO_OP.toString())) {
        LOG.info(
            "Dropping NO-OP message. mid: " + message.getId() + ", from: " + message.getMsgSrc());
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.DISCARDED);
        return true;
      }

      String tgtSessionId = message.getTgtSessionId();
      // sessionId mismatch normally means message comes from expired session, just remove it
      if (!sessionId.equals(tgtSessionId) && !tgtSessionId.equals("*")) {
        String warningMessage = "SessionId does NOT match. expected sessionId: " + sessionId
            + ", tgtSessionId in message: " + tgtSessionId + ", messageId: " + message.getMsgId();
        LOG.warn(warningMessage);
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.DISCARDED);
        _statusUpdateUtil
            .logWarning(message, HelixStateMachineEngine.class, warningMessage, manager);

        // Proactively send a session sync message from participant to controller
        // upon session mismatch after a new session is established
        if (manager.getInstanceType() == InstanceType.PARTICIPANT
            || manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
          if (message.getCreateTimeStamp() > manager.getSessionStartTime()) {
            syncSessionToController(manager);
          }
        }
        return true;
      }

      if ((manager.getInstanceType() == InstanceType.CONTROLLER
          || manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT)
          && MessageType.PARTICIPANT_SESSION_CHANGE.name().equals(message.getMsgType())) {
        LOG.info(String.format("Controller received PARTICIPANT_SESSION_CHANGE msg from src: %s",
            message.getMsgSrc()));
        PropertyKey key = new Builder(manager.getClusterName()).liveInstances();
        List<LiveInstance> liveInstances = manager.getHelixDataAccessor().getChildValues(key, true);
        _controller.onLiveInstanceChange(liveInstances, changeContext);
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.COMPLETED);
        return true;
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
        return true;
      }

      if (message.isExpired()) {
        LOG.info(
            "Dropping expired message. mid: " + message.getId() + ", from: " + message.getMsgSrc()
                + " relayed from: " + message.getRelaySrcHost());
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.DISCARDED);
        return true;
      }

      // State Transition Cancellation
      if (message.getMsgType().equals(MessageType.STATE_TRANSITION_CANCELLATION.name())) {
        boolean success =
            cancelNotStartedStateTransition(message, stateTransitionHandlers, accessor,
                instanceName);
        if (success) {
          return true;
        }
      }

      if (MessageType.PARTICIPANT_STATUS_CHANGE.name().equals(message.getMsgType())) {
        LiveInstanceStatus toStatus = LiveInstanceStatus.valueOf(message.getToState());
        changeParticipantStatus(instanceName, toStatus, manager);
        reportAndRemoveMessage(message, accessor, instanceName, ProcessedMessageState.COMPLETED);
        return true;
      }

      _monitor.reportReceivedMessage(message);
    } catch (Exception e) {
      LOG.error("Failed to process the message {}. Deleting the message from ZK. Exception: {}",
          message, e);
      removeMessageFromTaskAndFutureMap(message);
      removeMessageFromZK(accessor, message, instanceName);
      return true;
    }
    return false;
  }

  /**
   * Preprocess the state transition message to validate if the request is valid.
   * If no operation needs to be triggered, discard the the message.
   * @param message
   * @param manager
   * @param stateTransitionHandlers
   * @param createHandler
   * @return True if the requested state transition is valid, and need to schedule the transition.
   *         False if no more operation is required.
   */
  private boolean validateAndProcessStateTransitionMessage(Message message, HelixManager manager,
      Map<String, MessageHandler> stateTransitionHandlers, MessageHandler createHandler) {
    String messageTarget = getMessageTarget(message.getResourceName(), message.getPartitionName());

    try {
      if (message.getMsgType().equals(MessageType.STATE_TRANSITION.name())
          && isStateTransitionInProgress(messageTarget)) {
        String taskId = _messageTaskMap.get(messageTarget);
        Message msg = _taskMap.get(taskId).getTask().getMessage();
        // If there is another state transition for same partition is going on,
        // discard the message. Controller will resend if this is a valid message
        String errMsg = String.format(
            "Another state transition for %s:%s is in progress with msg: %s, p2p: %s, read: %d, current:%d. Discarding %s->%s message",
            message.getResourceName(), message.getPartitionName(), msg.getMsgId(),
            msg.isRelayMessage(), msg.getReadTimeStamp(), System.currentTimeMillis(),
            message.getFromState(), message.getToState());
        updateUnprocessableMessage(message, null /* exception */, errMsg, manager);
        return false;
      }
      if (createHandler instanceof HelixStateTransitionHandler) {
        // We only check to state if there is no ST task scheduled/executing.
        HelixStateTransitionHandler.StaleMessageValidateResult result =
            ((HelixStateTransitionHandler) createHandler).staleMessageValidator();
        if (!result.isValid) {
          updateUnprocessableMessage(message, null /* exception */, result.exception.getMessage(),
              manager);
          return false;
        }
      }
      if (stateTransitionHandlers.containsKey(messageTarget)) {
        // If there are 2 messages in same batch about same partition's state transition,
        // the later one is discarded
        Message duplicatedMessage = stateTransitionHandlers.get(messageTarget)._message;
        String errMsg = String.format(
            "Duplicated state transition message: %s. Existing: %s->%s; New (Discarded): %s->%s",
            message.getMsgId(), duplicatedMessage.getFromState(), duplicatedMessage.getToState(),
            message.getFromState(), message.getToState());
        updateUnprocessableMessage(message, null /* exception */, errMsg, manager);
        return false;
      }
      return true;
    } catch (Exception ex) {
      updateUnprocessableMessage(message, ex, "State transition validation failed with Exception.",
          manager);
      return false;
    }
  }

  /**
   * Schedule task to execute the message handler.
   * @param instanceName
   * @param accessor
   * @param handler
   * @param context
   * @return True if schedule the task successfully. False otherwise.
   */
  private boolean scheduleTaskForMessage(String instanceName, HelixDataAccessor accessor,
      MessageHandler handler, NotificationContext context) {
    Message msg = handler._message;
    if (!scheduleTask(new HelixTask(msg, context, handler, this))) {
      // Remove message if schedule tasks are failed.
      removeMessageFromTaskAndFutureMap(msg);
      removeMessageFromZK(accessor, msg, instanceName);
      return false;
    }
    return true;
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
  private boolean cancelNotStartedStateTransition(Message message,
      Map<String, MessageHandler> stateTransitionHandlers, HelixDataAccessor accessor,
      String instanceName) {
    String targetMessageName =
        getMessageTarget(message.getResourceName(), message.getPartitionName());
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

  private Message markReadMessage(Message message, NotificationContext context,
      HelixManager manager) {
    message.setMsgState(MessageState.READ);
    message.setReadTimeStamp(new Date().getTime());
    message.setExecuteSessionId(context.getManager().getSessionId());

    _statusUpdateUtil.logInfo(message, HelixStateMachineEngine.class, "New Message", manager);
    return message;
  }

  private void updateUnprocessableMessage(Message message, Exception exception, String errorMsg,
      HelixManager manager) {
    String error = "Message " + message.getMsgId() + " cannot be processed: " + message.getRecord();
    if (exception != null) {
      LOG.error(error, exception);
      _statusUpdateUtil.logError(message, HelixStateMachineEngine.class, exception, error, manager);
    } else {
      LOG.error(error + errorMsg);
      _statusUpdateUtil.logError(message, HelixStateMachineEngine.class, errorMsg, manager);
    }
    message.setMsgState(MessageState.UNPROCESSABLE);
    _monitor.reportProcessedMessage(message, ProcessedMessageState.FAILED);
  }

  public MessageHandler createMessageHandler(Message message, NotificationContext changeContext) {
    String msgType = message.getMsgType();

    MsgHandlerFactoryRegistryItem item = _hdlrFtyRegistry.get(msgType);

    // Fail to find a MessageHandlerFactory for the message
    // we will keep the message and the message will be handled when
    // the corresponding MessageHandlerFactory is registered
    if (item == null) {
      LOG.warn("Fail to find message handler factory for type: " + msgType + " msgId: " + message
          .getMsgId());
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

  private void changeParticipantStatus(String instanceName,
      LiveInstance.LiveInstanceStatus toStatus, HelixManager manager) {
    if (toStatus == null) {
      LOG.warn("To status is null! Skip participant status change.");
      return;
    }

    LOG.info("Changing participant {} status to {} from {}", instanceName, toStatus,
        _liveInstanceStatus);
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    String sessionId = manager.getSessionId();
    String path = accessor.keyBuilder().liveInstance(instanceName).getPath();
    boolean success = false;

    switch (toStatus) {
      case FROZEN:
        _freezeSessionId = sessionId;
        _liveInstanceStatus = toStatus;
        // Entering freeze mode, update live instance status.
        // If the update fails, another new freeze message will be sent by controller.
        success = accessor.getBaseDataAccessor().update(path, record -> {
          record.setEnumField(LiveInstance.LiveInstanceProperty.STATUS.name(), toStatus);
          return record;
        }, AccessOption.EPHEMERAL);
        break;
      case NORMAL:
        // Exiting freeze mode
        // session changed, should call state model sync
        if (_freezeSessionId != null && !_freezeSessionId.equals(sessionId)) {
          syncFactoryState();
          ParticipantManager.carryOverPreviousCurrentState(accessor, instanceName, sessionId,
              manager.getStateMachineEngine(), false);
        }
        _freezeSessionId = null;
        _liveInstanceStatus = toStatus;
        success = accessor.getBaseDataAccessor().update(path, record -> {
          // Remove the status field for backwards compatibility
          record.getSimpleFields().remove(LiveInstance.LiveInstanceProperty.STATUS.name());
          return record;
        }, AccessOption.EPHEMERAL);
        break;
      default:
        LOG.warn("To status {} is not supported", toStatus);
        break;
    }

    LOG.info("Changed participant {} status to {}. FreezeSessionId={}, update success={}",
        instanceName, _liveInstanceStatus, _freezeSessionId, success);
  }

  private String getStateTransitionType(String prefix, String fromState, String toState) {
    if (prefix == null || fromState == null || toState == null) {
      return null;
    }
    return String.format("%s.%s.%s", prefix, fromState, toState);
  }

  private String getPerResourceStateTransitionPoolName(String resourceName) {
    return MessageType.STATE_TRANSITION.name() + "." + resourceName;
  }

  public LiveInstanceStatus getLiveInstanceStatus() {
    return _liveInstanceStatus;
  }

  private void removeMessageFromZK(HelixDataAccessor accessor, Message message,
      String instanceName) {
    if (HelixUtil.removeMessageFromZK(accessor, message, instanceName)) {
      LOG.info("Successfully removed message {} from ZK.", message.getMsgId());
    } else {
      LOG.warn("Failed to remove message {} from ZK.", message.getMsgId());
    }
  }

  private void sendNopMessage(HelixDataAccessor accessor, String instanceName) {
    try {
      Message nopMsg = new Message(MessageType.NO_OP, UUID.randomUUID().toString());
      nopMsg.setSrcName(instanceName);
      nopMsg.setTgtName(instanceName);
      accessor
          .setProperty(accessor.keyBuilder().message(nopMsg.getTgtName(), nopMsg.getId()), nopMsg);
      LOG.info("Send NO_OP message to {}, msgId: {}.", nopMsg.getTgtName(), nopMsg.getId());
    } catch (Exception e) {
      LOG.error("Failed to send NO_OP message to {}.", instanceName, e);
    }
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down HelixTaskExecutor");
    _isShuttingDown = true;
    _timer.cancel();

    shutdownExecutors();
    reset();
    _monitor.shutDown();
    LOG.info("Shutdown HelixTaskExecutor finished");
  }
}
