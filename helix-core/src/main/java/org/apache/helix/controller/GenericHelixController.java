package org.apache.helix.controller;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.api.listeners.*;
import org.apache.helix.common.ClusterEventBlockingQueue;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.PipelineRegistry;
import org.apache.helix.controller.stages.*;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.resource.ResourceMessageDispatchStage;
import org.apache.helix.controller.stages.resource.ResourceMessageGenerationPhase;
import org.apache.helix.controller.stages.task.TaskMessageDispatchStage;
import org.apache.helix.controller.stages.task.TaskMessageGenerationPhase;
import org.apache.helix.controller.stages.task.TaskPersistDataStage;
import org.apache.helix.controller.stages.task.TaskSchedulingStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.mbeans.ClusterEventMonitor;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.task.TaskDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.HelixConstants.*;

/**
 * Cluster Controllers main goal is to keep the cluster state as close as possible to Ideal State.
 * It does this by listening to changes in cluster state and scheduling new tasks to get cluster
 * state to best possible ideal state. Every instance of this class can control can control only one
 * cluster Get all the partitions use IdealState, CurrentState and Messages <br>
 * foreach partition <br>
 * 1. get the (instance,state) from IdealState, CurrentState and PendingMessages <br>
 * 2. compute best possible state (instance,state) pair. This needs previous step data and state
 * model constraints <br>
 * 3. compute the messages/tasks needed to move to 1 to 2 <br>
 * 4. select the messages that can be sent, needs messages and state model constraints <br>
 * 5. send messages
 */
public class GenericHelixController implements IdealStateChangeListener,
    LiveInstanceChangeListener, MessageListener, CurrentStateChangeListener,
    ControllerChangeListener, InstanceConfigChangeListener, ResourceConfigChangeListener,
    ClusterConfigChangeListener {
  private static final Logger logger =
      LoggerFactory.getLogger(GenericHelixController.class.getName());

  private static final long EVENT_THREAD_JOIN_TIMEOUT = 1000;
  private static final int ASYNC_TASKS_THREADPOOL_SIZE = 10;
  private final PipelineRegistry _registry;
  private final PipelineRegistry _taskRegistry;

  final AtomicReference<Map<String, LiveInstance>> _lastSeenInstances;
  final AtomicReference<Map<String, LiveInstance>> _lastSeenSessions;

  // By default not reporting status until controller status is changed to activate
  // TODO This flag should be inside ClusterStatusMonitor. When false, no MBean registering.
  private boolean _isMonitoring = false;
  private final ClusterStatusMonitor _clusterStatusMonitor;

  /**
   * A queue for controller events and a thread that will consume it
   */
  private final ClusterEventBlockingQueue _eventQueue;
  private final ClusterEventProcessor _eventThread;

  private final ClusterEventBlockingQueue _taskEventQueue;
  private final ClusterEventProcessor _taskEventThread;

  private final Map<AsyncWorkerType, DedupEventProcessor<String, Runnable>> _asyncFIFOWorkerPool;

  private long _continousRebalanceFailureCount = 0;

  /**
   * The _paused flag is checked by function handleEvent(), while if the flag is set handleEvent()
   * will be no-op. Other event handling logic keeps the same when the flag is set.
   */
  private boolean _paused;
  private boolean _inMaintenanceMode;

  /**
   * The timer that can periodically run the rebalancing pipeline. The timer will start if there is
   * one resource group has the config to use the timer.
   */
  Timer _periodicalRebalanceTimer = null;

  /**
   * The timer to schedule ad hoc forced rebalance or retry rebalance event.
   */
  Timer _forceRebalanceTimer = null;

  long _timerPeriod = Long.MAX_VALUE;

  /**
   * A cache maintained across pipelines
   */
  private ClusterDataCache _cache;
  private ClusterDataCache _taskCache;
  private ScheduledExecutorService _asyncTasksThreadPool;

  /**
   * A record of last pipeline finish duration
   */
  private long _lastPipelineEndTimestamp;

  private String _clusterName;

  enum PipelineTypes {
    DEFAULT,
    TASK
  }

  /**
   * Default constructor that creates a default pipeline registry. This is sufficient in most cases,
   * but if there is a some thing specific needed use another constructor where in you can pass a
   * pipeline registry
   */
  public GenericHelixController() {
    this(createDefaultRegistry(PipelineTypes.DEFAULT.name()),
        createTaskRegistry(PipelineTypes.TASK.name()));
  }

  public GenericHelixController(String clusterName) {
    this(createDefaultRegistry(PipelineTypes.DEFAULT.name()),
        createTaskRegistry(PipelineTypes.TASK.name()), clusterName);
  }

  class RebalanceTask extends TimerTask {
    HelixManager _manager;
    ClusterEventType _clusterEventType;

    public RebalanceTask(HelixManager manager, ClusterEventType clusterEventType) {
      _manager = manager;
      _clusterEventType = clusterEventType;
    }

    @Override
    public void run() {
      try {
        if (_clusterEventType.equals(ClusterEventType.PeriodicalRebalance)) {
          _cache.requireFullRefresh();
          _taskCache.requireFullRefresh();
          _cache.refresh(_manager.getHelixDataAccessor());
          if (_cache.getLiveInstances() != null) {
            NotificationContext changeContext = new NotificationContext(_manager);
            changeContext.setType(NotificationContext.Type.CALLBACK);
            synchronized (_manager) {
              checkLiveInstancesObservation(new ArrayList<>(_cache.getLiveInstances().values()),
                  changeContext);
            }
          }
        }

        forceRebalance(_manager, _clusterEventType);
      } catch (Throwable ex) {
        logger.error("Time task failed. Rebalance task type: " + _clusterEventType + ", cluster: "
            + _clusterName, ex);
      }
    }
  }

  /* Trigger a rebalance pipeline */
  private void forceRebalance(HelixManager manager, ClusterEventType eventType) {
    NotificationContext changeContext = new NotificationContext(manager);
    changeContext.setType(NotificationContext.Type.CALLBACK);
    String uid = UUID.randomUUID().toString().substring(0, 8);
    ClusterEvent event = new ClusterEvent(_clusterName, eventType, uid);
    event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
    event.addAttribute(AttributeName.changeContext.name(), changeContext);
    event.addAttribute(AttributeName.eventData.name(), new ArrayList<>());
    event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), _asyncFIFOWorkerPool);

    _taskEventQueue.put(event);
    _eventQueue.put(event.clone(uid));

    logger.info(String
        .format("Controller rebalance event triggered with event type: %s for cluster %s",
            eventType, _clusterName));
  }

  // TODO who should stop this timer
  /**
   * Starts the rebalancing timer with the specified period. Start the timer if necessary; If the
   * period is smaller than the current period, cancel the current timer and use the new period.
   */
  void startRebalancingTimer(long period, HelixManager manager) {
    if (period != _timerPeriod) {
      logger.info("Controller starting timer at period " + period);
      if (_periodicalRebalanceTimer != null) {
        _periodicalRebalanceTimer.cancel();
      }
      _periodicalRebalanceTimer = new Timer(true);
      _timerPeriod = period;
      _periodicalRebalanceTimer
          .scheduleAtFixedRate(new RebalanceTask(manager, ClusterEventType.PeriodicalRebalance),
              _timerPeriod, _timerPeriod);
    } else {
      logger.info("Controller already has timer at period " + _timerPeriod);
    }
  }

  /**
   * Stops the rebalancing timer
   */
  void stopRebalancingTimers() {
    if (_periodicalRebalanceTimer != null) {
      _periodicalRebalanceTimer.cancel();
      _periodicalRebalanceTimer = null;
    }
    _timerPeriod = Integer.MAX_VALUE;
  }

  private static PipelineRegistry createDefaultRegistry(String pipelineName) {
    logger.info("createDefaultRegistry");
    synchronized (GenericHelixController.class) {
      PipelineRegistry registry = new PipelineRegistry();

      // cluster data cache refresh
      Pipeline dataRefresh = new Pipeline(pipelineName);
      dataRefresh.addStage(new ReadClusterDataStage());

      // data pre-process pipeline
      Pipeline dataPreprocess = new Pipeline(pipelineName);
      dataPreprocess.addStage(new ResourceComputationStage());
      dataPreprocess.addStage(new ResourceValidationStage());
      dataPreprocess.addStage(new CurrentStateComputationStage());
      dataPreprocess.addStage(new TopStateHandoffReportStage());

      // rebalance pipeline
      Pipeline rebalancePipeline = new Pipeline(pipelineName);
      rebalancePipeline.addStage(new BestPossibleStateCalcStage());
      rebalancePipeline.addStage(new IntermediateStateCalcStage());
      rebalancePipeline.addStage(new ResourceMessageGenerationPhase());
      rebalancePipeline.addStage(new MessageSelectionStage());
      rebalancePipeline.addStage(new MessageThrottleStage());
      rebalancePipeline.addStage(new ResourceMessageDispatchStage());
      rebalancePipeline.addStage(new PersistAssignmentStage());
      rebalancePipeline.addStage(new TargetExteralViewCalcStage());

      // external view generation
      Pipeline externalViewPipeline = new Pipeline(pipelineName);
      externalViewPipeline.addStage(new ExternalViewComputeStage());

      // backward compatibility check
      Pipeline liveInstancePipeline = new Pipeline(pipelineName);
      liveInstancePipeline.addStage(new CompatibilityCheckStage());

      registry.register(ClusterEventType.IdealStateChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.CurrentStateChange, dataRefresh, dataPreprocess, externalViewPipeline, rebalancePipeline);
      registry.register(ClusterEventType.InstanceConfigChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.ResourceConfigChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.ClusterConfigChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.LiveInstanceChange, dataRefresh, liveInstancePipeline, dataPreprocess, externalViewPipeline, rebalancePipeline);
      registry.register(ClusterEventType.MessageChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.Resume, dataRefresh, dataPreprocess, externalViewPipeline, rebalancePipeline);
      registry.register(ClusterEventType.PeriodicalRebalance, dataRefresh, dataPreprocess, externalViewPipeline, rebalancePipeline);
      return registry;
    }
  }

  private static PipelineRegistry createTaskRegistry(String pipelineName) {
    logger.info("createDefaultRegistry");
    synchronized (GenericHelixController.class) {
      PipelineRegistry registry = new PipelineRegistry();

      // cluster data cache refresh
      Pipeline dataRefresh = new Pipeline(pipelineName);
      dataRefresh.addStage(new ReadClusterDataStage());

      // data pre-process pipeline
      Pipeline dataPreprocess = new Pipeline(pipelineName);
      dataPreprocess.addStage(new ResourceComputationStage());
      dataPreprocess.addStage(new ResourceValidationStage());
      dataPreprocess.addStage(new CurrentStateComputationStage());

      // rebalance pipeline
      Pipeline rebalancePipeline = new Pipeline(pipelineName);
      rebalancePipeline.addStage(new TaskSchedulingStage());
      rebalancePipeline.addStage(new TaskPersistDataStage());
      rebalancePipeline.addStage(new TaskGarbageCollectionStage());
      rebalancePipeline.addStage(new TaskMessageGenerationPhase());
      rebalancePipeline.addStage(new TaskMessageDispatchStage());

      // backward compatibility check
      Pipeline liveInstancePipeline = new Pipeline(pipelineName);
      liveInstancePipeline.addStage(new CompatibilityCheckStage());

      registry.register(ClusterEventType.IdealStateChange, dataRefresh, dataPreprocess,
          rebalancePipeline);
      registry.register(ClusterEventType.CurrentStateChange, dataRefresh, dataPreprocess,
          rebalancePipeline);
      registry.register(ClusterEventType.InstanceConfigChange, dataRefresh, dataPreprocess,
          rebalancePipeline);
      registry.register(ClusterEventType.ResourceConfigChange, dataRefresh, dataPreprocess,
          rebalancePipeline);
      registry.register(ClusterEventType.ClusterConfigChange, dataRefresh, dataPreprocess,
          rebalancePipeline);
      registry.register(ClusterEventType.LiveInstanceChange, dataRefresh, liveInstancePipeline,
          dataPreprocess, rebalancePipeline);
      registry
          .register(ClusterEventType.MessageChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.Resume, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.PeriodicalRebalance, dataRefresh, dataPreprocess,
          rebalancePipeline);
      return registry;
    }
  }

  public GenericHelixController(PipelineRegistry registry, PipelineRegistry taskRegistry) {
    this(registry, taskRegistry, null);
  }

  private GenericHelixController(PipelineRegistry registry, PipelineRegistry taskRegistry,
      final String clusterName) {
    _paused = false;
    _registry = registry;
    _taskRegistry = taskRegistry;
    _lastSeenInstances = new AtomicReference<>();
    _lastSeenSessions = new AtomicReference<>();
    _clusterName = clusterName;
    _asyncTasksThreadPool =
        Executors.newScheduledThreadPool(ASYNC_TASKS_THREADPOOL_SIZE, new ThreadFactory() {
          @Override public Thread newThread(Runnable r) {
            return new Thread(r, "HelixController-async_tasks-" + _clusterName);
          }
        });

    _eventQueue = new ClusterEventBlockingQueue();
    _taskEventQueue = new ClusterEventBlockingQueue();

    _asyncFIFOWorkerPool = new HashMap<>();

    _cache = new ClusterDataCache(clusterName);
    _taskCache = new ClusterDataCache(clusterName);

    _eventThread = new ClusterEventProcessor(_cache, _eventQueue, "default-" + clusterName);
    _taskEventThread =
        new ClusterEventProcessor(_taskCache, _taskEventQueue, "task-" + clusterName);

    _forceRebalanceTimer = new Timer();
    _lastPipelineEndTimestamp = TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;

    initializeAsyncFIFOWorkers();
    initPipelines(_eventThread, _cache, false);
    initPipelines(_taskEventThread, _taskCache, true);

    _clusterStatusMonitor = new ClusterStatusMonitor(_clusterName);
  }

  private void initializeAsyncFIFOWorkers() {
    for (AsyncWorkerType type : AsyncWorkerType.values()) {
      DedupEventProcessor<String, Runnable> worker =
          new DedupEventProcessor<String, Runnable>(_clusterName, type.name()) {
            @Override
            protected void handleEvent(Runnable event) {
              // TODO: retry when queue is empty and event.run() failed?
              event.run();
            }
          };
      worker.start();
      _asyncFIFOWorkerPool.put(type, worker);
      logger.info("Started async worker {}", worker.getName());
    }
  }

  private void shutdownAsyncFIFOWorkers() {
    for (DedupEventProcessor processor : _asyncFIFOWorkerPool.values()) {
      processor.shutdown();
      logger.info("Shutdown async worker {}", processor.getName());
    }
  }

  private boolean isEventQueueEmpty(boolean taskQueue) {
    if (taskQueue) {
      return _taskEventQueue.isEmpty();
    } else {
      return _eventQueue.isEmpty();
    }
  }

  /**
   * lock-always: caller always needs to obtain an external lock before call, calls to handleEvent()
   * should be serialized
   * @param event
   */
  protected void handleEvent(ClusterEvent event, ClusterDataCache cache) {
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      logger.error("No cluster manager in event:" + event.getEventType());
      return;
    }

    if (!manager.isLeader()) {
      logger.error("Cluster manager: " + manager.getInstanceName() + " is not leader for " + manager
          .getClusterName() + ". Pipeline will not be invoked");
      return;
    }

    // TODO If init controller with paused = true, it may not take effect immediately
    // _paused is default false. If any events come before controllerChangeEvent, the controller
    // will be excuting in un-paused mode. Which might not be the config in ZK.
    if (_paused) {
      logger.info("Cluster " + manager.getClusterName() + " is paused. Ignoring the event:" + event
          .getEventType());
      return;
    }

    NotificationContext context = null;
    if (event.getAttribute(AttributeName.changeContext.name()) != null) {
      context = event.getAttribute(AttributeName.changeContext.name());
    }

    if (context != null) {
      if (context.getType() == Type.FINALIZE) {
        stopRebalancingTimers();
        logger.info("Get FINALIZE notification, skip the pipeline. Event :" + event.getEventType());
        return;
      } else {
        // TODO: should be in the initialization of controller.
        if (_cache != null) {
          checkRebalancingTimer(manager, Collections.EMPTY_LIST, _cache.getClusterConfig());
        }
        if (_isMonitoring) {
          event.addAttribute(AttributeName.clusterStatusMonitor.name(), _clusterStatusMonitor);
        }
      }
    }

    // add the cache
    _cache.setEventId(event.getEventId());
    event.addAttribute(AttributeName.ClusterDataCache.name(), cache);
    event.addAttribute(AttributeName.LastRebalanceFinishTimeStamp.name(), _lastPipelineEndTimestamp);

    List<Pipeline> pipelines = cache.isTaskCache() ?
        _taskRegistry.getPipelinesForEvent(event.getEventType()) : _registry
        .getPipelinesForEvent(event.getEventType());

    if (pipelines == null || pipelines.size() == 0) {
      logger.info(String
          .format("No % pipeline to run for event: %s %s", getPipelineType(cache.isTaskCache()),
              event.getEventType(), event.getEventId()));
      return;
    }

    logger.info(String.format("START: Invoking %s controller pipeline for cluster %s event: %s  %s",
        manager.getClusterName(), getPipelineType(cache.isTaskCache()), event.getEventType(),
        event.getEventId()));
    long startTime = System.currentTimeMillis();
    boolean rebalanceFail = false;
    for (Pipeline pipeline : pipelines) {
      event.addAttribute(AttributeName.PipelineType.name(), pipeline.getPipelineType());
      try {
        pipeline.handle(event);
        pipeline.finish();
      } catch (Exception e) {
        logger.error(
            "Exception while executing " + getPipelineType(cache.isTaskCache()) + "pipeline: "
                + pipeline + "for cluster ." + _clusterName
                + ". Will not continue to next pipeline", e);

        if (e instanceof HelixMetaDataAccessException) {
          rebalanceFail = true;
          // If pipeline failed due to read/write fails to zookeeper, retry the pipeline.
          cache.requireFullRefresh();
          logger.warn("Rebalance pipeline failed due to read failure from zookeeper, cluster: " + _clusterName);

          // only push a retry event when there is no pending event in the corresponding event queue.
          if (isEventQueueEmpty(cache.isTaskCache())) {
            _continousRebalanceFailureCount ++;
            long delay = getRetryDelay(_continousRebalanceFailureCount);
            if (delay == 0) {
              forceRebalance(manager, ClusterEventType.RetryRebalance);
            } else {
              _asyncTasksThreadPool
                  .schedule(new RebalanceTask(manager, ClusterEventType.RetryRebalance), delay,
                      TimeUnit.MILLISECONDS);
            }
            logger.info("Retry rebalance pipeline with delay " + delay + "ms for cluster: " + _clusterName);
          }
        }
        _clusterStatusMonitor.reportRebalanceFailure();
        break;
      }
    }
    if (!rebalanceFail) {
      _continousRebalanceFailureCount = 0;
    }

    _lastPipelineEndTimestamp = System.currentTimeMillis();
    logger.info(String
        .format("END: Invoking %s controller pipeline for event: %s %s for cluster %s, took %d ms",
            getPipelineType(cache.isTaskCache()), event.getEventType(), event.getEventId(),
            manager.getClusterName(), (_lastPipelineEndTimestamp - startTime)));

    if (!cache.isTaskCache()) {
      // report event process durations
      NotificationContext notificationContext =
          event.getAttribute(AttributeName.changeContext.name());
      long enqueueTime = event.getCreationTime();
      long zkCallbackTime;
      StringBuilder sb = new StringBuilder();
      if (notificationContext != null) {
        zkCallbackTime = notificationContext.getCreationTime();
        if (_isMonitoring) {
          _clusterStatusMonitor
              .updateClusterEventDuration(ClusterEventMonitor.PhaseName.Callback.name(),
                  enqueueTime - zkCallbackTime);
        }
        sb.append(String.format(
            "Callback time for event: " + event.getEventType() + " took: " + (enqueueTime
                - zkCallbackTime) + " ms\n"));
      }
      if (_isMonitoring) {
        _clusterStatusMonitor
            .updateClusterEventDuration(ClusterEventMonitor.PhaseName.InQueue.name(),
                startTime - enqueueTime);
        _clusterStatusMonitor
            .updateClusterEventDuration(ClusterEventMonitor.PhaseName.TotalProcessed.name(),
                _lastPipelineEndTimestamp - startTime);
      }
      sb.append(String.format(
          "InQueue time for event: " + event.getEventType() + " took: " + (startTime - enqueueTime)
              + " ms\n"));
      sb.append(String.format(
          "TotalProcessed time for event: " + event.getEventType() + " took: " + (
              _lastPipelineEndTimestamp
              - startTime) + " ms"));
      logger.info(sb.toString());
    } else if (_isMonitoring) {
      // report workflow status
      TaskDriver driver = new TaskDriver(manager);
      _clusterStatusMonitor.refreshWorkflowsStatus(driver);
      _clusterStatusMonitor.refreshJobsStatus(driver);
    }

    // If event handling happens before controller deactivate, the process may write unnecessary
    // MBeans to monitoring after the monitor is disabled.
    // So reset ClusterStatusMonitor according to it's status after all event handling.
    // TODO remove this once clusterStatusMonitor blocks any MBean register on isMonitoring = false.
    resetClusterStatusMonitor();
  }

  /**
   * get the delay on next retry rebalance due to zk read failure, We use a simple exponential
   * backoff to make the delay between [10ms, 1000ms]
   */
  private long getRetryDelay(long failCount) {
    int lowLimit = 5;
    if (failCount <= lowLimit) {
      return 0;
    }
    long backoff = (long) (Math.pow(2, failCount - lowLimit) * 10);
    return Math.min(backoff, 1000);
  }

  @Override
  @PreFetch(enabled = false)
  public void onStateChange(String instanceName, List<CurrentState> statesInfo,
      NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onStateChange()");
    notifyCaches(changeContext, ChangeType.CURRENT_STATE);
    pushToEventQueues(ClusterEventType.CurrentStateChange, changeContext, Collections
        .<String, Object>singletonMap(AttributeName.instanceName.name(), instanceName));
    logger.info("END: GenericClusterController.onStateChange()");
  }

  @Override
  @PreFetch(enabled = false)
  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onMessage() for cluster " + _clusterName);
    notifyCaches(changeContext, ChangeType.MESSAGE);
    pushToEventQueues(ClusterEventType.MessageChange, changeContext,
        Collections.<String, Object>singletonMap(AttributeName.instanceName.name(), instanceName));

    if (_isMonitoring && messages != null) {
      _clusterStatusMonitor.addMessageQueueSize(instanceName, messages.size());
    }

    logger.info("END: GenericClusterController.onMessage() for cluster " + _clusterName);
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    logger.info("START: Generic GenericClusterController.onLiveInstanceChange() for cluster " + _clusterName);
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
      _taskCache.requireFullRefresh();
    }

    if (liveInstances == null) {
      liveInstances = Collections.emptyList();
    }
    _cache.setLiveInstances(liveInstances);
    _taskCache.setLiveInstances(liveInstances);

    // Go though the live instance list and make sure that we are observing them
    // accordingly. The action is done regardless of the paused flag.
    if (changeContext.getType() == NotificationContext.Type.INIT
        || changeContext.getType() == NotificationContext.Type.CALLBACK) {
      checkLiveInstancesObservation(liveInstances, changeContext);
    } else if (changeContext.getType() == NotificationContext.Type.FINALIZE) {
      // on finalize, should remove all message/current-state listeners
      logger.info("remove message/current-state listeners. lastSeenInstances: " + _lastSeenInstances
          + ", lastSeenSessions: " + _lastSeenSessions);
      liveInstances = Collections.emptyList();
      checkLiveInstancesObservation(liveInstances, changeContext);
    }

    pushToEventQueues(ClusterEventType.LiveInstanceChange, changeContext,
        Collections.<String, Object>singletonMap(AttributeName.eventData.name(), liveInstances));

    logger.info(
        "END: Generic GenericClusterController.onLiveInstanceChange() for cluster " + _clusterName);
  }

  private void checkRebalancingTimer(HelixManager manager, List<IdealState> idealStates,
      ClusterConfig clusterConfig) {
    if (manager.getConfigAccessor() == null) {
      logger.warn(manager.getInstanceName()
          + " config accessor doesn't exist. should be in file-based mode.");
      return;
    }

    long minPeriod = Long.MAX_VALUE;
    if (clusterConfig != null) {
      long period = clusterConfig.getRebalanceTimePeriod();
      if (period > 0 && minPeriod > period) {
        minPeriod = period;
      }
    }

    // TODO: resource level rebalance does not make sense, to remove it!
    for (IdealState idealState : idealStates) {
      long period = idealState.getRebalanceTimerPeriod();
      if (period > 0 && minPeriod > period) {
        minPeriod = period;
      }
    }

    if (minPeriod != Long.MAX_VALUE) {
      startRebalancingTimer(minPeriod, manager);
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onIdealStateChange(List<IdealState> idealStates, NotificationContext changeContext) {
    logger.info(
        "START: Generic GenericClusterController.onIdealStateChange() for cluster " + _clusterName);
    notifyCaches(changeContext, ChangeType.IDEAL_STATE);
    pushToEventQueues(ClusterEventType.IdealStateChange, changeContext,
        Collections.<String, Object>emptyMap());

    if (changeContext.getType() != Type.FINALIZE) {
      checkRebalancingTimer(changeContext.getManager(), idealStates, _cache.getClusterConfig());
    }

    logger.info("END: GenericClusterController.onIdealStateChange() for cluster " + _clusterName);
  }

  @Override
  @PreFetch(enabled = false)
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
      NotificationContext changeContext) {
    logger.info(
        "START: GenericClusterController.onInstanceConfigChange() for cluster " + _clusterName);
    notifyCaches(changeContext, ChangeType.INSTANCE_CONFIG);
    pushToEventQueues(ClusterEventType.InstanceConfigChange, changeContext,
        Collections.<String, Object>emptyMap());
    logger.info(
        "END: GenericClusterController.onInstanceConfigChange() for cluster " + _clusterName);
  }

  @Override
  @PreFetch(enabled = false)
  public void onResourceConfigChange(
      List<ResourceConfig> resourceConfigs, NotificationContext context) {
    logger.info(
        "START: GenericClusterController.onResourceConfigChange() for cluster " + _clusterName);
    notifyCaches(context, ChangeType.RESOURCE_CONFIG);
    pushToEventQueues(ClusterEventType.ResourceConfigChange, context,
        Collections.<String, Object>emptyMap());
    logger
        .info("END: GenericClusterController.onResourceConfigChange() for cluster " + _clusterName);
  }

  @Override
  @PreFetch(enabled = false)
  public void onClusterConfigChange(ClusterConfig clusterConfig,
      NotificationContext context) {
    logger.info(
        "START: GenericClusterController.onClusterConfigChange() for cluster " + _clusterName);
    notifyCaches(context, ChangeType.CLUSTER_CONFIG);
    pushToEventQueues(ClusterEventType.ClusterConfigChange, context,
        Collections.<String, Object>emptyMap());
    logger
        .info("END: GenericClusterController.onClusterConfigChange() for cluster " + _clusterName);
  }

  private void notifyCaches(NotificationContext context, ChangeType changeType) {
    if (context == null || context.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
      _taskCache.requireFullRefresh();
    } else {
      _cache.notifyDataChange(changeType, context.getPathChanged());
      _taskCache.notifyDataChange(changeType, context.getPathChanged());
    }
  }

  private void pushToEventQueues(ClusterEventType eventType, NotificationContext changeContext,
      Map<String, Object> eventAttributes) {
    // No need for completed UUID, prefixed should be fine
    String uid = UUID.randomUUID().toString().substring(0, 8);
    ClusterEvent event = new ClusterEvent(_clusterName, eventType,
        String.format("%s_%s", uid, PipelineTypes.DEFAULT.name()));
    event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
    event.addAttribute(AttributeName.changeContext.name(), changeContext);
    event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), _asyncFIFOWorkerPool);
    for (Map.Entry<String, Object> attr : eventAttributes.entrySet()) {
      event.addAttribute(attr.getKey(), attr.getValue());
    }
    _eventQueue.put(event);
    _taskEventQueue.put(event.clone(String.format("%s_%s", uid, PipelineTypes.TASK.name())));
  }

  @Override
  public void onControllerChange(NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onControllerChange() for cluster " + _clusterName);

    _cache.requireFullRefresh();
    _taskCache.requireFullRefresh();

    boolean controllerIsLeader;

    if (changeContext != null && changeContext.getType() == Type.FINALIZE) {
      logger.info(
          "GenericClusterController.onControllerChange() FINALIZE for cluster " + _clusterName);
      controllerIsLeader = false;
    } else {
      // double check if this controller is the leader
      controllerIsLeader = changeContext.getManager().isLeader();
    }

    HelixManager manager = changeContext.getManager();
    if (controllerIsLeader) {
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();
      PauseSignal pauseSignal = accessor.getProperty(keyBuilder.pause());
      MaintenanceSignal maintenanceSignal = accessor.getProperty(keyBuilder.maintenance());
      _paused = updateControllerState(changeContext, pauseSignal, _paused);
      _inMaintenanceMode =
          updateControllerState(changeContext, maintenanceSignal, _inMaintenanceMode);
      enableClusterStatusMonitor(true);
      _clusterStatusMonitor.setEnabled(!_paused);
      _clusterStatusMonitor.setPaused(_paused);
      _clusterStatusMonitor.setMaintenance(_inMaintenanceMode);
    } else {
      enableClusterStatusMonitor(false);
    }

    logger.info("END: GenericClusterController.onControllerChange() for cluster " + _clusterName);
  }

  /**
   * Go through the list of liveinstances in the cluster, and add currentstateChange listener and
   * Message listeners to them if they are newly added. For current state change, the observation is
   * tied to the session id of each live instance.
   */
  protected void checkLiveInstancesObservation(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {

    // construct maps for current live-instances
    Map<String, LiveInstance> curInstances = new HashMap<>();
    Map<String, LiveInstance> curSessions = new HashMap<>();
    for (LiveInstance liveInstance : liveInstances) {
      curInstances.put(liveInstance.getInstanceName(), liveInstance);
      curSessions.put(liveInstance.getSessionId(), liveInstance);
    }

    // TODO: remove the synchronization here once we move this update into dataCache.
    synchronized (_lastSeenInstances) {
      Map<String, LiveInstance> lastInstances = _lastSeenInstances.get();
      Map<String, LiveInstance> lastSessions = _lastSeenSessions.get();

      HelixManager manager = changeContext.getManager();
      Builder keyBuilder = new Builder(manager.getClusterName());
      if (lastSessions != null) {
        for (String session : lastSessions.keySet()) {
          if (!curSessions.containsKey(session)) {
            // remove current-state listener for expired session
            String instanceName = lastSessions.get(session).getInstanceName();
            manager.removeListener(keyBuilder.currentStates(instanceName, session), this);
          }
        }
      }

      if (lastInstances != null) {
        for (String instance : lastInstances.keySet()) {
          if (!curInstances.containsKey(instance)) {
            // remove message listener for disconnected instances
            manager.removeListener(keyBuilder.messages(instance), this);
          }
        }
      }

      for (String session : curSessions.keySet()) {
        if (lastSessions == null || !lastSessions.containsKey(session)) {
          String instanceName = curSessions.get(session).getInstanceName();
          try {
            // add current-state listeners for new sessions
            manager.addCurrentStateChangeListener(this, instanceName, session);
            logger.info(manager.getInstanceName() + " added current-state listener for instance: "
                + instanceName + ", session: " + session + ", listener: " + this);
          } catch (Exception e) {
            logger.error("Fail to add current state listener for instance: " + instanceName
                + " with session: " + session, e);
          }
        }
      }

      for (String instance : curInstances.keySet()) {
        if (lastInstances == null || !lastInstances.containsKey(instance)) {
          try {
            // add message listeners for new instances
            manager.addMessageListener(this, instance);
            logger.info(manager.getInstanceName() + " added message listener for " + instance
                + ", listener: " + this);
          } catch (Exception e) {
            logger.error("Fail to add message listener for instance: " + instance, e);
          }
        }
      }

      // update last-seen
      _lastSeenInstances.set(curInstances);
      _lastSeenSessions.set(curSessions);
    }
  }

  public void shutdown() throws InterruptedException {
    stopRebalancingTimers();

    terminateEventThread(_eventThread);
    terminateEventThread(_taskEventThread);

    _eventQueue.clear();
    _taskEventQueue.clear();

    // shutdown asycTasksThreadpool and wait for terminate.
    _asyncTasksThreadPool.shutdownNow();
    try {
      _asyncTasksThreadPool.awaitTermination(EVENT_THREAD_JOIN_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      logger.warn("Timeout when terminating async tasks. Some async tasks are still executing.");
    }

    // shutdown async workers
    shutdownAsyncFIFOWorkers();

    enableClusterStatusMonitor(false);

    // TODO controller shouldn't be used in anyway after shutdown.
    // Need to record shutdown and throw Exception if the controller is used again.
  }

  private void enableClusterStatusMonitor(boolean enable) {
    synchronized (_clusterStatusMonitor) {
      if (_isMonitoring != enable) {
        // monitoring state changed
        if (enable) {
          logger.info("Enable clusterStatusMonitor for cluster " + _clusterName);
          // Clear old cached monitoring related data to avoid reporting stats cross different
          // leadership periods
          _cache.clearMonitoringRecords();
          _taskCache.clearMonitoringRecords();
          _clusterStatusMonitor.active();
        } else {
          logger.info("Disable clusterStatusMonitor for cluster " + _clusterName);
          // Reset will be done if (_isMonitoring = false) later, no matter if the state is changed or not.
        }
        _isMonitoring = enable;
      }
      // Due to multithreads processing, async thread may write to monitor even it is closed.
      // So when it is disabled, always try to clear the monitor.
      resetClusterStatusMonitor();
    }
  }

  private void resetClusterStatusMonitor() {
    synchronized (_clusterStatusMonitor) {
      if (!_isMonitoring) {
        _clusterStatusMonitor.reset();
      }
    }
  }

  private void terminateEventThread(Thread thread) throws InterruptedException {
    while (thread.isAlive()) {
      thread.interrupt();
      thread.join(EVENT_THREAD_JOIN_TIMEOUT);
    }
  }

  private boolean updateControllerState(NotificationContext changeContext, PauseSignal signal,
      boolean statusFlag) {
    if (signal != null) {
      // This logic is used for recording first time entering PAUSE/MAINTENCE mode
      if (!statusFlag) {
        statusFlag = true;
        logger.info(String.format("controller is now %s",
            (signal instanceof MaintenanceSignal) ? "in maintenance mode" : "paused"));
      }
    } else {
      if (statusFlag) {
        statusFlag = false;
        logger.info("controller is now resumed from paused state");
        String uid = UUID.randomUUID().toString().substring(0, 8);
        ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.Resume,
            String.format("%s_%s", uid, PipelineTypes.DEFAULT.name()));
        event.addAttribute(AttributeName.changeContext.name(), changeContext);
        event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
        event.addAttribute(AttributeName.eventData.name(), signal);
        event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), _asyncFIFOWorkerPool);
        _eventQueue.put(event);
        _taskEventQueue.put(event.clone(String.format("%s_%s", uid, PipelineTypes.TASK.name())));
      }
    }
    return statusFlag;
  }


  // TODO: refactor this to use common/ClusterEventProcessor.
  private class ClusterEventProcessor extends Thread {
    private final ClusterDataCache _cache;
    private final ClusterEventBlockingQueue _eventBlockingQueue;
    private final String _processorName;

    public ClusterEventProcessor(ClusterDataCache cache,
        ClusterEventBlockingQueue eventBlockingQueue, String processorName) {
      super("HelixController-pipeline-" + processorName);
      _cache = cache;
      _eventBlockingQueue = eventBlockingQueue;
      _processorName = processorName;
    }

    @Override
    public void run() {
      logger.info(
          "START ClusterEventProcessor thread  for cluster " + _clusterName + ", processor name: "
              + _processorName);
      while (!isInterrupted()) {
        try {
          handleEvent(_eventBlockingQueue.take(), _cache);
        } catch (InterruptedException e) {
          logger.warn("ClusterEventProcessor interrupted " + _processorName, e);
          interrupt();
        } catch (ZkInterruptedException e) {
          logger
              .warn("ClusterEventProcessor caught a ZK connection interrupt " + _processorName, e);
          interrupt();
        } catch (ThreadDeath death) {
          logger.error("ClusterEventProcessor caught a ThreadDeath  " + _processorName, death);
          throw death;
        } catch (Throwable t) {
          logger.error("ClusterEventProcessor failed while running the controller pipeline "
              + _processorName, t);
        }
      }
      logger.info("END ClusterEventProcessor thread " + _processorName);
    }
  }

  private void initPipelines(Thread eventThread, ClusterDataCache cache, boolean isTask) {
    cache.setTaskCache(isTask);
    cache.setAsyncTasksThreadPool(_asyncTasksThreadPool);

    eventThread.setDaemon(true);
    eventThread.start();
  }

  public static String getPipelineType(boolean isTask) {
    return isTask ? PipelineTypes.TASK.name() : PipelineTypes.DEFAULT.name();
  }
}
