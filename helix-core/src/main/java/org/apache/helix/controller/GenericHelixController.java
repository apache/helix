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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.common.ClusterEventBlockingQueue;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.PipelineRegistry;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CompatibilityCheckStage;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ExternalViewComputeStage;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.MaintenanceRecoveryStage;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.controller.stages.MessageThrottleStage;
import org.apache.helix.controller.stages.PersistAssignmentStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.controller.stages.ResourceValidationStage;
import org.apache.helix.controller.stages.TargetExteralViewCalcStage;
import org.apache.helix.controller.stages.TaskGarbageCollectionStage;
import org.apache.helix.controller.stages.TopStateHandoffReportStage;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.HelixConstants.ChangeType;

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
  long _timerPeriod = Long.MAX_VALUE;


  /**
   * The timer that triggers the on-demand rebalance pipeline.
   */
  Timer _onDemandRebalanceTimer = null;
  AtomicReference<RebalanceTask> _nextRebalanceTask = new AtomicReference<>();


  /**
   * A cache maintained across pipelines
   */
  private final ResourceControllerDataProvider _resourceControlDataProvider;
  private final WorkflowControllerDataProvider _workflowControlDataProvider;
  private final ScheduledExecutorService _asyncTasksThreadPool;

  /**
   * A record of last pipeline finish duration
   */
  private long _lastPipelineEndTimestamp;

  private String _clusterName;
  private final Set<Pipeline.Type> _enabledPipelineTypes;

  private HelixManager _helixManager;

  /**
   * TODO: We should get rid of this once we move to:
   *  1) ZK callback should go to ClusterDataCache and trigger data cache refresh only
   *  2) then ClusterDataCache.refresh triggers rebalance pipeline.
   */
  /* Map of cluster->GenrichelixController */
  private static Map<String, GenericHelixController> HelixControllerFactory = new ConcurrentHashMap<>();
  public static GenericHelixController getController(String clusterName) {
    return HelixControllerFactory.get(clusterName);
  }

  /**
   * Default constructor that creates a default pipeline registry. This is sufficient in most cases,
   * but if there is a some thing specific needed use another constructor where in you can pass a
   * pipeline registry
   */
  public GenericHelixController() {
    this(createDefaultRegistry(Pipeline.Type.DEFAULT.name()),
        createTaskRegistry(Pipeline.Type.TASK.name()));
  }

  public GenericHelixController(String clusterName) {
    this(createDefaultRegistry(Pipeline.Type.DEFAULT.name()),
        createTaskRegistry(Pipeline.Type.TASK.name()), clusterName,
        Sets.newHashSet(Pipeline.Type.TASK, Pipeline.Type.DEFAULT));
  }

  public GenericHelixController(String clusterName, Set<Pipeline.Type> enabledPipelins) {
    this(createDefaultRegistry(Pipeline.Type.DEFAULT.name()),
        createTaskRegistry(Pipeline.Type.TASK.name()), clusterName, enabledPipelins);
  }

  class RebalanceTask extends TimerTask {
    final HelixManager _manager;
    final ClusterEventType _clusterEventType;
    private final Optional<Boolean> _shouldRefreshCacheOption;
    private long _nextRebalanceTime;

    public RebalanceTask(HelixManager manager, ClusterEventType clusterEventType) {
      this(manager, clusterEventType, -1);
    }

    public RebalanceTask(HelixManager manager, ClusterEventType clusterEventType,
        long nextRebalanceTime) {
      this(manager, clusterEventType, nextRebalanceTime, Optional.empty());
    }

    public RebalanceTask(HelixManager manager, ClusterEventType clusterEventType,
        long nextRebalanceTime, boolean shouldRefreshCache) {
      this(manager, clusterEventType, nextRebalanceTime, Optional.of(shouldRefreshCache));
    }

    private RebalanceTask(HelixManager manager, ClusterEventType clusterEventType,
        long nextRebalanceTime, Optional<Boolean> shouldRefreshCacheOption) {
      _manager = manager;
      _clusterEventType = clusterEventType;
      _nextRebalanceTime = nextRebalanceTime;
      _shouldRefreshCacheOption = shouldRefreshCacheOption;
    }

    public long getNextRebalanceTime() {
      return _nextRebalanceTime;
    }

    @Override
    public void run() {
      try {
        if (_shouldRefreshCacheOption.orElse(
            _clusterEventType.equals(ClusterEventType.PeriodicalRebalance) || _clusterEventType
                .equals(ClusterEventType.OnDemandRebalance))) {
          requestDataProvidersFullRefresh();

          HelixDataAccessor accessor = _manager.getHelixDataAccessor();
          PropertyKey.Builder keyBuilder = accessor.keyBuilder();
          Map<String, LiveInstance> liveInstanceMap =
              accessor.getChildValuesMap(keyBuilder.liveInstances());

          if (liveInstanceMap != null && !liveInstanceMap.isEmpty()) {
            NotificationContext changeContext = new NotificationContext(_manager);
            changeContext.setType(NotificationContext.Type.CALLBACK);
            synchronized (_manager) {
              checkLiveInstancesObservation(new ArrayList<>(liveInstanceMap.values()),
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

    enqueueEvent(_taskEventQueue, event);
    enqueueEvent(_eventQueue, event.clone(uid));
    logger.info(String
        .format("Controller rebalance pipeline triggered with event type: %s for cluster %s",
            eventType, _clusterName));
  }

  /**
   * Starts the rebalancing timer with the specified period. Start the timer if necessary; If the
   * period is smaller than the current period, cancel the current timer and use the new period.
   */
  void startPeriodRebalance(long period, HelixManager manager) {
    if (period != _timerPeriod) {
      logger.info("Controller starting periodical rebalance timer at period " + period);
      if (_periodicalRebalanceTimer != null) {
        _periodicalRebalanceTimer.cancel();
      }
      _periodicalRebalanceTimer = new Timer(true);
      _timerPeriod = period;
      _periodicalRebalanceTimer
          .scheduleAtFixedRate(new RebalanceTask(manager, ClusterEventType.PeriodicalRebalance),
              _timerPeriod, _timerPeriod);
    } else {
      logger.info("Controller already has periodical rebalance timer at period " + _timerPeriod);
    }
  }

  /**
   * Stops the rebalancing timer.
   */
  void stopPeriodRebalance() {
    logger.info("Controller stopping periodical rebalance timer at period " + _timerPeriod);
    if (_periodicalRebalanceTimer != null) {
      _periodicalRebalanceTimer.cancel();
      _periodicalRebalanceTimer = null;
      _timerPeriod = Long.MAX_VALUE;
      logger.info("Controller stopped periodical rebalance timer at period " + _timerPeriod);
    }
  }

  /**
   * This function is deprecated. Please use RebalanceUtil.scheduleInstantPipeline method instead.
   * schedule a future rebalance pipeline run, delayed at given time.
   */
  @Deprecated
  public void scheduleRebalance(long rebalanceTime) {
    if (_helixManager == null) {
      logger.warn(
          "Failed to schedule a future pipeline run for cluster " + _clusterName + " helix manager is null!");
      return;
    }

    long current = System.currentTimeMillis();
    long delay = rebalanceTime - current;

    if (rebalanceTime > current) {
      RebalanceTask preTask = _nextRebalanceTask.get();
      if (preTask != null && preTask.getNextRebalanceTime() > current
          && preTask.getNextRebalanceTime() < rebalanceTime) {
        // already have a earlier rebalance scheduled, no need to schedule again.
        return;
      }

      RebalanceTask newTask =
          new RebalanceTask(_helixManager, ClusterEventType.OnDemandRebalance, rebalanceTime);

      _onDemandRebalanceTimer.schedule(newTask, delay);
      logger.info(
          "Scheduled a future pipeline run for cluster " + _helixManager.getClusterName() + " in delay "
              + delay);

      preTask = _nextRebalanceTask.getAndSet(newTask);
      if (preTask != null) {
        preTask.cancel();
      }
    }
  }

  /**
   * Schedule an on demand rebalance pipeline.
   * @param delay
   */
  @Deprecated
  public void scheduleOnDemandRebalance(long delay) {
    scheduleOnDemandRebalance(delay, true);
  }

  /**
   * Schedule an on demand rebalance pipeline.
   * @param delay
   * @param shouldRefreshCache true if refresh the cache before scheduling a rebalance.
   */
  public void scheduleOnDemandRebalance(long delay, boolean shouldRefreshCache) {
    if (_helixManager == null) {
      logger.error("Failed to schedule a future pipeline run for cluster {}. Helix manager is null!",
          _clusterName);
      return;
    }
    long currentTime = System.currentTimeMillis();
    long rebalanceTime = currentTime + delay;
    if (delay > 0) {
      RebalanceTask preTask = _nextRebalanceTask.get();
      if (preTask != null && preTask.getNextRebalanceTime() > currentTime
          && preTask.getNextRebalanceTime() < rebalanceTime) {
        // already have a earlier rebalance scheduled, no need to schedule again.
        return;
      }
    }

    RebalanceTask newTask =
        new RebalanceTask(_helixManager, ClusterEventType.OnDemandRebalance, rebalanceTime,
            shouldRefreshCache);

    _onDemandRebalanceTimer.schedule(newTask, delay);
    logger.info("Scheduled instant pipeline run for cluster {}." , _helixManager.getClusterName());

    RebalanceTask preTask = _nextRebalanceTask.getAndSet(newTask);
    if (preTask != null) {
      preTask.cancel();
    }
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
      // Need to add MaintenanceRecoveryStage here because MAX_PARTITIONS_PER_INSTANCE check could
      // only occur after IntermediateStateCalcStage calculation
      rebalancePipeline.addStage(new MaintenanceRecoveryStage());
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

      // auto-exit maintenance mode if applicable
      Pipeline autoExitMaintenancePipeline = new Pipeline(pipelineName);
      autoExitMaintenancePipeline.addStage(new MaintenanceRecoveryStage());

      registry.register(ClusterEventType.IdealStateChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.CurrentStateChange, dataRefresh, dataPreprocess, externalViewPipeline, rebalancePipeline);
      registry.register(ClusterEventType.InstanceConfigChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.ResourceConfigChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.ClusterConfigChange, dataRefresh, autoExitMaintenancePipeline, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.LiveInstanceChange, dataRefresh, autoExitMaintenancePipeline, liveInstancePipeline, dataPreprocess, externalViewPipeline, rebalancePipeline);
      registry.register(ClusterEventType.MessageChange, dataRefresh, dataPreprocess, rebalancePipeline);
      registry.register(ClusterEventType.Resume, dataRefresh, dataPreprocess, externalViewPipeline, rebalancePipeline);
      registry.register(ClusterEventType.PeriodicalRebalance, dataRefresh, autoExitMaintenancePipeline, dataPreprocess, externalViewPipeline, rebalancePipeline);
      registry.register(ClusterEventType.OnDemandRebalance, dataRefresh, autoExitMaintenancePipeline, dataPreprocess, externalViewPipeline, rebalancePipeline);
      return registry;
    }
  }

  private static PipelineRegistry createTaskRegistry(String pipelineName) {
    logger.info("createTaskRegistry");
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
      registry.register(ClusterEventType.OnDemandRebalance, dataRefresh, dataPreprocess,
          rebalancePipeline);
      return registry;
    }
  }

  // TODO: refactor the constructor as providing both registry but only enabling one looks confusing
  public GenericHelixController(PipelineRegistry registry, PipelineRegistry taskRegistry) {
    this(registry, taskRegistry, null, Sets.newHashSet(
        Pipeline.Type.TASK, Pipeline.Type.DEFAULT));
  }

  private GenericHelixController(PipelineRegistry registry, PipelineRegistry taskRegistry,
      final String clusterName, Set<Pipeline.Type> enabledPipelineTypes) {
    _paused = false;
    _enabledPipelineTypes = enabledPipelineTypes;
    _registry = registry;
    _taskRegistry = taskRegistry;
    _lastSeenInstances = new AtomicReference<>();
    _lastSeenSessions = new AtomicReference<>();
    _clusterName = clusterName;
    _lastPipelineEndTimestamp = TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
    _clusterStatusMonitor = new ClusterStatusMonitor(_clusterName);

    _asyncTasksThreadPool =
        Executors.newScheduledThreadPool(ASYNC_TASKS_THREADPOOL_SIZE, new ThreadFactory() {
          @Override public Thread newThread(Runnable r) {
            return new Thread(r, "HelixController-async_tasks-" + _clusterName);
          }
        });
    _asyncFIFOWorkerPool = new HashMap<>();
    initializeAsyncFIFOWorkers();

    _onDemandRebalanceTimer = new Timer(true);

    // initialize pipelines at the end so we have everything else prepared
    if (_enabledPipelineTypes.contains(Pipeline.Type.DEFAULT)) {
      logger.info("Initializing {} pipeline", Pipeline.Type.DEFAULT.name());
      _resourceControlDataProvider = new ResourceControllerDataProvider(clusterName);
      _eventQueue = new ClusterEventBlockingQueue();
      _eventThread = new ClusterEventProcessor(_resourceControlDataProvider, _eventQueue,
          "default-" + clusterName);
      initPipeline(_eventThread, _resourceControlDataProvider);
      logger.info("Initialized {} pipeline", Pipeline.Type.DEFAULT.name());
    } else {
      _eventQueue = null;
      _resourceControlDataProvider = null;
      _eventThread = null;
    }

    if (_enabledPipelineTypes.contains(Pipeline.Type.TASK)) {
      logger.info("Initializing {} pipeline", Pipeline.Type.TASK.name());
      _workflowControlDataProvider = new WorkflowControllerDataProvider(clusterName);
      _taskEventQueue = new ClusterEventBlockingQueue();
      _taskEventThread = new ClusterEventProcessor(_workflowControlDataProvider, _taskEventQueue,
          "task-" + clusterName);
      initPipeline(_taskEventThread, _workflowControlDataProvider);
      logger.info("Initialized {} pipeline", Pipeline.Type.TASK.name());
    } else {
      _workflowControlDataProvider = null;
      _taskEventQueue = null;
      _taskEventThread = null;
    }

    if (clusterName != null) {
      HelixControllerFactory.put(clusterName, this);
    }
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
      return _taskEventQueue == null ||  _taskEventQueue.isEmpty();
    } else {
      return _eventQueue == null || _eventQueue.isEmpty();
    }
  }

  /**
   * lock-always: caller always needs to obtain an external lock before call, calls to handleEvent()
   * should be serialized
   * @param event cluster event to handle
   */
  private void handleEvent(ClusterEvent event, BaseControllerDataProvider dataProvider) {
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

    _helixManager = manager;

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
      if (context.getType() == NotificationContext.Type.FINALIZE) {
        stopPeriodRebalance();
        logger.info("Get FINALIZE notification, skip the pipeline. Event :" + event.getEventType());
        return;
      } else {
        // TODO: should be in the initialization of controller.
        if (_resourceControlDataProvider != null) {
          checkRebalancingTimer(manager, Collections.<IdealState>emptyList(), dataProvider.getClusterConfig());
        }
        if (_isMonitoring) {
          event.addAttribute(AttributeName.clusterStatusMonitor.name(), _clusterStatusMonitor);
        }
      }
    }

    dataProvider.setClusterEventId(event.getEventId());
    event.addAttribute(AttributeName.LastRebalanceFinishTimeStamp.name(), _lastPipelineEndTimestamp);

    // Prepare ClusterEvent
    // TODO (harry): this is a temporal workaround - after controller is separated we should not
    // have this instanceof clauses
    List<Pipeline> pipelines;
    boolean isTaskFrameworkPipeline = false;

    if (dataProvider instanceof ResourceControllerDataProvider) {
      pipelines = _registry
          .getPipelinesForEvent(event.getEventType());
    } else if (dataProvider instanceof WorkflowControllerDataProvider) {
      pipelines = _taskRegistry
          .getPipelinesForEvent(event.getEventType());
      isTaskFrameworkPipeline = true;
    } else {
      logger.warn(String
          .format("No %s pipeline to run for event: %s::%s", dataProvider.getPipelineName(),
              event.getEventType(), event.getEventId()));
      return;
    }
    event.addAttribute(AttributeName.ControllerDataProvider.name(), dataProvider);

    logger.info(String.format("START: Invoking %s controller pipeline for cluster %s event: %s  %s",
        manager.getClusterName(), dataProvider.getPipelineName(), event.getEventType(),
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
            "Exception while executing {} pipeline: {} for cluster {}. Will not continue to next pipeline",
            dataProvider.getPipelineName(), _clusterName, Arrays.toString(e.getStackTrace()));

        if (e instanceof HelixMetaDataAccessException) {
          rebalanceFail = true;
          // If pipeline failed due to read/write fails to zookeeper, retry the pipeline.
          dataProvider.requireFullRefresh();
          logger.warn("Rebalance pipeline failed due to read failure from zookeeper, cluster: " + _clusterName);

          // only push a retry event when there is no pending event in the corresponding event queue.
          if (isEventQueueEmpty(isTaskFrameworkPipeline)) {
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
    logger.info("END: Invoking {} controller pipeline for event {}::{} for cluster {}, took {} ms",
        dataProvider.getPipelineName(), event.getEventType(), event.getEventId(), _clusterName,
        _lastPipelineEndTimestamp - startTime);

    if (!isTaskFrameworkPipeline) {
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
        sb.append(String.format("Callback time for event: %s took: %s ms\n", event.getEventType(),
            enqueueTime - zkCallbackTime));
      }
      if (_isMonitoring) {
        _clusterStatusMonitor
            .updateClusterEventDuration(ClusterEventMonitor.PhaseName.InQueue.name(),
                startTime - enqueueTime);
        _clusterStatusMonitor
            .updateClusterEventDuration(ClusterEventMonitor.PhaseName.TotalProcessed.name(),
                _lastPipelineEndTimestamp - startTime);
      }
      sb.append(String.format("InQueue time for event: %s took: %s ms\n", event.getEventType(),
          startTime - enqueueTime));
      sb.append(String.format("TotalProcessed time for event: %s took: %s ms", event.getEventType(),
          _lastPipelineEndTimestamp - startTime));
      logger.info(sb.toString());
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
    notifyCaches(changeContext, ChangeType.LIVE_INSTANCE);

    if (liveInstances == null) {
      liveInstances = Collections.emptyList();
    }

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
      startPeriodRebalance(minPeriod, manager);
    } else {
      stopPeriodRebalance();
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

    if (changeContext.getType() != NotificationContext.Type.FINALIZE) {
      HelixManager manager = changeContext.getManager();
      if (manager != null) {
        HelixDataAccessor dataAccessor = changeContext.getManager().getHelixDataAccessor();
        checkRebalancingTimer(changeContext.getManager(), idealStates,
            (ClusterConfig) dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig()));
      }
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
    if (context == null || context.getType() != NotificationContext.Type.CALLBACK) {
      requestDataProvidersFullRefresh();
    } else {
      updateDataChangeInProvider(changeType, context.getPathChanged());
    }
  }

  private void updateDataChangeInProvider(ChangeType type, String path) {
    if (_resourceControlDataProvider != null) {
      _resourceControlDataProvider.notifyDataChange(type, path);
    }

    if (_workflowControlDataProvider != null) {
      _workflowControlDataProvider.notifyDataChange(type, path);
    }
  }

  private void requestDataProvidersFullRefresh() {
    if (_resourceControlDataProvider != null) {
      _resourceControlDataProvider.requireFullRefresh();
    }

    if (_workflowControlDataProvider != null) {
      _workflowControlDataProvider.requireFullRefresh();
    }
  }

  private void pushToEventQueues(ClusterEventType eventType, NotificationContext changeContext,
      Map<String, Object> eventAttributes) {
    // No need for completed UUID, prefixed should be fine
    String uid = UUID.randomUUID().toString().substring(0, 8);
    ClusterEvent event = new ClusterEvent(_clusterName, eventType,
        String.format("%s_%s", uid, Pipeline.Type.DEFAULT.name()));
    event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
    event.addAttribute(AttributeName.changeContext.name(), changeContext);
    event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), _asyncFIFOWorkerPool);
    for (Map.Entry<String, Object> attr : eventAttributes.entrySet()) {
      event.addAttribute(attr.getKey(), attr.getValue());
    }
    enqueueEvent(_eventQueue, event);
    enqueueEvent(_taskEventQueue,
        event.clone(String.format("%s_%s", uid, Pipeline.Type.TASK.name())));
  }

  private void enqueueEvent(ClusterEventBlockingQueue queue, ClusterEvent event) {
    if (event == null || queue == null) {
      return;
    }
    queue.put(event);
  }

  @Override
  public void onControllerChange(NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onControllerChange() for cluster " + _clusterName);

    requestDataProvidersFullRefresh();

    boolean controllerIsLeader;

    if (changeContext == null || changeContext.getType() == NotificationContext.Type.FINALIZE) {
      logger.info(
          "GenericClusterController.onControllerChange() Cluster change type {} for cluster {}. Disable leadership.",
          changeContext == null ? null : changeContext.getType(), _clusterName);
      controllerIsLeader = false;
    } else {
      // double check if this controller is the leader
      controllerIsLeader = changeContext.getManager().isLeader();
    }

    if (controllerIsLeader) {
      HelixManager manager = changeContext.getManager();
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
      curSessions.put(liveInstance.getEphemeralOwner(), liveInstance);
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
    stopPeriodRebalance();

    logger.info("Shutting down {} pipeline", Pipeline.Type.DEFAULT.name());
    shutdownPipeline(_eventThread, _eventQueue);

    logger.info("Shutting down {} pipeline", Pipeline.Type.TASK.name());
    shutdownPipeline(_taskEventThread, _taskEventQueue);

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
          if (_resourceControlDataProvider != null) {
            _resourceControlDataProvider.clearMonitoringRecords();
          }
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

  private void shutdownPipeline(Thread thread, ClusterEventBlockingQueue queue)
      throws InterruptedException {
    if (queue != null) {
      queue.clear();
    }

    if (thread != null) {
      while (thread.isAlive()) {
        thread.interrupt();
        thread.join(EVENT_THREAD_JOIN_TIMEOUT);
      }
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
            String.format("%s_%s", uid, Pipeline.Type.DEFAULT.name()));
        event.addAttribute(AttributeName.changeContext.name(), changeContext);
        event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
        event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), _asyncFIFOWorkerPool);
        enqueueEvent(_eventQueue, event);
        enqueueEvent(_taskEventQueue,
            event.clone(String.format("%s_%s", uid, Pipeline.Type.TASK.name())));
      }
    }
    return statusFlag;
  }


  // TODO: refactor this to use common/ClusterEventProcessor.
  @Deprecated
  private class ClusterEventProcessor extends Thread {
    private final BaseControllerDataProvider _cache;
    private final ClusterEventBlockingQueue _eventBlockingQueue;
    private final String _processorName;

    ClusterEventProcessor(BaseControllerDataProvider cache,
        ClusterEventBlockingQueue eventBlockingQueue, String processorName) {
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
          ClusterEvent newClusterEvent = _eventBlockingQueue.take();
          String threadName = String.format(
              "HelixController-pipeline-%s-(%s)", _processorName, newClusterEvent.getEventId());
          this.setName(threadName);
          handleEvent(newClusterEvent, _cache);
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

  private void initPipeline(Thread eventThread, BaseControllerDataProvider cache) {
    if (eventThread == null || cache == null) {
      logger.warn("pipeline cannot be initialized");
      return;
    }
    cache.setAsyncTasksThreadPool(_asyncTasksThreadPool);

    eventThread.setDaemon(true);
    eventThread.start();
  }
}
