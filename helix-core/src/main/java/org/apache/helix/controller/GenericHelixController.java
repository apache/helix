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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.PipelineRegistry;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventBlockingQueue;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CompatibilityCheckStage;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ExternalViewComputeStage;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.MessageGenerationPhase;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.controller.stages.MessageThrottleStage;
import org.apache.helix.controller.stages.PersistAssignmentStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.controller.stages.ResourceValidationStage;
import org.apache.helix.controller.stages.TaskAssignmentStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.monitoring.mbeans.ClusterEventMonitor;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.task.TaskDriver;
import org.apache.log4j.Logger;

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
public class GenericHelixController implements ConfigChangeListener, IdealStateChangeListener,
    LiveInstanceChangeListener, MessageListener, CurrentStateChangeListener,
    ControllerChangeListener, InstanceConfigChangeListener {
  private static final Logger logger = Logger.getLogger(GenericHelixController.class.getName());
  private static final long EVENT_THREAD_JOIN_TIMEOUT = 1000;
  private static final int ASYNC_TASKS_THREADPOOL_SIZE = 40;
  private final PipelineRegistry _registry;
  private final PipelineRegistry _taskRegistry;

  final AtomicReference<Map<String, LiveInstance>> _lastSeenInstances;
  final AtomicReference<Map<String, LiveInstance>> _lastSeenSessions;

  ClusterStatusMonitor _clusterStatusMonitor;

  /**
   * A queue for controller events and a thread that will consume it
   */
  private final ClusterEventBlockingQueue _eventQueue;
  private final ClusterEventProcessor _eventThread;

  private final ClusterEventBlockingQueue _taskEventQueue;
  private final ClusterEventProcessor _taskEventThread;

  /**
   * The _paused flag is checked by function handleEvent(), while if the flag is set handleEvent()
   * will be no-op. Other event handling logic keeps the same when the flag is set.
   */
  private boolean _paused;

  /**
   * The timer that can periodically run the rebalancing pipeline. The timer will start if there is
   * one resource group has the config to use the timer.
   */
  Timer _rebalanceTimer = null;
  long _timerPeriod = Long.MAX_VALUE;

  /**
   * A cache maintained across pipelines
   */
  private ClusterDataCache _cache;
  private ClusterDataCache _taskCache;
  private ExecutorService _asyncTasksThreadPool;

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
        createDefaultRegistry(PipelineTypes.TASK.name()));
  }

  public GenericHelixController(String clusterName) {
    this(createDefaultRegistry(PipelineTypes.DEFAULT.name()),
        createDefaultRegistry(PipelineTypes.TASK.name()), clusterName);
  }

  class RebalanceTask extends TimerTask {
    HelixManager _manager;

    public RebalanceTask(HelixManager manager) {
      _manager = manager;
    }

    @Override
    public void run() {
      //TODO: this is the temporary workaround
      _cache.requireFullRefresh();
      _taskCache.requireFullRefresh();
      _cache.refresh(_manager.getHelixDataAccessor());
      _taskCache.refresh(_manager.getHelixDataAccessor());
      if (_cache.getLiveInstances() != null) {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        synchronized (_manager) {
          checkLiveInstancesObservation(new ArrayList<>(_cache.getLiveInstances().values()),
              changeContext);
        }
      }

      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      ClusterEvent event = new ClusterEvent(_clusterName,ClusterEventType.PeriodicalRebalance);
      event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
      event.addAttribute(AttributeName.changeContext.name(), changeContext);
      List<ZNRecord> dummy = new ArrayList<ZNRecord>();
      event.addAttribute(AttributeName.eventData.name(), dummy);
      // Should be able to process
      _eventQueue.put(event);
      _taskEventQueue.put(event.clone());
      logger.info("Controller periodicalRebalance event triggered!");
    }
  }

  // TODO who should stop this timer
  /**
   * Starts the rebalancing timer with the specified period. Start the timer if necessary; If the
   * period is smaller than the current period, cancel the current timer and use the new period.
   */
  void startRebalancingTimer(long period, HelixManager manager) {
    if (period != _timerPeriod) {
      logger.info("Controller starting timer at period " + period);
      if (_rebalanceTimer != null) {
        _rebalanceTimer.cancel();
      }
      _rebalanceTimer = new Timer(true);
      _timerPeriod = period;
      _rebalanceTimer
          .scheduleAtFixedRate(new RebalanceTask(manager), _timerPeriod, _timerPeriod);
    } else {
      logger.info("Controller already has timer at period " + _timerPeriod);
    }
  }

  /**
   * Stops the rebalancing timer
   */
  void stopRebalancingTimer() {
    if (_rebalanceTimer != null) {
      _rebalanceTimer.cancel();
      _rebalanceTimer = null;
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

      // rebalance pipeline
      Pipeline rebalancePipeline = new Pipeline(pipelineName);
      rebalancePipeline.addStage(new ResourceComputationStage());
      rebalancePipeline.addStage(new ResourceValidationStage());
      rebalancePipeline.addStage(new CurrentStateComputationStage());
      rebalancePipeline.addStage(new BestPossibleStateCalcStage());
      rebalancePipeline.addStage(new IntermediateStateCalcStage());
      rebalancePipeline.addStage(new MessageGenerationPhase());
      rebalancePipeline.addStage(new MessageSelectionStage());
      rebalancePipeline.addStage(new MessageThrottleStage());
      rebalancePipeline.addStage(new TaskAssignmentStage());
      rebalancePipeline.addStage(new PersistAssignmentStage());

      // external view generation
      Pipeline externalViewPipeline = new Pipeline(pipelineName);
      externalViewPipeline.addStage(new ExternalViewComputeStage());

      // backward compatibility check
      Pipeline liveInstancePipeline = new Pipeline(pipelineName);
      liveInstancePipeline.addStage(new CompatibilityCheckStage());

      registry.register(ClusterEventType.IdealStateChange, dataRefresh, rebalancePipeline);
      registry.register(ClusterEventType.CurrentStateChange, dataRefresh, rebalancePipeline, externalViewPipeline);
      registry.register(ClusterEventType.ConfigChange, dataRefresh, rebalancePipeline);
      registry.register(ClusterEventType.LiveInstanceChange, dataRefresh, liveInstancePipeline, rebalancePipeline,
          externalViewPipeline);

      registry.register(ClusterEventType.MessageChange, dataRefresh, rebalancePipeline);
      registry.register(ClusterEventType.ExternalViewChange, dataRefresh);
      registry.register(ClusterEventType.Resume, dataRefresh, rebalancePipeline, externalViewPipeline);
      registry
          .register(ClusterEventType.PeriodicalRebalance, dataRefresh, rebalancePipeline, externalViewPipeline);
      return registry;
    }
  }

  public GenericHelixController(PipelineRegistry registry, PipelineRegistry taskRegistry) {
    this(registry, taskRegistry, null);
  }

  private GenericHelixController(PipelineRegistry registry, PipelineRegistry taskRegistry,
      String clusterName) {
    _paused = false;
    _registry = registry;
    _taskRegistry = taskRegistry;
    _lastSeenInstances = new AtomicReference<>();
    _lastSeenSessions = new AtomicReference<>();
    _clusterName = clusterName;
    _asyncTasksThreadPool = Executors.newFixedThreadPool(ASYNC_TASKS_THREADPOOL_SIZE);

    _eventQueue = new ClusterEventBlockingQueue();
    _taskEventQueue = new ClusterEventBlockingQueue();
    _cache = new ClusterDataCache(clusterName);
    _taskCache = new ClusterDataCache(clusterName);

    _eventThread = new ClusterEventProcessor(_cache, _eventQueue);
    _taskEventThread = new ClusterEventProcessor(_taskCache, _taskEventQueue);

    initPipelines(_eventThread, _cache, false);
    initPipelines(_taskEventThread, _taskCache, true);
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

    if (_paused) {
      logger.info("Cluster " + manager.getClusterName() + " is paused. Ignoring the event:" + event
          .getEventType());
      return;
    }

    NotificationContext context = null;
    if (event.getAttribute(AttributeName.changeContext.name()) != null) {
      context = event.getAttribute(AttributeName.changeContext.name());
    }

    // Initialize _clusterStatusMonitor
    if (context != null) {
      if (context.getType() == Type.FINALIZE) {
        stopRebalancingTimer();
        logger.info("Get FINALIZE notification, skip the pipeline. Event :" + event.getEventType());
        return;
      } else {
        if (_clusterStatusMonitor == null) {
          _clusterStatusMonitor = new ClusterStatusMonitor(manager.getClusterName());
        }
        // TODO: should be in the initization of controller.
        if (_cache != null) {
          checkRebalancingTimer(manager, Collections.EMPTY_LIST, _cache.getClusterConfig());
        }

        if (cache.isTaskCache()) {
          TaskDriver driver = new TaskDriver(manager);
          _clusterStatusMonitor.refreshWorkflowsStatus(driver);
          _clusterStatusMonitor.refreshJobsStatus(driver);
        }
        event.addAttribute(AttributeName.clusterStatusMonitor.name(), _clusterStatusMonitor);
      }
    }

    // add the cache
    event.addAttribute(AttributeName.ClusterDataCache.name(), cache);

    List<Pipeline> pipelines = cache.isTaskCache()
        ? _registry.getPipelinesForEvent(event.getEventType())
        : _taskRegistry.getPipelinesForEvent(event.getEventType());
    if (pipelines == null || pipelines.size() == 0) {
      logger.info(
          "No " + getPipelineType(cache.isTaskCache()) + " pipeline to run for event:" + event
              .getEventType());
      return;
    }

    logger.info(String.format("START: Invoking %s controller pipeline for event: %s",
        getPipelineType(cache.isTaskCache()), event.getEventType()));
    long startTime = System.currentTimeMillis();
    for (Pipeline pipeline : pipelines) {
      try {
        pipeline.handle(event);
        pipeline.finish();
      } catch (Exception e) {
        logger.error(
            "Exception while executing " + getPipelineType(cache.isTaskCache()) + "pipeline: "
                + pipeline + ". Will not continue to next pipeline", e);
        break;
      }
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        "END: Invoking " + getPipelineType(cache.isTaskCache()) + " controller pipeline for event: "
            + event.getEventType() + " for cluster " + manager.getClusterName() + ", took " + (
            endTime - startTime) + " ms");

    if (!cache.isTaskCache()) {
      // report event process durations
      if (_clusterStatusMonitor != null) {
        NotificationContext notificationContext =
            event.getAttribute(AttributeName.changeContext.name());
        long enqueueTime = event.getCreationTime();
        long zkCallbackTime;
        StringBuilder sb = new StringBuilder();
        if (notificationContext != null) {
          zkCallbackTime = notificationContext.getCreationTime();
          _clusterStatusMonitor
              .updateClusterEventDuration(ClusterEventMonitor.PhaseName.Callback.name(),
                  enqueueTime - zkCallbackTime);
          sb.append(String.format(
              "Callback time for event: " + event.getEventType() + " took: " + (enqueueTime
                  - zkCallbackTime) + " ms\n"));

        }
        _clusterStatusMonitor
            .updateClusterEventDuration(ClusterEventMonitor.PhaseName.InQueue.name(),
                startTime - enqueueTime);
        _clusterStatusMonitor
            .updateClusterEventDuration(ClusterEventMonitor.PhaseName.TotalProcessed.name(),
                endTime - startTime);
        sb.append(String.format(
            "InQueue time for event: " + event.getEventType() + " took: " + (startTime
                - enqueueTime) + " ms\n"));
        sb.append(String.format(
            "TotalProcessed time for event: " + event.getEventType() + " took: " + (endTime
                - startTime) + " ms"));
        logger.info(sb.toString());
      }
    }
  }


  @Override
  @PreFetch(enabled = false)
  public void onStateChange(String instanceName, List<CurrentState> statesInfo,
      NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onStateChange()");
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
      _taskCache.requireFullRefresh();
    } else {
      _cache.updateDataChange(ChangeType.CURRENT_STATE);
      _taskCache.updateDataChange(ChangeType.CURRENT_STATE);
    }
    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.CurrentStateChange);
    event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
    event.addAttribute(AttributeName.instanceName.name(), instanceName);
    event.addAttribute(AttributeName.changeContext.name(), changeContext);

    _eventQueue.put(event);
    _taskEventQueue.put(event.clone());
    logger.info("END: GenericClusterController.onStateChange()");
  }

  @Override
  @PreFetch(enabled = false)
  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onMessage() for cluster " + _clusterName);
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
      _taskCache.requireFullRefresh();
    } else {
      _cache.updateDataChange(ChangeType.MESSAGE);
      _taskCache.updateDataChange(ChangeType.MESSAGE);
    }

    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.MessageChange);
    event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
    event.addAttribute(AttributeName.instanceName.name(), instanceName);
    event.addAttribute(AttributeName.changeContext.name(), changeContext);

    _eventQueue.put(event);
    _taskEventQueue.put(event.clone());

    if (_clusterStatusMonitor != null && messages != null) {
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

    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.LiveInstanceChange);
    event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
    event.addAttribute(AttributeName.changeContext.name(), changeContext);
    event.addAttribute(AttributeName.eventData.name(), liveInstances);
    _eventQueue.put(event);
    _taskEventQueue.put(event.clone());
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
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
      _taskCache.requireFullRefresh();
    } else {
      _cache.updateDataChange(ChangeType.IDEAL_STATE);
      _taskCache.updateDataChange(ChangeType.IDEAL_STATE);
    }

    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.IdealStateChange);
    event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
    event.addAttribute(AttributeName.changeContext.name(), changeContext);

    _eventQueue.put(event);
    _taskEventQueue.put(event.clone());

    if (changeContext.getType() != Type.FINALIZE) {
      checkRebalancingTimer(changeContext.getManager(), idealStates, _cache.getClusterConfig());
    }

    logger.info("END: GenericClusterController.onIdealStateChange() for cluster " + _clusterName);
  }

  @Override
  @PreFetch(enabled = false)
  public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onConfigChange() for cluster " + _clusterName);
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
      _taskCache.requireFullRefresh();
    } else {
      _cache.updateDataChange(ChangeType.INSTANCE_CONFIG);
      _taskCache.updateDataChange(ChangeType.INSTANCE_CONFIG);
    }

    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.ConfigChange);
    event.addAttribute(AttributeName.changeContext.name(), changeContext);
    event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
    _eventQueue.put(event);
    _taskEventQueue.put(event.clone());

    logger.info("END: GenericClusterController.onConfigChange() for cluster " + _clusterName);
  }

  @Override
  @PreFetch(enabled = false)
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
      NotificationContext changeContext) {
    logger.info(
        "START: GenericClusterController.onInstanceConfigChange() for cluster " + _clusterName);
    onConfigChange(instanceConfigs, changeContext);
    logger.info("END: GenericClusterController.onInstanceConfigChange() for cluster " + _clusterName);
  }

  @Override
  public void onControllerChange(NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onControllerChange() for cluster " + _clusterName);
    _cache.requireFullRefresh();
    _taskCache.requireFullRefresh();
    if (changeContext != null && changeContext.getType() == Type.FINALIZE) {
      logger.info("GenericClusterController.onControllerChange() FINALIZE for cluster " + _clusterName);
      return;
    }
    HelixDataAccessor accessor = changeContext.getManager().getHelixDataAccessor();

    // double check if this controller is the leader
    Builder keyBuilder = accessor.keyBuilder();
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null) {
      logger
          .warn("No controller exists for cluster:" + changeContext.getManager().getClusterName());
      return;
    } else {
      String leaderName = leader.getInstanceName();

      String instanceName = changeContext.getManager().getInstanceName();
      if (leaderName == null || !leaderName.equals(instanceName)) {
        logger.warn("leader name does NOT match, my name: " + instanceName + ", leader: " + leader);
        return;
      }
    }

    PauseSignal pauseSignal = accessor.getProperty(keyBuilder.pause());
    if (pauseSignal != null) {
      if (!_paused) {
        _paused = true;
        logger.info("controller is now paused");
      }
    } else {
      if (_paused) {
        _paused = false;
        logger.info("controller is now resumed");
        ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.Resume);
        event.addAttribute(AttributeName.changeContext.name(), changeContext);
        event.addAttribute(AttributeName.helixmanager.name(), changeContext.getManager());
        event.addAttribute(AttributeName.eventData.name(), pauseSignal);
        _eventQueue.put(event);
        _taskEventQueue.put(event.clone());
      }
    }
    if (_clusterStatusMonitor == null) {
      _clusterStatusMonitor = new ClusterStatusMonitor(changeContext.getManager().getClusterName());
    }
    _clusterStatusMonitor.setEnabled(!_paused);
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
    Map<String, LiveInstance> curInstances = new HashMap<String, LiveInstance>();
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

  public void shutdownClusterStatusMonitor(String clusterName) {
    if (_clusterStatusMonitor != null) {
      logger.info("Shut down _clusterStatusMonitor for cluster " + clusterName);
      _clusterStatusMonitor.reset();
      _clusterStatusMonitor = null;
    }
  }

  public void shutdown() throws InterruptedException {
    stopRebalancingTimer();

    terminateEventThread(_eventThread);
    terminateEventThread(_taskEventThread);

    _asyncTasksThreadPool.shutdown();
  }

  private void terminateEventThread(Thread thread) throws InterruptedException {
    while (thread.isAlive()) {
      thread.interrupt();
      thread.join(EVENT_THREAD_JOIN_TIMEOUT);
    }
  }

  private class ClusterEventProcessor extends Thread {
    private final ClusterDataCache _cache;
    private final ClusterEventBlockingQueue _eventBlockingQueue;

    public ClusterEventProcessor(ClusterDataCache cache,
        ClusterEventBlockingQueue eventBlockingQueue) {
      _cache = cache;
      _eventBlockingQueue = eventBlockingQueue;
    }

    @Override public void run() {
      logger.info("START ClusterEventProcessor thread  for cluster " + _clusterName);
      while (!isInterrupted()) {
        try {
          ClusterEvent event = _eventBlockingQueue.take();
          handleEvent(event, _cache);
        } catch (InterruptedException e) {
          logger.warn("ClusterEventProcessor interrupted", e);
          interrupt();
        } catch (ZkInterruptedException e) {
          logger.warn("ClusterEventProcessor caught a ZK connection interrupt", e);
          interrupt();
        } catch (ThreadDeath death) {
          throw death;
        } catch (Throwable t) {
          logger.error("ClusterEventProcessor failed while running the controller pipeline", t);
        }
      }
      logger.info("END ClusterEventProcessor thread");
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
