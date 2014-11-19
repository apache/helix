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
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.MessageListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ScopedConfigChangeListener;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.PipelineRegistry;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventBlockingQueue;
import org.apache.helix.controller.stages.CompatibilityCheckStage;
import org.apache.helix.controller.stages.ContainerProvisioningStage;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ExternalViewComputeStage;
import org.apache.helix.controller.stages.MessageGenerationStage;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.controller.stages.MessageThrottleStage;
import org.apache.helix.controller.stages.PersistAssignmentStage;
import org.apache.helix.controller.stages.PersistContextStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.controller.stages.ResourceValidationStage;
import org.apache.helix.controller.stages.TaskAssignmentStage;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

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
    ControllerChangeListener, InstanceConfigChangeListener, ScopedConfigChangeListener {
  private static final Logger logger = Logger.getLogger(GenericHelixController.class.getName());
  private static final long EVENT_THREAD_JOIN_TIMEOUT = 1000L;
  volatile boolean init = false;
  private final PipelineRegistry _registry;

  final AtomicReference<Map<String, LiveInstance>> _lastSeenInstances;
  final AtomicReference<Map<String, LiveInstance>> _lastSeenSessions;

  ClusterStatusMonitor _clusterStatusMonitor;

  /**
   * A queue for controller events and a thread that will consume it
   */
  private final ClusterEventBlockingQueue _eventQueue;
  private final ClusterEventProcessor _eventThread;

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
  int _timerPeriod = Integer.MAX_VALUE;

  /**
   * A cache maintained across pipelines
   */
  private ClusterDataCache _cache;

  /**
   * Default constructor that creates a default pipeline registry. This is sufficient in most cases,
   * but if there is a some thing specific needed use another constructor where in you can pass a
   * pipeline registry
   */
  public GenericHelixController() {
    this(createDefaultRegistry());
  }

  class RebalanceTask extends TimerTask {
    HelixManager _manager;

    public RebalanceTask(HelixManager manager) {
      _manager = manager;
    }

    @Override
    public void run() {
      _cache.requireFullRefresh();
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      ClusterEvent event = new ClusterEvent("periodicalRebalance");
      event.addAttribute("helixmanager", changeContext.getManager());
      event.addAttribute("changeContext", changeContext);
      List<ZNRecord> dummy = new ArrayList<ZNRecord>();
      event.addAttribute("eventData", dummy);
      // Should be able to process
      _eventQueue.put(event);
    }
  }

  // TODO who should stop this timer
  /**
   * Starts the rebalancing timer with the specified period. Start the timer if necessary; If the
   * period is smaller than the current period, cancel the current timer and use the new period.
   */
  void startRebalancingTimer(int period, HelixManager manager) {
    logger.info("Controller starting timer at period " + period);
    if (period < _timerPeriod) {
      if (_rebalanceTimer != null) {
        _rebalanceTimer.cancel();
      }
      _rebalanceTimer = new Timer(true);
      _timerPeriod = period;
      _rebalanceTimer.scheduleAtFixedRate(new RebalanceTask(manager), _timerPeriod, _timerPeriod);
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

  private static PipelineRegistry createDefaultRegistry() {
    logger.info("createDefaultRegistry");
    synchronized (GenericHelixController.class) {
      PipelineRegistry registry = new PipelineRegistry();

      // cluster data cache refresh
      Pipeline dataRefresh = new Pipeline();
      dataRefresh.addStage(new ReadClusterDataStage());

      // rebalance pipeline
      Pipeline rebalancePipeline = new Pipeline();
      rebalancePipeline.addStage(new CompatibilityCheckStage());
      rebalancePipeline.addStage(new ResourceComputationStage());
      rebalancePipeline.addStage(new ResourceValidationStage());
      rebalancePipeline.addStage(new ContainerProvisioningStage());
      rebalancePipeline.addStage(new CurrentStateComputationStage());
      rebalancePipeline.addStage(new BestPossibleStateCalcStage());
      rebalancePipeline.addStage(new MessageGenerationStage());
      rebalancePipeline.addStage(new MessageSelectionStage());
      rebalancePipeline.addStage(new MessageThrottleStage());
      rebalancePipeline.addStage(new TaskAssignmentStage());
      rebalancePipeline.addStage(new PersistAssignmentStage());
      rebalancePipeline.addStage(new PersistContextStage());

      // external view generation
      Pipeline externalViewPipeline = new Pipeline();
      externalViewPipeline.addStage(new ExternalViewComputeStage());

      registry.register("idealStateChange", dataRefresh, rebalancePipeline);
      registry.register("currentStateChange", dataRefresh, rebalancePipeline, externalViewPipeline);
      registry.register("configChange", dataRefresh, rebalancePipeline);
      registry.register("instanceConfigChange", dataRefresh, rebalancePipeline);
      registry.register("resourceConfigChange", dataRefresh, rebalancePipeline);
      registry.register("liveInstanceChange", dataRefresh, rebalancePipeline, externalViewPipeline);

      registry.register("messageChange", dataRefresh, rebalancePipeline);
      registry.register("externalView", dataRefresh);
      registry.register("resume", dataRefresh, rebalancePipeline, externalViewPipeline);
      registry
          .register("periodicalRebalance", dataRefresh, rebalancePipeline, externalViewPipeline);
      return registry;
    }
  }

  public GenericHelixController(PipelineRegistry registry) {
    _paused = false;
    _registry = registry;
    _lastSeenInstances = new AtomicReference<Map<String, LiveInstance>>();
    _lastSeenSessions = new AtomicReference<Map<String, LiveInstance>>();
    _cache = new ClusterDataCache();
    _eventQueue = new ClusterEventBlockingQueue();
    _eventThread = new ClusterEventProcessor();
    _eventThread.setDaemon(true);
    _eventThread.start();
  }

  /**
   * lock-always: caller always needs to obtain an external lock before call, calls to handleEvent()
   * should be serialized
   * @param event
   */
  protected synchronized void handleEvent(ClusterEvent event) {
    HelixManager manager = event.getAttribute("helixmanager");
    if (manager == null) {
      logger.error("No cluster manager in event:" + event.getName());
      return;
    }

    if (!manager.isLeader()) {
      logger.error("Cluster manager: " + manager.getInstanceName()
          + " is not leader. Pipeline will not be invoked");
      return;
    }

    if (_paused) {
      logger.info("Cluster is paused. Ignoring the event:" + event.getName());
      return;
    }

    NotificationContext context = null;
    if (event.getAttribute("changeContext") != null) {
      context = (NotificationContext) (event.getAttribute("changeContext"));
    }

    // Initialize _clusterStatusMonitor
    if (context != null) {
      if (context.getType() == Type.FINALIZE) {
        stopRebalancingTimer();
        logger.info("Get FINALIZE notification, skip the pipeline. Event: " + event.getName());
        return;
      } else {
        if (_clusterStatusMonitor == null) {
          _clusterStatusMonitor = new ClusterStatusMonitor(manager.getClusterName());
        }

        event.addAttribute("clusterStatusMonitor", _clusterStatusMonitor);
      }
    }

    // add the cache
    event.addAttribute("ClusterDataCache", _cache);

    List<Pipeline> pipelines = _registry.getPipelinesForEvent(event.getName());
    if (pipelines == null || pipelines.size() == 0) {
      logger.info("No pipeline to run for event:" + event.getName());
      return;
    }

    logger.info("START: Invoking controller pipeline for event: " + event.getName());
    long startTime = System.currentTimeMillis();
    for (Pipeline pipeline : pipelines) {
      try {
        pipeline.handle(event);
        pipeline.finish();
      } catch (Exception e) {
        logger.error("Exception while executing pipeline: " + pipeline
            + ". Will not continue to next pipeline", e);
        break;
      }
    }
    long endTime = System.currentTimeMillis();
    logger.info("END: Invoking controller pipeline for event: " + event.getName() + ", took "
        + (endTime - startTime) + " ms");
  }

  // TODO since we read data in pipeline, we can get rid of reading from zookeeper in
  // callback

  @Override
  public void onStateChange(String instanceName, List<CurrentState> statesInfo,
      NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onStateChange()");
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
    }
    ClusterEvent event = new ClusterEvent("currentStateChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("instanceName", instanceName);
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", statesInfo);
    _eventQueue.put(event);
    logger.info("END: GenericClusterController.onStateChange()");
  }

  @Override
  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onMessage()");
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
    }

    ClusterEvent event = new ClusterEvent("messageChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("instanceName", instanceName);
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", messages);
    _eventQueue.put(event);

    if (_clusterStatusMonitor != null && messages != null) {
      _clusterStatusMonitor.addMessageQueueSize(instanceName, messages.size());
    }

    logger.info("END: GenericClusterController.onMessage()");
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    logger.info("START: Generic GenericClusterController.onLiveInstanceChange()");
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
    }

    if (liveInstances == null) {
      liveInstances = Collections.emptyList();
    }
    _cache.setLiveInstances(liveInstances);

    // Go though the live instance list and make sure that we are observing them
    // accordingly. The action is done regardless of the paused flag.
    if (changeContext.getType() == NotificationContext.Type.INIT
        || changeContext.getType() == NotificationContext.Type.CALLBACK) {
      checkLiveInstancesObservation(liveInstances, changeContext);
    } else if (changeContext.getType() == NotificationContext.Type.FINALIZE) {
      // on finalize, should remove all message/current-state listeners
      logger.info("remove message/current-state listeners. lastSeenInstances: "
          + _lastSeenInstances + ", lastSeenSessions: " + _lastSeenSessions);
      liveInstances = Collections.emptyList();
      checkLiveInstancesObservation(liveInstances, changeContext);
    }

    ClusterEvent event = new ClusterEvent("liveInstanceChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", liveInstances);
    _eventQueue.put(event);
    logger.info("END: Generic GenericClusterController.onLiveInstanceChange()");
  }

  void checkRebalancingTimer(HelixManager manager, List<IdealState> idealStates) {
    if (manager.getConfigAccessor() == null) {
      logger.warn(manager.getInstanceName()
          + " config accessor doesn't exist. should be in file-based mode.");
      return;
    }

    for (IdealState idealState : idealStates) {
      int period = idealState.getRebalanceTimerPeriod();
      if (period > 0) {
        startRebalancingTimer(period, manager);
      }
    }
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealStates, NotificationContext changeContext) {
    logger.info("START: Generic GenericClusterController.onIdealStateChange()");
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
    }

    if (idealStates == null) {
      idealStates = Collections.emptyList();
    }
    _cache.setIdealStates(idealStates);
    ClusterEvent event = new ClusterEvent("idealStateChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", idealStates);
    _eventQueue.put(event);

    if (changeContext.getType() != Type.FINALIZE) {
      checkRebalancingTimer(changeContext.getManager(), idealStates);
    }

    logger.info("END: GenericClusterController.onIdealStateChange()");
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onInstanceConfigChange()");
    if (changeContext == null || changeContext.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
    }

    if (configs == null) {
      configs = Collections.emptyList();
    }
    _cache.setInstanceConfigs(configs);

    ClusterEvent event = new ClusterEvent("instanceConfigChange");
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("eventData", configs);
    _eventQueue.put(event);
    logger.info("END: GenericClusterController.onInstanceConfigChange()");
  }

  @Override
  public void onConfigChange(List<HelixProperty> configs, NotificationContext context) {
    logger.info("START: GenericClusterController.onConfigChange()");
    if (context == null || context.getType() != Type.CALLBACK) {
      _cache.requireFullRefresh();
    }

    if (configs == null) {
      configs = Collections.emptyList();
    }

    String eventName;
    String path = context.getPathChanged();
    if (path.contains(ConfigScopeProperty.RESOURCE.toString())) {
      List<ResourceConfiguration> resourceConfigs = Lists.newArrayList();
      for (HelixProperty property : configs) {
        resourceConfigs.add(new ResourceConfiguration(property.getRecord()));
      }
      _cache.setResourceConfigs(resourceConfigs);
      eventName = "resourceConfigChange";
    } else if (path.contains(ConfigScopeProperty.CONSTRAINT.toString())) {
      List<ClusterConstraints> constraints = Lists.newArrayList();
      for (HelixProperty property : configs) {
        constraints.add(new ClusterConstraints(property.getRecord()));
      }
      _cache.setConstraints(constraints);
      eventName = "constraintChange";
    } else {
      logger.warn("Controller received event for unsupported path: " + path);
      eventName = "configChange";
    }

    ClusterEvent event = new ClusterEvent(eventName);
    event.addAttribute("changeContext", context);
    event.addAttribute("helixmanager", context.getManager());
    event.addAttribute("eventData", configs);
    _eventQueue.put(event);
    logger.info("END: GenericClusterController.onConfigChange()");
  }

  @Override
  public void onControllerChange(NotificationContext changeContext) {
    logger.info("START: GenericClusterController.onControllerChange()");
    _cache.requireFullRefresh();
    if (changeContext != null && changeContext.getType() == Type.FINALIZE) {
      logger.info("GenericClusterController.onControllerChange() FINALIZE");
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
      _paused = true;
      logger.info("controller is now paused");
    } else {
      if (_paused) {
        // it currently paused
        logger.info("controller is now resumed");
        _paused = false;
        ClusterEvent event = new ClusterEvent("resume");
        event.addAttribute("changeContext", changeContext);
        event.addAttribute("helixmanager", changeContext.getManager());
        event.addAttribute("eventData", pauseSignal);
        _eventQueue.put(event);
      } else {
        _paused = false;
      }
    }
    logger.info("END: GenericClusterController.onControllerChange()");
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
    Map<String, LiveInstance> curSessions = new HashMap<String, LiveInstance>();
    for (LiveInstance liveInstance : liveInstances) {
      curInstances.put(liveInstance.getInstanceName(), liveInstance);
      curSessions.put(liveInstance.getInstanceName() + "|" + liveInstance.getSessionId(),
          liveInstance);
    }

    Map<String, LiveInstance> lastInstances = _lastSeenInstances.get();
    Map<String, LiveInstance> lastSessions = _lastSeenSessions.get();

    HelixManager manager = changeContext.getManager();
    Builder keyBuilder = new Builder(manager.getClusterName());
    if (lastSessions != null) {
      for (String session : lastSessions.keySet()) {
        if (!curSessions.containsKey(session)) {
          // remove current-state listener for expired session
          String instanceName = lastSessions.get(session).getInstanceName();
          SessionId sessionId = lastSessions.get(session).getTypedSessionId();
          manager
              .removeListener(keyBuilder.currentStates(instanceName, sessionId.toString()), this);
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
        SessionId sessionId = curSessions.get(session).getTypedSessionId();
        try {
          // add current-state listeners for new sessions
          manager.addCurrentStateChangeListener(this, instanceName, sessionId.toString());
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

  public void shutdownClusterStatusMonitor(String clusterName) {
    if (_clusterStatusMonitor != null) {
      logger.info("Shut down _clusterStatusMonitor for cluster " + clusterName);
      _clusterStatusMonitor.reset();
      _clusterStatusMonitor = null;
    }
  }

  public void shutdown() throws InterruptedException {
    stopRebalancingTimer();
    while (_eventThread.isAlive()) {
      _eventThread.interrupt();
      _eventThread.join(EVENT_THREAD_JOIN_TIMEOUT);
    }
  }

  private class ClusterEventProcessor extends Thread {
    @Override
    public void run() {
      logger.info("START ClusterEventProcessor thread");
      while (!isInterrupted()) {
        try {
          ClusterEvent event = _eventQueue.take();
          handleEvent(event);
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
}
