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
package com.linkedin.helix.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigAccessor;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.pipeline.Pipeline;
import com.linkedin.helix.controller.pipeline.PipelineRegistry;
import com.linkedin.helix.controller.stages.BestPossibleStateCalcStage;
import com.linkedin.helix.controller.stages.ClusterEvent;
import com.linkedin.helix.controller.stages.CompatibilityCheckStage;
import com.linkedin.helix.controller.stages.CurrentStateComputationStage;
import com.linkedin.helix.controller.stages.ExternalViewComputeStage;
import com.linkedin.helix.controller.stages.MessageGenerationPhase;
import com.linkedin.helix.controller.stages.MessageSelectionStage;
import com.linkedin.helix.controller.stages.MessageThrottleStage;
import com.linkedin.helix.controller.stages.ReadClusterDataStage;
import com.linkedin.helix.controller.stages.ResourceComputationStage;
import com.linkedin.helix.controller.stages.TaskAssignmentStage;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.PauseSignal;
import com.linkedin.helix.monitoring.mbeans.ClusterStatusMonitor;
import com.linkedin.helix.monitoring.mbeans.HelixMessageQueueMonitor;

/**
 * Cluster Controllers main goal is to keep the cluster state as close as possible to
 * Ideal State. It does this by listening to changes in cluster state and scheduling new
 * tasks to get cluster state to best possible ideal state. Every instance of this class
 * can control can control only one cluster
 *
 *
 * Get all the partitions use IdealState, CurrentState and Messages <br>
 * foreach partition <br>
 * 1. get the (instance,state) from IdealState, CurrentState and PendingMessages <br>
 * 2. compute best possible state (instance,state) pair. This needs previous step data and
 * state model constraints <br>
 * 3. compute the messages/tasks needed to move to 1 to 2 <br>
 * 4. select the messages that can be sent, needs messages and state model constraints <br>
 * 5. send messages
 */
public class GenericHelixController implements
    ConfigChangeListener,
    IdealStateChangeListener,
    LiveInstanceChangeListener,
    MessageListener,
    CurrentStateChangeListener,
    ExternalViewChangeListener,
    ControllerChangeListener,
    HealthStateChangeListener
{
  private static final Logger    logger =
                                            Logger.getLogger(GenericHelixController.class.getName());
  volatile boolean               init   = false;
  private final PipelineRegistry _registry;

  /**
   * Since instance current state is per-session-id, we need to track the session-ids of
   * the current states that the ClusterController is observing. this set contains all the
   * session ids that we add currentState listener
   */
  private final Set<String>      _instanceCurrentStateChangeSubscriptionSessionIds;

  /**
   * this set contains all the instance names that we add message listener
   */
  private final Set<String>      _instanceSubscriptionNames;

  ClusterStatusMonitor           _clusterStatusMonitor;
  

  /**
   * The _paused flag is checked by function handleEvent(), while if the flag is set
   * handleEvent() will be no-op. Other event handling logic keeps the same when the flag
   * is set.
   */
  private boolean                _paused;

  /**
   * The timer that can periodically run the rebalancing pipeline. The timer will start if there
   * is one resource group has the config to use the timer.
   */
  Timer _rebalanceTimer = null;
  int _timerPeriod = Integer.MAX_VALUE;

  /**
   * message queue size monitor mbean
   */
  private HelixMessageQueueMonitor _msgQueueMonitor;
  
  /**
   * Default constructor that creates a default pipeline registry. This is sufficient in
   * most cases, but if there is a some thing specific needed use another constructor
   * where in you can pass a pipeline registry
   */
  public GenericHelixController()
  {
    this(createDefaultRegistry());
  }

  class RebalanceTask extends TimerTask
  {
    HelixManager _manager;
    
    public RebalanceTask(HelixManager manager)
    {
      _manager = manager;
    }
    
    @Override
    public void run()
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      ClusterEvent event = new ClusterEvent("periodicalRebalance");
      event.addAttribute("helixmanager", changeContext.getManager());
      event.addAttribute("changeContext", changeContext);
      List<ZNRecord> dummy = new ArrayList<ZNRecord>();
      event.addAttribute("eventData", dummy);
      // Should be able to process  
      handleEvent(event);
    }
  }
  
  /**
   * Starts the rebalancing timer with the specified period. Start the timer if necessary;
   * If the period is smaller than the current period, cancel the current timer and use 
   * the new period.
   */
  void startRebalancingTimer(int period, HelixManager manager)
  {
    logger.info("Controller starting timer at period " + period);
    if(period < _timerPeriod)
    {
      if(_rebalanceTimer != null)
      {
        _rebalanceTimer.cancel();
      }
      _rebalanceTimer = new Timer(true);
      _timerPeriod = period;
      _rebalanceTimer.scheduleAtFixedRate(new RebalanceTask(manager), _timerPeriod, _timerPeriod);
    }
    else
    {
      logger.info("Controller already has timer at period " + _timerPeriod);
    }
  }
  
  /**
   * Starts the rebalancing timer 
   */
  void stopRebalancingTimer()
  {
    if(_rebalanceTimer != null)
    {
      _rebalanceTimer.cancel();
      _rebalanceTimer = null;
    }
    _timerPeriod = Integer.MAX_VALUE;
  }
  
  private static PipelineRegistry createDefaultRegistry()
  {
    logger.info("createDefaultRegistry");
    synchronized (GenericHelixController.class)
    {
      PipelineRegistry registry = new PipelineRegistry();

      // cluster data cache refresh
      Pipeline dataRefresh = new Pipeline();
      dataRefresh.addStage(new ReadClusterDataStage());

      // rebalance pipeline
      Pipeline rebalancePipeline = new Pipeline();
      rebalancePipeline.addStage(new ResourceComputationStage());
      rebalancePipeline.addStage(new CurrentStateComputationStage());
      rebalancePipeline.addStage(new BestPossibleStateCalcStage());
      rebalancePipeline.addStage(new MessageGenerationPhase());
      rebalancePipeline.addStage(new MessageSelectionStage());
      rebalancePipeline.addStage(new MessageThrottleStage());
      rebalancePipeline.addStage(new TaskAssignmentStage());

      // external view generation
      Pipeline externalViewPipeline = new Pipeline();
      externalViewPipeline.addStage(new ExternalViewComputeStage());

      // backward compatibility check
      Pipeline liveInstancePipeline = new Pipeline();
      liveInstancePipeline.addStage(new CompatibilityCheckStage());

      registry.register("idealStateChange", dataRefresh, rebalancePipeline);
      registry.register("currentStateChange",
                        dataRefresh,
                        rebalancePipeline,
                        externalViewPipeline);
      registry.register("configChange", dataRefresh, rebalancePipeline);
      registry.register("liveInstanceChange",
                        dataRefresh,
                        liveInstancePipeline,
                        rebalancePipeline,
                        externalViewPipeline);

      registry.register("messageChange",
                        dataRefresh,
                        rebalancePipeline);
      registry.register("externalView", dataRefresh);
      registry.register("resume", dataRefresh, rebalancePipeline, externalViewPipeline);
      registry.register("periodicalRebalance", dataRefresh, rebalancePipeline, externalViewPipeline);

      // health stats pipeline
      // Pipeline healthStatsAggregationPipeline = new Pipeline();
      // StatsAggregationStage statsStage = new StatsAggregationStage();
      // healthStatsAggregationPipeline.addStage(new ReadHealthDataStage());
      // healthStatsAggregationPipeline.addStage(statsStage);
      // registry.register("healthChange", healthStatsAggregationPipeline);

      return registry;
    }
  }

  public GenericHelixController(PipelineRegistry registry)
  {
    _paused = false;
    _registry = registry;
    _instanceCurrentStateChangeSubscriptionSessionIds =
        new ConcurrentSkipListSet<String>();
    _instanceSubscriptionNames = new ConcurrentSkipListSet<String>();
    // _externalViewGenerator = new ExternalViewGenerator();
  }

  /**
   * lock-always: caller always needs to obtain an external lock before call, calls to
   * handleEvent() should be serialized
   *
   * @param event
   */
  protected synchronized void handleEvent(ClusterEvent event)
  {
    HelixManager manager = event.getAttribute("helixmanager");
    if (manager == null)
    {
      logger.error("No cluster manager in event:" + event.getName());
      return;
    }

    if (!manager.isLeader())
    {
      logger.error("Cluster manager: " + manager.getInstanceName()
          + " is not leader. Pipeline will not be invoked");
      return;
    }

    if (_paused)
    {
      logger.info("Cluster is paused. Ignoring the event:" + event.getName());
      return;
    }

    NotificationContext context = null;
    if (event.getAttribute("changeContext") != null)
    {
      context = (NotificationContext) (event.getAttribute("changeContext"));
    }

    // Initialize _clusterStatusMonitor
    if (context != null)
    {
      if (context.getType() == Type.FINALIZE)
      {
        if (_clusterStatusMonitor != null)
        {
          _clusterStatusMonitor.reset();
          _clusterStatusMonitor = null;
        }
        
        if (_msgQueueMonitor != null)
        {
          _msgQueueMonitor.reset();
          _msgQueueMonitor = null;
        }
        
        stopRebalancingTimer();
        logger.info("Get FINALIZE notification, skip the pipeline. Event :" + event.getName());
        return;
      }
      else
      {
        if (_clusterStatusMonitor == null)
        {
          _clusterStatusMonitor = new ClusterStatusMonitor(manager.getClusterName());
        }
        
        if (_msgQueueMonitor == null)
        {
          _msgQueueMonitor = new HelixMessageQueueMonitor(manager.getClusterName());
        }
        
        event.addAttribute("clusterStatusMonitor", _clusterStatusMonitor);
      }
    }

    List<Pipeline> pipelines = _registry.getPipelinesForEvent(event.getName());
    if (pipelines == null || pipelines.size() == 0)
    {
      logger.info("No pipeline to run for event:" + event.getName());
      return;
    }

    for (Pipeline pipeline : pipelines)
    {
      try
      {
        pipeline.handle(event);
        pipeline.finish();
      }
      catch (Exception e)
      {
        logger.error("Exception while executing pipeline: " + pipeline
            + ". Will not continue to next pipeline", e);
        break;
      }
    }
  }

  // TODO since we read data in pipeline, we can get rid of reading from zookeeper in
  // callback

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
                                   NotificationContext changeContext)
  {
//    logger.info("START: GenericClusterController.onExternalViewChange()");
//    ClusterEvent event = new ClusterEvent("externalViewChange");
//    event.addAttribute("helixmanager", changeContext.getManager());
//    event.addAttribute("changeContext", changeContext);
//    event.addAttribute("eventData", externalViewList);
//    // handleEvent(event);
//    logger.info("END: GenericClusterController.onExternalViewChange()");
  }

  @Override
  public void onStateChange(String instanceName,
                            List<CurrentState> statesInfo,
                            NotificationContext changeContext)
  {
    logger.info("START: GenericClusterController.onStateChange()");
    ClusterEvent event = new ClusterEvent("currentStateChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("instanceName", instanceName);
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", statesInfo);
    handleEvent(event);
    logger.info("END: GenericClusterController.onStateChange()");
  }

  @Override
  public void onHealthChange(String instanceName,
                             List<HealthStat> reports,
                             NotificationContext changeContext)
  {
    /**
     * When there are more participant ( > 20, can be in hundreds), This callback can be
     * called quite frequently as each participant reports health stat every minute. Thus
     * we change the health check pipeline to run in a timer callback.
     */
  }

  @Override
  public void onMessage(String instanceName,
                        List<Message> messages,
                        NotificationContext changeContext)
  {
    logger.info("START: GenericClusterController.onMessage()");
    
    if (_msgQueueMonitor != null)
    {
      _msgQueueMonitor.addMessageQueueSize(messages.size());
    }
    
    ClusterEvent event = new ClusterEvent("messageChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("instanceName", instanceName);
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", messages);
    handleEvent(event);
    logger.info("END: GenericClusterController.onMessage()");
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
                                   NotificationContext changeContext)
  {
    logger.info("START: Generic GenericClusterController.onLiveInstanceChange()");
    if (liveInstances == null)
    {
      liveInstances = Collections.emptyList();
    }
    // Go though the live instance list and make sure that we are observing them
    // accordingly. The action is done regardless of the paused flag.
    if (changeContext.getType() == NotificationContext.Type.INIT ||
        changeContext.getType() == NotificationContext.Type.CALLBACK)
    {
      checkLiveInstancesObservation(liveInstances, changeContext);
    }

    ClusterEvent event = new ClusterEvent("liveInstanceChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", liveInstances);
    handleEvent(event);
    logger.info("END: Generic GenericClusterController.onLiveInstanceChange()");
  }
  
  void checkRebalancingTimer(HelixManager manager, List<IdealState> idealStates)
  {
    if (manager.getConfigAccessor() == null)
    {
      logger.warn(manager.getInstanceName() + " config accessor doesn't exist. should be in file-based mode.");
      return;
    }
    
    for(IdealState idealState : idealStates)
    {
      int period = idealState.getRebalanceTimerPeriod();
      if(period > 0)
      {
        startRebalancingTimer(period, manager);
      }
    }
  }
  
  @Override
  public void onIdealStateChange(List<IdealState> idealStates,
                                 NotificationContext changeContext)
  {
    logger.info("START: Generic GenericClusterController.onIdealStateChange()");
    ClusterEvent event = new ClusterEvent("idealStateChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", idealStates);
    handleEvent(event);
    
    if(changeContext.getType() != Type.FINALIZE)
    {
      checkRebalancingTimer(changeContext.getManager(), idealStates);
    }
    
    logger.info("END: Generic GenericClusterController.onIdealStateChange()");
  }

  @Override
  public void onConfigChange(List<InstanceConfig> configs,
                             NotificationContext changeContext)
  {
    logger.info("START: GenericClusterController.onConfigChange()");
    ClusterEvent event = new ClusterEvent("configChange");
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("eventData", configs);
    handleEvent(event);
    logger.info("END: GenericClusterController.onConfigChange()");
  }

  @Override
  public void onControllerChange(NotificationContext changeContext)
  {
    logger.info("START: GenericClusterController.onControllerChange()");
    HelixDataAccessor accessor = changeContext.getManager().getHelixDataAccessor();

    // double check if this controller is the leader
    Builder keyBuilder = accessor.keyBuilder();
    LiveInstance leader =
        accessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null)
    {
      logger.warn("No controller exists for cluster:"
          + changeContext.getManager().getClusterName());
      return;
    }
    else
    {
      String leaderName = leader.getInstanceName();

      String instanceName = changeContext.getManager().getInstanceName();
      if (leaderName == null || !leaderName.equals(instanceName))
      {
        logger.warn("leader name does NOT match, my name: " + instanceName + ", leader: "
            + leader);
        return;
      }
    }

    PauseSignal pauseSignal = accessor.getProperty(keyBuilder.pause());
    if (pauseSignal != null)
    {
      _paused = true;
      logger.info("controller is now paused");
    }
    else
    {
      if (_paused)
      {
        // it currently paused
        logger.info("controller is now resumed");
        _paused = false;
        ClusterEvent event = new ClusterEvent("resume");
        event.addAttribute("changeContext", changeContext);
        event.addAttribute("helixmanager", changeContext.getManager());
        event.addAttribute("eventData", pauseSignal);
        handleEvent(event);
      }
      else
      {
        _paused = false;
      }
    }
    logger.info("END: GenericClusterController.onControllerChange()");
  }

  /**
   * Go through the list of liveinstances in the cluster, and add currentstateChange
   * listener and Message listeners to them if they are newly added. For current state
   * change, the observation is tied to the session id of each live instance.
   *
   */
  protected void checkLiveInstancesObservation(List<LiveInstance> liveInstances,
                                               NotificationContext changeContext)
  {
    for (LiveInstance instance : liveInstances)
    {
      String instanceName = instance.getId();
      String clientSessionId = instance.getSessionId();
      HelixManager manager = changeContext.getManager();

      // _instanceCurrentStateChangeSubscriptionSessionIds contains all the sessionIds
      // that we've added a currentState listener
      if (!_instanceCurrentStateChangeSubscriptionSessionIds.contains(clientSessionId))
      {
        try
        {
          manager.addCurrentStateChangeListener(this, instanceName, clientSessionId);
          _instanceCurrentStateChangeSubscriptionSessionIds.add(clientSessionId);
          logger.info("Observing client session id: " + clientSessionId);
        }
        catch (Exception e)
        {
          logger.error("Exception adding current state and message listener for instance:"
                           + instanceName,
                       e);
        }
      }

      // _instanceSubscriptionNames contains all the instanceNames that we've added a
      // message listener
      if (!_instanceSubscriptionNames.contains(instanceName))
      {
        try
        {
          logger.info("Adding message listener for " + instanceName);
          manager.addMessageListener(this, instanceName);
          _instanceSubscriptionNames.add(instanceName);
        }
        catch (Exception e)
        {
          logger.error("Exception adding message listener for instance:" + instanceName,
                       e);
        }
      }

      // TODO we need to remove currentState listeners and message listeners
      // when a session or an instance no longer exists. This may happen
      // in case of session expiry, participant rebound, participant goes and new
      // participant comes

      // TODO shi should call removeListener on the previous session id;
      // but the removeListener with that functionality is not implemented yet
    }
  }

}
