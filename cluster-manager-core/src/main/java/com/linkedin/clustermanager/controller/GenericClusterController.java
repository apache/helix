package com.linkedin.clustermanager.controller;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.controller.stages.BestPossibleStateCalcStage;
import com.linkedin.clustermanager.controller.stages.ClusterEvent;
import com.linkedin.clustermanager.controller.stages.CurrentStateComputationStage;
import com.linkedin.clustermanager.controller.stages.ExternalViewComputeStage;
import com.linkedin.clustermanager.controller.stages.MessageGenerationPhase;
import com.linkedin.clustermanager.controller.stages.MessageSelectionStage;
import com.linkedin.clustermanager.controller.stages.ReadClusterDataStage;
import com.linkedin.clustermanager.controller.stages.ResourceComputationStage;
import com.linkedin.clustermanager.controller.stages.TaskAssignmentStage;
import com.linkedin.clustermanager.pipeline.Pipeline;
import com.linkedin.clustermanager.pipeline.PipelineRegistry;

/**
 * Cluster Controllers main goal is to keep the cluster state as close as
 * possible to Ideal State. It does this by listening to changes in cluster
 * state and scheduling new tasks to get cluster state to best possible ideal
 * state. Every instance of this class can control can control only one cluster
 * 
 * 
 * // get all the resourceKeys use IdealState, CurrentState and Messages // for
 * each resourceKey // 1. get the (instance,state) from IdealState, CurrentState
 * and PendingMessages // 2. compute best possible state (instance,state) pair.
 * This needs previous step data and state model constraints // compute the
 * messages/tasks needed to move to 1 to 2 // select the messages that can be
 * sent, needs messages and state model constraints // send messages
 * 
 * 
 * //}
 */
public class GenericClusterController implements ConfigChangeListener,
    IdealStateChangeListener, LiveInstanceChangeListener, MessageListener,
    CurrentStateChangeListener, ExternalViewChangeListener
{
  private static final Logger logger = Logger
      .getLogger(GenericClusterController.class.getName());
  volatile boolean init = false;
  private PipelineRegistry _registry;
  private final Set<String> _instanceSubscriptionList;
  private final ExternalViewGenerator _externalViewGenerator;

  public GenericClusterController()
  {
    this(createDefaultRegistry());
  }

  private static PipelineRegistry createDefaultRegistry()
  {
    synchronized (GenericClusterController.class)
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
      rebalancePipeline.addStage(new TaskAssignmentStage());

      // external view generation
      Pipeline externalViewPipeline = new Pipeline();
      externalViewPipeline.addStage(new ExternalViewComputeStage());

      registry.register("idealStateChange", dataRefresh, rebalancePipeline);
      registry.register("currentStateChange", dataRefresh, rebalancePipeline,
          externalViewPipeline);
      registry.register("configChange", dataRefresh, rebalancePipeline);
      registry.register("liveInstanceChange", dataRefresh, rebalancePipeline);

      registry.register("messageChange", dataRefresh);
      registry.register("externalView", dataRefresh);

      return registry;
    }
  }

  public GenericClusterController(PipelineRegistry registry)
  {
    _registry = registry;
    _instanceSubscriptionList = new HashSet<String>();
    _externalViewGenerator = new ExternalViewGenerator();
  }

  protected void handleEvent(ClusterEvent event)
  {
    List<Pipeline> pipelines = _registry.getPipelinesForEvent(event.getName());
    if (pipelines == null || pipelines.size() == 0)
    {
      logger.info("No pipeline to run for event:" + event.getName());
      return;
    }
    for (Pipeline pipeline : pipelines)
    {
      pipeline.handle(event);
      pipeline.finish();
    }
  }

  @Override
  public void onExternalViewChange(List<ZNRecord> externalViewList,
      NotificationContext changeContext)
  {
    ClusterEvent event = new ClusterEvent("externalViewChange");
    event.addAttribute("clustermanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", externalViewList);
    handleEvent(event);
  }

  @Override
  public void onStateChange(String instanceName, List<ZNRecord> statesInfo,
      NotificationContext changeContext)
  {
    logger.info("START:ClusterController.onStateChange()");
    ClusterEvent event = new ClusterEvent("currentStateChange");
    event.addAttribute("clustermanager", changeContext.getManager());
    event.addAttribute("instanceName", instanceName);
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", statesInfo);
    handleEvent(event);
    logger.info("END:ClusterController.onStateChange()");
  }

  @Override
  public void onMessage(String instanceName, List<ZNRecord> messages,
      NotificationContext changeContext)
  {
    logger.info("START:ClusterController.onMessage()");
    ClusterEvent event = new ClusterEvent("messageChange");
    event.addAttribute("clustermanager", changeContext.getManager());
    event.addAttribute("instanceName", instanceName);
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", messages);
    handleEvent(event);
    logger.info("END:ClusterController.onMessage()");
  }

  @Override
  public void onLiveInstanceChange(List<ZNRecord> liveInstances,
      NotificationContext changeContext)
  {
    logger.info("START: ClusterController.onLiveInstanceChange()");
    if (liveInstances == null)
    {
      liveInstances = Collections.emptyList();
    }
    for (ZNRecord instance : liveInstances)
    {
      String instanceName = instance.getId();
      String clientSessionId  = instance.getSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString());
      
      if (!_instanceSubscriptionList.contains(instanceName))
      {
        try
        {
          changeContext.getManager().addCurrentStateChangeListener(this,
              instanceName, clientSessionId);
          changeContext.getManager().addMessageListener(this, instanceName);
        } catch (Exception e)
        {
          logger.error(
              "Exception adding current state and message listener for instance:"
                  + instanceName, e);
        }

        _instanceSubscriptionList.add(instanceName);
      }
    }
    ClusterEvent event = new ClusterEvent("liveInstanceChange");
    event.addAttribute("clustermanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", liveInstances);
    handleEvent(event);
    logger.info("END: ClusterController.onLiveInstanceChange()");

  }

  @Override
  public void onIdealStateChange(List<ZNRecord> idealStates,
      NotificationContext changeContext)
  {
    logger.info("START: ClusterController.onIdealStateChange()");
    ClusterEvent event = new ClusterEvent("idealStateChange");
    event.addAttribute("clustermanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", idealStates);
    handleEvent(event);
    logger.info("END: ClusterController.onIdealStateChange()");
  }

  @Override
  public void onConfigChange(List<ZNRecord> configs,
      NotificationContext changeContext)
  {
    logger.info("START:ClusterController.onConfigChange()");
    ClusterEvent event = new ClusterEvent("configChange");
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("clustermanager", changeContext.getManager());
    event.addAttribute("eventData", configs);
    handleEvent(event);
    logger.info("END:ClusterController.onConfigChange()");
  }

}
