package com.linkedin.clustermanager.controller;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.ControllerChangeListener;
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
 * Get all the resourceKeys use IdealState, CurrentState and Messages <br>
 * foreach resourceKey <br>
 * 1. get the (instance,state) from IdealState, CurrentState and PendingMessages <br>
 * 2. compute best possible state (instance,state) pair. This needs previous
 * step data and state model constraints <br>
 * 3. compute the messages/tasks needed to move to 1 to 2 <br>
 * 4. select the messages that can be sent, needs messages and state model
 * constraints <br>
 * 5. send messages
 */
public class GenericClusterController implements ConfigChangeListener,
    IdealStateChangeListener, LiveInstanceChangeListener, MessageListener,
    CurrentStateChangeListener, ExternalViewChangeListener,
    ControllerChangeListener
{
	private static final Logger logger = Logger
	    .getLogger(GenericClusterController.class.getName());
	volatile boolean init = false;
	private PipelineRegistry _registry;
	
	/** 
	 * Since instance current state is per-session-id, we need to track the session-ids of 
	 * the current states that the ClusterController is observing.
	 */
	private final Set<String> _instanceCurrentStateChangeSubscriptionList;
	private final ExternalViewGenerator _externalViewGenerator;
	
	/**
	 * The _paused flag is checked by function handleEvent(), while if the flag is set handleEvent()
	 * will be no-op. Other event handling logic keeps the same when the flag is set. 
	 */
	private boolean _paused;

  /**
   * Default constructor that creates a default pipeline registry.
   * This is sufficient in most cases, but if there is a some thing specific needed use another constructor where in you can pass a pipeline registry
   */
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
			registry.register("liveInstanceChange", dataRefresh, rebalancePipeline,
			    externalViewPipeline);

			registry.register("messageChange", dataRefresh);
			registry.register("externalView", dataRefresh);
			registry.register("resume", dataRefresh, rebalancePipeline,
			    externalViewPipeline);

			return registry;
		}
	}

	public GenericClusterController(PipelineRegistry registry)
	{
		_paused = false;
		_registry = registry;
		_instanceCurrentStateChangeSubscriptionList = new HashSet<String>();
		_externalViewGenerator = new ExternalViewGenerator();
	}

	protected void handleEvent(ClusterEvent event)
	{
		if (_paused)
		{
			logger.info("Cluster is paused. Ignoring the event:" + event.getName());
			return;
		}
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
		// handleEvent(event);
	}

	@Override
	public void onStateChange(String instanceName, List<ZNRecord> statesInfo,
	    NotificationContext changeContext)
	{
		logger.info("START: GenericClusterController.onStateChange()");
		ClusterEvent event = new ClusterEvent("currentStateChange");
		event.addAttribute("clustermanager", changeContext.getManager());
		event.addAttribute("instanceName", instanceName);
		event.addAttribute("changeContext", changeContext);
		event.addAttribute("eventData", statesInfo);
		handleEvent(event);
		logger.info("END: GenericClusterController.onStateChange()");
	}

	@Override
	public void onMessage(String instanceName, List<ZNRecord> messages,
	    NotificationContext changeContext)
	{
		logger.info("START: GenericClusterController.onMessage()");
		ClusterEvent event = new ClusterEvent("messageChange");
		event.addAttribute("clustermanager", changeContext.getManager());
		event.addAttribute("instanceName", instanceName);
		event.addAttribute("changeContext", changeContext);
		event.addAttribute("eventData", messages);
		handleEvent(event);
		logger.info("END: GenericClusterController.onMessage()");
	}

	@Override
	public void onLiveInstanceChange(List<ZNRecord> liveInstances,
	    NotificationContext changeContext)
	{
		logger
		    .info("START: Generic GenericClusterController.onLiveInstanceChange()");
		if (liveInstances == null)
		{
			liveInstances = Collections.emptyList();
		}
		// Go though the live instance list and make sure that we are observing them
		// accordingly. The action is done regardless of the paused flag.
		checkLiveInstancesObservation(liveInstances, changeContext);
		
		ClusterEvent event = new ClusterEvent("liveInstanceChange");
		event.addAttribute("clustermanager", changeContext.getManager());
		event.addAttribute("changeContext", changeContext);
		event.addAttribute("eventData", liveInstances);
		handleEvent(event);
		logger.info("END: Generic GenericClusterController.onLiveInstanceChange()");
	}

	@Override
	public void onIdealStateChange(List<ZNRecord> idealStates,
	    NotificationContext changeContext)
	{
		logger.info("START: Generic GenericClusterController.onIdealStateChange()");
		ClusterEvent event = new ClusterEvent("idealStateChange");
		event.addAttribute("clustermanager", changeContext.getManager());
		event.addAttribute("changeContext", changeContext);
		event.addAttribute("eventData", idealStates);
		handleEvent(event);
		logger.info("END: Generic GenericClusterController.onIdealStateChange()");
	}

	@Override
	public void onConfigChange(List<ZNRecord> configs,
	    NotificationContext changeContext)
	{
		logger.info("START: GenericClusterController.onConfigChange()");
		ClusterEvent event = new ClusterEvent("configChange");
		event.addAttribute("changeContext", changeContext);
		event.addAttribute("clustermanager", changeContext.getManager());
		event.addAttribute("eventData", configs);
		handleEvent(event);
		logger.info("END: GenericClusterController.onConfigChange()");
	}

	@Override
	public void onControllerChange(NotificationContext changeContext)
	{
		ClusterDataAccessor dataAccessor = changeContext.getManager()
		    .getDataAccessor();
		
		// double check if this controller is the leader
		ZNRecord leaderRecord = dataAccessor
		    .getControllerProperty(ControllerPropertyType.LEADER);
		if (leaderRecord == null)
		{
		  logger.warn("No controller exists for cluster:" + 
		      changeContext.getManager().getClusterName());
		  return;
		}
		else
		{
  		  String leader = leaderRecord
  		    .getSimpleField(ControllerPropertyType.LEADER.toString());
  		  String name = changeContext.getManager().getInstanceName();
  		  if (leader == null || !leader.equals(name))
  		  {
  		    logger.warn("leader name does NOT match, my name:" + name + 
  		                ", leader:" + leader);
  		    return;
  		  }
		}
		
		ZNRecord pauseSignal = dataAccessor
		    .getControllerProperty(ControllerPropertyType.PAUSE);
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
				event.addAttribute("clustermanager", changeContext.getManager());
				event.addAttribute("eventData", pauseSignal);
				handleEvent(event);
			} else
			{
				_paused = false;
			}
		}
	}
	
	/**
	 * Go through the list of liveinstances in the cluster, and add currentstateChange listener
	 * and Message listeners to them if they are newly added. For current state change, the 
	 * observation is tied to the session id of each live instance.
	 *
	 */
	protected void checkLiveInstancesObservation(List<ZNRecord> liveInstances,
      NotificationContext changeContext)
	{
	  for (ZNRecord instance : liveInstances)
    {
      String instanceName = instance.getId();
      String clientSessionId = instance
          .getSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString());

      if (!_instanceCurrentStateChangeSubscriptionList.contains(clientSessionId))
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
        _instanceCurrentStateChangeSubscriptionList.add(clientSessionId);
      }
      // TODO shi call removeListener
    }
	}
}
