package com.linkedin.clustermanager.participant;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.controller.GenericClusterController;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelInfo;
import com.linkedin.clustermanager.participant.statemachine.StateModelParser;
import com.linkedin.clustermanager.participant.statemachine.StateTransitionError;
import com.linkedin.clustermanager.participant.statemachine.Transition;


@StateModelInfo(initialState = "OFFLINE", states = { "LEADER", "STANDBY" })
public class DistClusterControllerStateModel extends StateModel 
{
  private static Logger logger = Logger.getLogger(DistClusterControllerStateModel.class);
  // private final ConcurrentHashMap<String, ClusterManager> _controllers 
  //          = new ConcurrentHashMap<String, ClusterManager>();
  private ClusterManager _controller = null;
  private final String _zkAddr;
  
  public DistClusterControllerStateModel(String zkAddr)
  {
    StateModelParser parser = new StateModelParser();
    _currentState = parser.getInitialState(DistClusterControllerStateModel.class);
    _zkAddr = zkAddr;
  }
  
  @Transition(to="STANDBY",from="OFFLINE")
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context)
  {
    logger.info("Becoming standby from offline");
  }
  
  @Transition(to="LEADER",from="STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
  throws Exception
  {
    String clusterName = message.getStateUnitKey();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becomes leader from standby for " + clusterName);
    // System.out.println(controllerName + " becomes leader from standby for " + clusterName);
 
    // ClusterManager manager 
    if (_controller == null)
    {
      _controller = ClusterManagerFactory.getZKBasedManagerForController(clusterName, controllerName, _zkAddr);
      // manager.connect();
      // _controllers.put(controllerName, manager);
      
      _controller.connect();
    
    
      DistClusterControllerElection leaderElection = new DistClusterControllerElection(_zkAddr);
      // TODO need sync
      _controller.addControllerListener(leaderElection);
      context.add(clusterName, leaderElection.getController());
    }
    else
    {
      logger.error("a controller already exists:" + _controller.getInstanceName() 
                   + " for " + clusterName);
    }
    
  }
  
  @Transition(to="STANDBY",from="LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context)
  {
    String clusterName = message.getStateUnitKey();
    String controllerName = message.getTgtName();
    
    logger.info(controllerName + " becoming standby from leader for " + clusterName);
    // System.out.println(controllerName + " becoming standby from leader for " + clusterName);
    // ClusterManager manager = _controllers.remove(controllerName);
  
    if (_controller != null)
    {
      _controller.disconnect();
      _controller = null;
    }
    else
    {
      logger.error("a controller doesn't exist:" + _controller.getInstanceName() 
                   + " for " + clusterName);
    }
  }

  @Transition(to="OFFLINE",from="STANDBY")
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context)
  {
    String clusterName = message.getStateUnitKey();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming offline from standby for cluster:" + clusterName);
    
    // ClusterManager manager = _controllers.remove(controllerName);
    // manager.disconnect();
  }
  
  @Transition(to="DROPPED",from="OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
  {
    logger.info("Becoming dropped from offline");
  }
  
  @Transition(to="OFFLINE",from="DROPPED")
  public void onBecomeOfflineFromDropped(Message message, NotificationContext context)
  {
    logger.info("Becoming offline from dropped");
  }

  
  @Override
  public void rollbackOnError(Message message, NotificationContext context,
                              StateTransitionError error)
  {
    String clusterName = message.getStateUnitKey();
    String controllerName = message.getTgtName();
    
    logger.error(controllerName + " rollbacks on error for cluster:" + clusterName);
    
    // ClusterManager manager = _controllers.remove(controllerName);
    if (_controller != null)
    {
      // do clean
      GenericClusterController listener = (GenericClusterController)context.get(clusterName);
      if (listener != null)
      {
        // TODO need sync
        _controller.removeListener(listener);
      }
      _controller.disconnect();
    }

  }
  
  public ClusterManager getController()
  {
    return _controller;
  }

}
