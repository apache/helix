package com.linkedin.clustermanager.participant;

import java.util.concurrent.ConcurrentHashMap;

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
  private static Logger LOG = Logger.getLogger(DistClusterControllerStateModel.class);
  private ConcurrentHashMap<String, ClusterManager> _controllerMap 
    = new ConcurrentHashMap<String, ClusterManager>();
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
    LOG.info("Becoming standby from offline");
  }
  
  
  @Transition(to="LEADER",from="STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
  throws Exception
  {
    LOG.info("Becoming leader from standby");
    String clusterName = message.getStateUnitKey();
    String controllerName = message.getTgtName();
 
    ClusterManager manager  
      = ClusterManagerFactory.getZKBasedManagerForController(clusterName, controllerName, _zkAddr);
    _controllerMap.put(clusterName, manager);
    
    DistClusterControllerElection leaderElection = new DistClusterControllerElection();
    manager.addControllerListener(leaderElection);
    context.add(clusterName, leaderElection.getController());
    // manager.connect();
  }
  
  @Transition(to="STANDBY",from="LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context)
  {
    LOG.info("Becoming standby from leader");
  }

  
  @Transition(to="OFFLINE",from="SLAVE")
  public void onBecomeOfflineromStandby(Message message, NotificationContext context)
  {
    LOG.info("Becoming offline from standby");
  }
  
  @Override
  public void rollbackOnError(Message message, NotificationContext context,
                              StateTransitionError error)
  {
    String clusterName = message.getStateUnitKey();
    LOG.error("rollback on error, clusterName:" + clusterName);
    
    ClusterManager manager = _controllerMap.remove(clusterName);
    if (manager != null)
    {
        // do clean
      GenericClusterController listener = (GenericClusterController)context.get(clusterName);
      if (listener != null)
      {
        manager.removeListener(listener);
      }
    }

  }

}
