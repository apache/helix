package com.linkedin.clustermanager.participant;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelInfo;
import com.linkedin.clustermanager.participant.statemachine.Transition;

// TODO: change to use leader/standby state model
@StateModelInfo(initialState = "OFFLINE", states = { "SLAVE", "MASTER" })
public class DistClusterControllerStateModel extends StateModel 
{
  private static Logger LOG = Logger.getLogger(DistClusterControllerStateModel.class);
  private ClusterManager _managerForCntrl;
  private final String _zkAddr;
  
  public DistClusterControllerStateModel(String zkAddr)
  {
    _zkAddr = zkAddr;
  }
  
  @Transition(to="SLAVE",from="OFFLINE")
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context)
  {
    LOG.info("Becoming standby from offline");
  }
  
  @Transition(to="MASTER",from="SLAVE")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
  {
    LOG.info("Becoming leader from standby");
    String clusterName = message.getStateUnitKey();

    /**
    try
    {
      _managerForCntrl 
        = ClusterManagerFactory.getZKBasedManagerForController(clusterName, _zkAddr);
      
      final GenericClusterController controller = new GenericClusterController();
      _managerForCntrl.addConfigChangeListener(controller);
      _managerForCntrl.addLiveInstanceChangeListener(controller);
      _managerForCntrl.addIdealStateChangeListener(controller);
      _managerForCntrl.addExternalViewChangeListener(controller);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    **/
  }
  
  @Transition(to="SLAVE",from="MASTER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context)
  {
    LOG.info("Becoming standby from leader");
  }

  @Transition(to="OFFLINE",from="SLAVE")
  public void onBecomeOfflineromStandby(Message message, NotificationContext context)
  {
    LOG.info("Becoming offline from standby");
  }

}
