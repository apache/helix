package com.linkedin.clustermanager.participant;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.controller.GenericClusterController;

public class DistClusterControllerElection implements ControllerChangeListener
{
  /**
  private final String _zkAddr;
  private ClusterManager _controller = null;
  
  public LeaderElection(String zkAddr)
  {
    _zkAddr = zkAddr;
  }
  **/
  
  private void doLeaderElection(ClusterManager manager) 
  throws Exception
  {
    boolean isLeader = manager.tryUpdateController();
    if (isLeader)
    {
      // String clusterName = manager.getClusterName();
      // _controller = ClusterManagerFactory.getZKBasedManagerForController(clusterName, 
      //                                                                   _zkAddr);
      
      final GenericClusterController controller = new GenericClusterController();
      manager.addConfigChangeListener(controller);
      manager.addLiveInstanceChangeListener(controller);
      manager.addIdealStateChangeListener(controller);
      manager.addExternalViewChangeListener(controller);
    }
  }
  
  @Override
  public void onControllerChange(NotificationContext changeContext)
  {
    // TODO Auto-generated method stub
    ClusterManager manager = changeContext.getManager();
    try
    {
      doLeaderElection(manager);
    } 
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
