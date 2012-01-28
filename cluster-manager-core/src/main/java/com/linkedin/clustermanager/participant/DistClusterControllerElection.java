package com.linkedin.clustermanager.participant;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.controller.GenericClusterController;
import com.linkedin.clustermanager.model.LeaderHistory;
import com.linkedin.clustermanager.model.LiveInstance;

public class DistClusterControllerElection implements ControllerChangeListener
{
  private static Logger LOG = Logger.getLogger(DistClusterControllerElection.class);
  private final String _zkAddr;
  private final GenericClusterController _controller = new GenericClusterController();
  private ClusterManager _leader;

  public DistClusterControllerElection(String zkAddr)
  {
    _zkAddr = zkAddr;
  }

  /**
   * may be accessed by multiple threads: zk-client thread and
   * ZkClusterManager.disconnect()->reset()
   */
  @Override
  public synchronized void onControllerChange(NotificationContext changeContext)
  {
    ClusterManager manager = changeContext.getManager();
    if (manager == null)
    {
      LOG.error("missing attributes in changeContext. requires ClusterManager");
      return;
    }

    InstanceType type = manager.getInstanceType();
    if (type != InstanceType.CONTROLLER && type != InstanceType.CONTROLLER_PARTICIPANT)
    {
      LOG.error("fail to become controller because incorrect instanceType (was "
          + type.toString() + ", requires CONTROLLER | CONTROLLER_PARTICIPANT)");
      return;
    }

    try
    {
      if (changeContext.getType().equals(NotificationContext.Type.INIT)
          || changeContext.getType().equals(NotificationContext.Type.CALLBACK))
      {
        ClusterDataAccessor dataAccessor = manager.getDataAccessor();
        while (dataAccessor.getProperty(PropertyType.LEADER) == null)
        {
          boolean success = tryUpdateController(manager);
          if (success)
          {
            updateHistory(manager);
            if (type == InstanceType.CONTROLLER)
            {
              ClusterManagerMain.addListenersToController(manager, _controller);
            }
            else if (type == InstanceType.CONTROLLER_PARTICIPANT)
            {
              String clusterName = manager.getClusterName();
              String controllerName = manager.getInstanceName();
              _leader =
                  ClusterManagerFactory.getZKClusterManager(clusterName,
                                                            controllerName,
                                                            InstanceType.CONTROLLER,
                                                            _zkAddr);

              _leader.connect();
              ClusterManagerMain.addListenersToController(_leader, _controller);
            }

          }
        }
      }
      else if (changeContext.getType().equals(NotificationContext.Type.FINALIZE))
      {

        if (_leader != null)
        {
          _leader.disconnect();
        }
      }

    }
    catch (Exception e)
    {
      LOG.error("Exception when trying to become leader", e);
    }
  }

  private boolean tryUpdateController(ClusterManager manager)
  {
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    LiveInstance leader = new LiveInstance(PropertyType.LEADER.toString());
    try
    {
      leader.setLeader(manager.getInstanceName());
      leader.setSessionId(manager.getSessionId());
      leader.setClusterManagerVersion(manager.getVersion());
      boolean success = dataAccessor.setProperty(PropertyType.LEADER, leader);
      if (success)
      {
        return true;
      }
      else
      {
        LOG.info("Unable to become leader probably because some other controller becames the leader");
      }
    }
    catch (Exception e)
    {
      LOG.error("Exception when trying to updating leader record in cluster:"
          + manager.getClusterName()
          + ". Need to check again whether leader node has been created or not");
    }
    leader = dataAccessor.getProperty(LiveInstance.class, PropertyType.LEADER);
    if (leader != null)
    {
      String leaderName = leader.getLeader();
      LOG.info("Leader exists for cluster:" + manager.getClusterName() + ", currentLeader:"
          + leaderName);

      if (leaderName != null && leaderName.equals(manager.getInstanceName()))
      {
        return true;
      }
    }

    return false;
  }

  private void updateHistory(ClusterManager manager)
  {
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();

    LeaderHistory history = dataAccessor.getProperty(LeaderHistory.class, PropertyType.HISTORY);
    if (history == null)
    {
      history = new LeaderHistory(PropertyType.HISTORY.toString());
    }
    history.updateHistory(manager.getClusterName(), manager.getInstanceName());
    dataAccessor.setProperty(PropertyType.HISTORY, history);
  }
}
