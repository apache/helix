package com.linkedin.helix.participant;

import org.apache.log4j.Logger;

import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.controller.GenericHelixController;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.model.LeaderHistory;
import com.linkedin.helix.model.LiveInstance;

public class DistClusterControllerElection implements ControllerChangeListener
{
  private static Logger LOG = Logger.getLogger(DistClusterControllerElection.class);
  private final String _zkAddr;
  private final GenericHelixController _controller = new GenericHelixController();
  private HelixManager _leader;

  public DistClusterControllerElection(String zkAddr)
  {
    _zkAddr = zkAddr;
  }

  /**
   * may be accessed by multiple threads: zk-client thread and
   * ZkHelixManager.disconnect()->reset()
   *TODO: Refactor accessing HelixMaangerMain class statically
   */
  @Override
  public synchronized void onControllerChange(NotificationContext changeContext)
  {
    HelixManager manager = changeContext.getManager();
    if (manager == null)
    {
      LOG.error("missing attributes in changeContext. requires HelixManager");
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
        DataAccessor dataAccessor = manager.getDataAccessor();
        while (dataAccessor.getProperty(PropertyType.LEADER) == null)
        {
          boolean success = tryUpdateController(manager);
          if (success)
          {
            updateHistory(manager);
            if (type == InstanceType.CONTROLLER)
            {
              HelixControllerMain.addListenersToController(manager, _controller);
            }
            else if (type == InstanceType.CONTROLLER_PARTICIPANT)
            {
              String clusterName = manager.getClusterName();
              String controllerName = manager.getInstanceName();
              _leader =
                  HelixManagerFactory.getZKHelixManager(clusterName,
                                                            controllerName,
                                                            InstanceType.CONTROLLER,
                                                            _zkAddr);

              _leader.connect();
              HelixControllerMain.addListenersToController(_leader, _controller);
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

  private boolean tryUpdateController(HelixManager manager)
  {
    DataAccessor dataAccessor = manager.getDataAccessor();
    LiveInstance leader = new LiveInstance(PropertyType.LEADER.toString());
    try
    {
      leader.setLeader(manager.getInstanceName());
      // TODO: this session id is not the leader's session id in distributed mode
      leader.setSessionId(manager.getSessionId());
      leader.setHelixVersion(manager.getVersion());
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

  private void updateHistory(HelixManager manager)
  {
    DataAccessor dataAccessor = manager.getDataAccessor();

    LeaderHistory history = dataAccessor.getProperty(LeaderHistory.class, PropertyType.HISTORY);
    if (history == null)
    {
      history = new LeaderHistory(PropertyType.HISTORY.toString());
    }
    history.updateHistory(manager.getClusterName(), manager.getInstanceName());
    dataAccessor.setProperty(PropertyType.HISTORY, history);
  }
}
