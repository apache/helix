package com.linkedin.clustermanager.participant;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.controller.GenericClusterController;

public class DistClusterControllerElection implements ControllerChangeListener
{
  private static Logger LOG = Logger.getLogger(DistClusterControllerElection.class);
  private final static int HISTORY_SIZE = 8;
  private final String _zkAddr;
  private final GenericClusterController _controller = new GenericClusterController();
  private ClusterManager _leader = null;

  public DistClusterControllerElection(String zkAddr)
  {
    _zkAddr = zkAddr;
  }

  /**
   * may be accessed by multiple threads:
   * zk-client thread and ZkClusterManager.disconnect()->reset()
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
          + type.toString() + ", required CONTROLLER | CONTROLLER_PARTICIPANT)");
      return;
    }

    try
    {
      if (changeContext.getType().equals(NotificationContext.Type.INIT)
          || changeContext.getType().equals(NotificationContext.Type.CALLBACK))
      {
        boolean isLeader = tryUpdateController(manager);
        if (isLeader)
        {
          if (type == InstanceType.CONTROLLER)
          {
            ClusterManagerMain.addListenersToController(manager, _controller);
          }
          else if (type == InstanceType.CONTROLLER_PARTICIPANT)
          {
            if (_leader == null)
            {
              String clusterName = manager.getClusterName();
              String controllerName = manager.getInstanceName();
              _leader =
                  ClusterManagerFactory.getZKBasedManagerForController(clusterName,
                                                                       controllerName,
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
//          System.out.println("disconnect " + _leader.getInstanceName() + "("
//                             + _leader.getInstanceType() + ") from "
//                             + _leader.getClusterName());
          _leader.disconnect();
          _leader = null;
        }
      }

    }
    catch (Exception e)
    {
      LOG.error("Exception when trying to become leader, exception:" + e);
    }
  }

  private boolean tryUpdateController(ClusterManager manager)
  {

    String instanceName = manager.getInstanceName();
    String clusterName = manager.getClusterName();
    final ZNRecord leaderRecord = new ZNRecord(PropertyType.LEADER.toString());
    leaderRecord.setSimpleField(CMConstants.ZNAttribute.LEADER.toString(), manager.getInstanceName());
    leaderRecord.setSimpleField(CMConstants.ZNAttribute.CLUSTER_MANAGER_VERSION.toString(), manager.getVersion());
    leaderRecord.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(), manager.getSessionId());

    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    ZNRecord currentleader;
    do
    {
      currentleader = dataAccessor.getProperty(PropertyType.LEADER);
      if (currentleader == null)
      {
        boolean success = dataAccessor.setProperty(PropertyType.LEADER, leaderRecord);

        if (success)
        {
          ZNRecord histRecord = dataAccessor.getProperty(PropertyType.HISTORY);
          // set controller history
          if (histRecord == null)
          {
            histRecord = new ZNRecord(PropertyType.HISTORY.toString());
          }

          List<String> list = histRecord.getListField(clusterName);
          if (list == null)
          {
            list = new ArrayList<String>();
            histRecord.setListField(clusterName, list);
          }

          // record up to HISTORY_SIZE number of leaders in FIFO order
          if (list.size() == HISTORY_SIZE)
          {
            list.remove(0);
          }
          list.add(instanceName);
          dataAccessor.setProperty(PropertyType.HISTORY, histRecord);
          return true;
        }
        else
        {
          LOG.info("Unable to become leader probably some other controller became the leader");
        }
      }
      else
      {
        LOG.info("Leader exists for cluster:" + clusterName + " currentLeader:"
            + currentleader.getId());
      }
    }
    while ((currentleader == null));

    // TODO
    //  read leader property
    //  compare with this.manager.name

    return false;
  }
}
