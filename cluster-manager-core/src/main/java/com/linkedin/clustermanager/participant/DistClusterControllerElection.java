package com.linkedin.clustermanager.participant;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
  private ClusterManager _leader;
  private final AtomicBoolean _isLeader = new AtomicBoolean(false);

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
        boolean success = tryUpdateController(manager);
        if (success)
        {
          if (_isLeader.compareAndSet(false, true))
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
                  ClusterManagerFactory.getZKBasedManagerForController(clusterName,
                                                                       controllerName,
                                                                       _zkAddr);
              _leader.connect();
              ClusterManagerMain.addListenersToController(_leader, _controller);
            }
          }
        }
        else
        {
          if (_isLeader.get() == true)
          {
            LOG.error("_isLeader should NOT be true");
          }
        }
      }
      else if (changeContext.getType().equals(NotificationContext.Type.FINALIZE))
      {
        if (_isLeader.compareAndSet(true, false))
        {
          if (type == InstanceType.CONTROLLER_PARTICIPANT)
          {
//          System.out.println("disconnect " + _leader.getInstanceName() + "("
//                             + _leader.getInstanceType() + ") from "
//                             + _leader.getClusterName());
            _leader.disconnect();
          }
        }
      }

    }
    catch (Exception e)
    {
      LOG.error("Exception when trying to become leader.", e);
    }
  }

  private boolean tryUpdateController(ClusterManager manager)
  {
    ZNRecord currentLeader = null;
    try
    {
      ClusterDataAccessor dataAccessor = manager.getDataAccessor();
      currentLeader = dataAccessor.getProperty(PropertyType.LEADER);
      while (currentLeader == null)
      {
        currentLeader = new ZNRecord(PropertyType.LEADER.toString());
        currentLeader.setSimpleField(CMConstants.ZNAttribute.LEADER.toString(), manager.getInstanceName());
        currentLeader.setSimpleField(CMConstants.ZNAttribute.CLUSTER_MANAGER_VERSION.toString(), manager.getVersion());
        currentLeader.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(), manager.getSessionId());
        boolean success = dataAccessor.setProperty(PropertyType.LEADER, currentLeader);
        if (success)
        {
          return true;
        }
        else
        {
          LOG.info("Unable to become leader probably because some other controller becames the leader");
        }
        currentLeader = dataAccessor.getProperty(PropertyType.LEADER);
      }
    }
    catch (Exception e)
    {
      LOG.error("Exception when trying to updating leader record in cluster:" + manager.getClusterName()
              + ". Need to double-check whether leader node has been created or not");
    }

    String leaderName = (currentLeader == null? null : currentLeader.getSimpleField(CMConstants.ZNAttribute.LEADER.toString()));
    LOG.info("Leader exists for cluster:" + manager.getClusterName() + ", currentLeader:" + leaderName);

    if (leaderName != null && leaderName.equals(manager.getInstanceName()))
    {
      return true;
    }

    return false;
  }

  private void updateHistory(ClusterManager manager)
  {
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();

    ZNRecord histRecord = dataAccessor.getProperty(PropertyType.HISTORY);
    if (histRecord == null)
    {
      histRecord = new ZNRecord(PropertyType.HISTORY.toString());
    }

    List<String> list = histRecord.getListField(manager.getClusterName());
    if (list == null)
    {
      list = new ArrayList<String>();
      histRecord.setListField(manager.getClusterName(), list);
    }

    // record up to HISTORY_SIZE number of leaders in FIFO order
    if (list.size() == HISTORY_SIZE)
    {
      list.remove(0);
    }
    list.add(manager.getInstanceName());
    dataAccessor.setProperty(PropertyType.HISTORY, histRecord);
  }
}
