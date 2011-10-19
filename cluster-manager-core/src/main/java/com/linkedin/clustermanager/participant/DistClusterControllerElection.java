package com.linkedin.clustermanager.participant;

import java.util.List;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.controller.GenericClusterController;

public class DistClusterControllerElection implements ControllerChangeListener
{
  private static Logger LOG = Logger.getLogger(DistClusterControllerElection.class);
  private final String _zkAddr;
  private GenericClusterController _controller = null;
  private ClusterManager _leader = null;

  public DistClusterControllerElection(String zkAddr)
  {
    _zkAddr = zkAddr;
  }

  public GenericClusterController getController()
  {
    return _controller;
  }

  public ClusterManager getLeader()
  {
    return _leader;
  }

  @Override
  public void onControllerChange(NotificationContext changeContext)
  {
    ClusterManager manager = changeContext.getManager();
    try
    {
      if (changeContext.getType().equals(NotificationContext.Type.INIT)
          || changeContext.getType().equals(NotificationContext.Type.CALLBACK))
      {
        boolean isLeader = tryUpdateController(manager);
        if (isLeader)
        {
          if (_controller == null)
          {
            _controller = new GenericClusterController();
            InstanceType type = manager.getInstanceType();
            if (type == InstanceType.CONTROLLER)
            {
              ClusterManagerMain.addListenersToController(manager, _controller);
            } else if (type == InstanceType.CONTROLLER_PARTICIPANT)
            {
              String clusterName = manager.getClusterName();
              String controllerName = manager.getInstanceName();
              _leader =
                  ClusterManagerFactory.getZKBasedManagerForController(clusterName,
                                                                       controllerName,
                                                                       _zkAddr);
              _leader.connect();
              ClusterManagerMain.addListenersToController(_leader, _controller);
            } else
            {
              LOG.error("fail to setup a cluster controller: instanceType incorrect:"
                  + type.toString());
            }
          }
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
    try
    {
      String instanceName = manager.getInstanceName();
      String clusterName = manager.getClusterName();
      final ZNRecord leaderRecord = new ZNRecord(ControllerPropertyType.LEADER.toString());
      leaderRecord.setSimpleField(ControllerPropertyType.LEADER.toString(),
                                  manager.getInstanceName());
      ClusterDataAccessor dataAccessor = manager.getDataAccessor();
      ZNRecord currentleader = dataAccessor.getControllerProperty(ControllerPropertyType.LEADER);
      if (currentleader == null)
      {
        dataAccessor.createControllerProperty(ControllerPropertyType.LEADER,
                                              leaderRecord,
                                              CreateMode.EPHEMERAL);
        // set controller history
        ZNRecord histRecord =
            dataAccessor.getControllerProperty(ControllerPropertyType.HISTORY);

        List<String> list = histRecord.getListField(clusterName);

        list.add(instanceName);
        dataAccessor.setControllerProperty(ControllerPropertyType.HISTORY,
                                           histRecord,
                                           CreateMode.PERSISTENT);
        return true;
      } else
      {
        LOG.info("Leader exists for cluster:" + clusterName + " currentLeader:"
            + currentleader.getId());
      }

    } catch (ZkNodeExistsException e)
    {
      LOG.warn("Ignorable exception. Found that leader already exists, " + e.getMessage());
    }
    return false;
  }

}
