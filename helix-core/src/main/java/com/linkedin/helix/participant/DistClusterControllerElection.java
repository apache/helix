/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.participant;

import java.lang.management.ManagementFactory;

import org.apache.log4j.Logger;

import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.controller.GenericHelixController;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.model.LeaderHistory;
import com.linkedin.helix.model.LiveInstance;

// TODO: merge with GenericHelixController
public class DistClusterControllerElection implements ControllerChangeListener
{
  private static Logger                LOG         =
                                                       Logger.getLogger(DistClusterControllerElection.class);
  private final String                 _zkAddr;
  private final GenericHelixController _controller = new GenericHelixController();
  private HelixManager                 _leader;

  public DistClusterControllerElection(String zkAddr)
  {
    _zkAddr = zkAddr;
  }

  /**
   * may be accessed by multiple threads: zk-client thread and
   * ZkHelixManager.disconnect()->reset() TODO: Refactor accessing HelixMaangerMain class
   * statically
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
        // DataAccessor dataAccessor = manager.getDataAccessor();
        HelixDataAccessor accessor = manager.getHelixDataAccessor();
        Builder keyBuilder = accessor.keyBuilder();

        while (accessor.getProperty(keyBuilder.controllerLeader()) == null)
        {
          boolean success = tryUpdateController(manager);
          if (success)
          {
            updateHistory(manager);
            if (type == InstanceType.CONTROLLER)
            {
              HelixControllerMain.addListenersToController(manager, _controller);
              manager.startTimerTasks();
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
              _leader.startTimerTasks();
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
    // DataAccessor dataAccessor = manager.getDataAccessor();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = new LiveInstance(manager.getInstanceName());
    try
    {
      leader.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
      // TODO: this session id is not the leader's session id in distributed mode
      leader.setSessionId(manager.getSessionId());
      leader.setHelixVersion(manager.getVersion());
      boolean success = accessor.createProperty(keyBuilder.controllerLeader(), leader);
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
                    + ". Need to check again whether leader node has been created or not",
                e);
    }

    leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader != null)
    {
      String leaderName = leader.getInstanceName(); // leader.getLeader();
      LOG.info("Leader exists for cluster:" + manager.getClusterName()
          + ", currentLeader:" + leaderName);

      if (leaderName != null && leaderName.equals(manager.getInstanceName()))
      {
        return true;
      }
    }

    return false;
  }

  private void updateHistory(HelixManager manager)
  {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    LeaderHistory history = accessor.getProperty(keyBuilder.controllerLeaderHistory());
    if (history == null)
    {
      history = new LeaderHistory(PropertyType.HISTORY.toString());
    }
    history.updateHistory(manager.getClusterName(), manager.getInstanceName());
    accessor.setProperty(keyBuilder.controllerLeaderHistory(), history);
  }
}
