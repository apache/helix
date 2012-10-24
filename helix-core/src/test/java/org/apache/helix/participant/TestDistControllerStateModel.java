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
package org.apache.helix.participant;

import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.participant.DistClusterControllerStateModel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDistControllerStateModel extends ZkUnitTestBase
{
  final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  DistClusterControllerStateModel stateModel = null;

  @BeforeMethod()
  public void beforeMethod()
  {
    stateModel = new DistClusterControllerStateModel(ZK_ADDR);
    if (_gZkClient.exists("/" + clusterName))
    {
      _gZkClient.deleteRecursive("/" + clusterName);
    }
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
  }

  @Test()
  public void testOnBecomeStandbyFromOffline()
  {
    stateModel.onBecomeStandbyFromOffline(null, null);
  }

  @Test()
  public void testOnBecomeLeaderFromStandby()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");
    try
    {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }

  @Test()
  public void testOnBecomeStandbyFromLeader()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }

  @Test()
  public void testOnBecomeOfflineFromStandby()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");

    stateModel.onBecomeOfflineFromStandby(message, null);
  }

  @Test()
  public void testOnBecomeDroppedFromOffline()
  {
    stateModel.onBecomeDroppedFromOffline(null, null);
  }

  @Test()
  public void testOnBecomeOfflineFromDropped()
  {
    stateModel.onBecomeOfflineFromDropped(null, null);
  }

  @Test()
  public void testRollbackOnError()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");
    try
    {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    stateModel.rollbackOnError(message, new NotificationContext(null), null);
  }

  @Test()
  public void testReset()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");
    try
    {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    stateModel.reset();
  }

}
