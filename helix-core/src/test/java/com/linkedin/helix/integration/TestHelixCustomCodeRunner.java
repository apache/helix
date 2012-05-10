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
package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.mock.storage.MockJobIntf;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.participant.CustomCodeCallbackHandler;
import com.linkedin.helix.participant.HelixCustomCodeRunner;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestHelixCustomCodeRunner extends ZkIntegrationTestBase
{
  private final String _clusterName = "CLUSTER_" + getShortClassName();
  private final int _nodeNb = 5;
  private final int _startPort = 12918;
  private final MockCallback _callback = new MockCallback();

  class MockCallback implements CustomCodeCallbackHandler
  {
    boolean _isCallbackInvoked;

    @Override
    public void onCallback(NotificationContext context)
    {
      HelixManager manager = context.getManager();
      Type type = context.getType();
      _isCallbackInvoked = true;
//      System.out.println(type + ": TestCallback invoked on " + manager.getInstanceName());
    }

  }

  class MockJob implements MockJobIntf
  {
    @Override
    public void doPreConnectJob(HelixManager manager)
    {
      try
      {
        // delay the start of the 1st participant
        //  so there will be a leadership transfer from localhost_12919 to 12918
        if (manager.getInstanceName().equals("localhost_12918"))
        {
          Thread.sleep(2000);
        }

        HelixCustomCodeRunner customCodeRunner = new HelixCustomCodeRunner(manager, ZK_ADDR);
        customCodeRunner.invoke(_callback)
                         .on(ChangeType.LIVE_INSTANCE)
                         .usingLeaderStandbyModel("TestParticLeader")
                         .start();
      } catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Override
    public void doPostConnectJob(HelixManager manager)
    {
      // TODO Auto-generated method stub

    }

  }

  @Test
  public void testCustomCodeRunner() throws Exception
  {
    System.out.println("START " + _clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(_clusterName,
                            ZK_ADDR,
                            _startPort,
                            "localhost",  // participant name prefix
                            "TestDB",     // resource name prefix
                            1,  // resourceNb
                            5,  // partitionNb
                            _nodeNb,  // nodesNb
                            _nodeNb,  // replica
                            "MasterSlave",
                            true);

    TestHelper.startController(_clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);

    MockParticipant[] partics = new MockParticipant[5];
    for (int i = 0; i < _nodeNb; i++)
    {
      String instanceName =  "localhost_" + (_startPort + i);

      partics[i] = new MockParticipant(_clusterName, instanceName, ZK_ADDR,
                        null, new MockJob());
      partics[i].syncStart();
//      new Thread(partics[i]).start();
    }
    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName));
    Assert.assertTrue(result);

    Thread.sleep(1000);  // wait for the INIT type callback to finish
    Assert.assertTrue(_callback._isCallbackInvoked);
    _callback._isCallbackInvoked = false;

    // add a new live instance
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    DataAccessor accessor = new ZKDataAccessor(_clusterName, zkClient);
    LiveInstance newLiveIns = new LiveInstance("newLiveInstance");
    newLiveIns.setHelixVersion("0.0.0");
    newLiveIns.setSessionId("randomSessionId");
    accessor.setProperty(PropertyType.LIVEINSTANCES, newLiveIns, "newLiveInstance");

    Thread.sleep(1000);  // wait for the CALLBACK type callback to finish
    Assert.assertTrue(_callback._isCallbackInvoked);

    System.out.println("END " + _clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
