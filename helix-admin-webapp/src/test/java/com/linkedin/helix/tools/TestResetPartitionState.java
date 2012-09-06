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
package com.linkedin.helix.tools;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.integration.ZkIntegrationTestBase;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockParticipant.ErrTransition;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.webapp.HelixAdminWebApp;
import com.linkedin.helix.webapp.resources.ClusterRepresentationUtil;
import com.linkedin.helix.webapp.resources.InstanceResource;

public class TestResetPartitionState extends ZkIntegrationTestBase
{
  int _port = 2202;
  Thread _adminThread = null;
  void startAdminWebAppThread() throws Exception
  {
    if(_adminThread!= null)
    {
      return;
    }
      _adminThread = new Thread(new Runnable() {
      @Override
      public void run()
      {
        HelixAdminWebApp app = null;
        try
        {
          app = new HelixAdminWebApp(ZK_ADDR, _port);
          app.start();
          Thread.currentThread().join();
        } 
        catch (Exception e)
        {
          e.printStackTrace();
        }
        finally
        {
          if(app != null)
          {
            app.stop();
          }
        }
      }
    });
    _adminThread.setDaemon(true);
    _adminThread.start();
  }
  
  String getClusterUrl(String cluster)
  {
    return "http://localhost:" + _port + "/clusters" + "/" + cluster;
  }
  
  String getInstanceUrl(String cluster, String instance)
  {
    return "http://localhost:" + _port + "/clusters/" + cluster + "/instances/" + instance;
  }
  
  String getResourceUrl(String cluster, String resourceGroup)
  {
    return "http://localhost:" + _port + "/clusters/" + cluster + "/resourceGroups/" + resourceGroup;
  }
  int _errToOfflineInvoked = 0;

  class ErrTransitionWithReset extends ErrTransition
  {
    public ErrTransitionWithReset(Map<String, Set<String>> errPartitions)
    {
      super(errPartitions);
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      // System.err.println("doRest() invoked");
      super.doTransition(message, context);
      String fromState = message.getFromState();
      String toState = message.getToState();
      if (fromState.equals("ERROR") && toState.equals("OFFLINE"))
      {
        _errToOfflineInvoked++;
      }
    }

  }

  @Test()
  public void testResetPartitionState() throws Exception
  {
    startAdminWebAppThread();
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
                            1, 10, 5, 3, "MasterSlave", true);

    TestHelper.startController(clusterName, "controller_0",
                                      ZK_ADDR, HelixControllerMain.STANDALONE);
    Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>()
    {
      {
        put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
        put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_8"));
      }
    };
    for (int i = 0; i < 5; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (12918 + i);

      if (i == 0)
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR,
                                new ErrTransition(errPartitions));
      }
      else
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR);
      }
      participants[i].syncStart();
      // new Thread(participants[i]).start();
    }

    Map<String, Map<String, String>> errStateMap = new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    errStateMap.get("TestDB0").put("TestDB0_8", "localhost_12918");
    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStateMap));
    Assert.assertTrue(result);

    // reset a non-exist partition, should throw exception
    String hostName = "localhost_12918";
    String instanceUrl = getInstanceUrl(clusterName, hostName);
    
    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(ClusterRepresentationUtil._managementCommand, ClusterSetup.resetPartition);
    paraMap.put(InstanceResource._partition, "TestDB0_nonExist");
    paraMap.put(InstanceResource._resource, "TestDB0");

    TestHelixAdminScenariosRest.assertSuccessPostOperation(instanceUrl, paraMap, true);
    
    // reset one error partition
    errPartitions.remove("SLAVE-MASTER");
    participants[0].setTransition(new ErrTransitionWithReset(errPartitions));
    clearStatusUpdate(clusterName, "localhost_12918", "TestDB0", "TestDB0_4");
    _errToOfflineInvoked = 0;
    
    paraMap.put(InstanceResource._partition, "TestDB0_4");
    TestHelixAdminScenariosRest.assertSuccessPostOperation(instanceUrl, paraMap, false);

    Thread.sleep(200);  // wait reset to be done
    TestHelixAdminScenariosRest.assertSuccessPostOperation(instanceUrl, paraMap, true);


    errStateMap.get("TestDB0").remove("TestDB0_4");
    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStateMap));
    Assert.assertTrue(result);

    Assert.assertEquals(_errToOfflineInvoked, 1);

    // reset the other error partition
    participants[0].setTransition(new ErrTransitionWithReset(null));
    clearStatusUpdate(clusterName, "localhost_12918", "TestDB0", "TestDB0_8");
    //tool.resetPartition(clusterName, "localhost_12918", "TestDB0", "TestDB0_8");
    paraMap.put(InstanceResource._partition, "TestDB0_8");
    TestHelixAdminScenariosRest.assertSuccessPostOperation(instanceUrl, paraMap, false);
    
    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    Assert.assertEquals(_errToOfflineInvoked, 2);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  private void clearStatusUpdate(String clusterName, String instance, String resource,
      String partition)
  {
    // clear status update for error partition so verify() will not fail on old
    // errors
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instance));
    accessor.removeProperty(keyBuilder.stateTransitionStatus(instance, liveInstance.getSessionId(), resource, partition));

   }
  // TODO: throw exception in reset()
}
