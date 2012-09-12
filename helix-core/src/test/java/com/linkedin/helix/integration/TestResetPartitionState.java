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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.mock.controller.ClusterController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockParticipant.ErrTransition;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestResetPartitionState extends ZkIntegrationTestBase
{
  int _errToOfflineInvoked = 0;

  class ErrTransitionWithResetCnt extends ErrTransition
  {
    public ErrTransitionWithResetCnt(Map<String, Set<String>> errPartitions)
    {
      super(errPartitions);
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      // System.err.println("doReset() invoked");
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
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // start controller
    ClusterController controller =
        new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>()
    {
      {
        put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
        put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_8"));
      }
    };

    // start mock participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0)
      {
        participants[i] =
            new MockParticipant(clusterName,
                                instanceName,
                                ZK_ADDR,
                                new ErrTransition(errPartitions));
      }
      else
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR);
      }
      participants[i].syncStart();
    }

    // verify cluster
    Map<String, Map<String, String>> errStateMap =
        new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    errStateMap.get("TestDB0").put("TestDB0_8", "localhost_12918");
    boolean result =
        ClusterStateVerifier.verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                       clusterName,
                                                                                                       errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");

    // reset a non-exist partition, should throw exception
    try
    {
      String command =
          "--zkSvr " + ZK_ADDR + " --resetPartition " + clusterName
              + " localhost_12918 TestDB0 TestDB0_nonExist";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("Should throw exception on reset a non-exist partition");
    }
    catch (Exception e)
    {
      // OK
    }

    // reset one error partition
    errPartitions.remove("SLAVE-MASTER");
    participants[0].setTransition(new ErrTransitionWithResetCnt(errPartitions));
    clearStatusUpdate(clusterName, "localhost_12918", "TestDB0", "TestDB0_4");
    _errToOfflineInvoked = 0;
    String command =
        "--zkSvr " + ZK_ADDR + " --resetPartition " + clusterName
            + " localhost_12918 TestDB0 TestDB0_4";

    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(200); // wait reset to be done
    try
    {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("Should throw exception on reset a partition not in ERROR state");
    }
    catch (Exception e)
    {
      // OK
    }

    errStateMap.get("TestDB0").remove("TestDB0_4");
    result =
        ClusterStateVerifier.verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                   clusterName,
                                                                                                   errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");
    Assert.assertEquals(_errToOfflineInvoked, 1);

    // reset the other error partition
    participants[0].setTransition(new ErrTransitionWithResetCnt(null));
    clearStatusUpdate(clusterName, "localhost_12918", "TestDB0", "TestDB0_8");

    command =
        "--zkSvr " + ZK_ADDR + " --resetPartition " + clusterName
            + " localhost_12918 TestDB0 TestDB0_8";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                   clusterName));
    Assert.assertTrue(result, "Cluster verification fails");
    Assert.assertEquals(_errToOfflineInvoked, 2, "Should reset 2 partitions");

    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }

  private void clearStatusUpdate(String clusterName,
                                 String instance,
                                 String resource,
                                 String partition)
  {
    // clear status update for error partition so verify() will not fail on old
    // errors
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instance));
    accessor.removeProperty(keyBuilder.stateTransitionStatus(instance,
                                                             liveInstance.getSessionId(),
                                                             resource,
                                                             partition));

  }
  // TODO: throw exception in reset()
}
