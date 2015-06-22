package org.apache.helix.integration;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.api.State;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResetPartitionState extends ZkTestBase {
  int _errToOfflineInvoked = 0;

  class ErrTransitionWithResetCnt extends ErrTransition {
    public ErrTransitionWithResetCnt(Map<String, Set<String>> errPartitions) {
      super(errPartitions);
    }

    @Override
    public void doTransition(Message message, NotificationContext context) {
      // System.err.println("doReset() invoked");
      super.doTransition(message, context);
      State fromState = message.getTypedFromState();
      State toState = message.getTypedToState();
      if (fromState.toString().equals("ERROR") && toState.toString().equals("OFFLINE")) {
        _errToOfflineInvoked++;
      }
    }

  }

  @Test()
  public void testResetPartitionState() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>();
    errPartitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
    errPartitions.put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_8"));

    // start mock participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
        participants[i].setTransition(new ErrTransition(errPartitions));
      } else {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    // verify cluster
    Map<String, Map<String, String>> errStateMap = new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    errStateMap.get("TestDB0").put("TestDB0_8", "localhost_12918");
    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName, errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");

    // reset a non-exist partition, should throw exception
    try {
      String command =
          "--zkSvr " + _zkaddr + " --resetPartition " + clusterName
              + " localhost_12918 TestDB0 TestDB0_nonExist";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("Should throw exception on reset a non-exist partition");
    } catch (Exception e) {
      // OK
    }

    // reset one error partition
    errPartitions.remove("SLAVE-MASTER");
    participants[0].setTransition(new ErrTransitionWithResetCnt(errPartitions));
    clearStatusUpdate(clusterName, "localhost_12918", "TestDB0", "TestDB0_4");
    _errToOfflineInvoked = 0;
    String command =
        "--zkSvr " + _zkaddr + " --resetPartition " + clusterName
            + " localhost_12918 TestDB0 TestDB0_4";

    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(200); // wait reset to be done
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("Should throw exception on reset a partition not in ERROR state");
    } catch (Exception e) {
      // OK
    }

    errStateMap.get("TestDB0").remove("TestDB0_4");
    result =
        ClusterStateVerifier
            .verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName, errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");
    Assert.assertEquals(_errToOfflineInvoked, 1);

    // reset the other error partition
    participants[0].setTransition(new ErrTransitionWithResetCnt(null));
    clearStatusUpdate(clusterName, "localhost_12918", "TestDB0", "TestDB0_8");

    command =
        "--zkSvr " + _zkaddr + " --resetPartition " + clusterName
            + " localhost_12918 TestDB0 TestDB0_8";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result, "Cluster verification fails");
    Assert.assertEquals(_errToOfflineInvoked, 2, "Should reset 2 partitions");

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  private void clearStatusUpdate(String clusterName, String instance, String resource,
      String partition) {
    // clear status update for error partition so verify() will not fail on old
    // errors
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instance));
    accessor.removeProperty(keyBuilder.stateTransitionStatus(instance, liveInstance
        .getTypedSessionId().stringify(), resource, partition));

  }
  // TODO: throw exception in reset()
}
