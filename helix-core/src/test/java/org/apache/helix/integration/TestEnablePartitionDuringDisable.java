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

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.api.State;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.Message;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestEnablePartitionDuringDisable extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestEnablePartitionDuringDisable.class);

  static {
    // Logger.getRootLogger().setLevel(Level.INFO);
  }

  class EnablePartitionTransition extends MockTransition {
    int slaveToOfflineCnt = 0;
    int offlineToSlave = 0;

    @Override
    public void doTransition(Message message, NotificationContext context) {
      HelixManager manager = context.getManager();
      String clusterName = manager.getClusterName();

      String instance = message.getTgtName();
      PartitionId partitionId = message.getPartitionId();
      State fromState = message.getTypedFromState();
      State toState = message.getTypedToState();
      if (instance.equals("localhost_12919") && partitionId.equals(PartitionId.from("TestDB0_0"))) {
        if (fromState.equals("SLAVE") && toState.equals("OFFLINE")) {
          slaveToOfflineCnt++;

          try {
            String command =
                "--zkSvr " + _zkaddr + " --enablePartition true " + clusterName
                    + " localhost_12919 TestDB0 TestDB0_0";

            ClusterSetup.processCommandLineArgs(command.split("\\s+"));
          } catch (Exception e) {
            LOG.error("Exception in cluster setup", e);
          }

        } else if (slaveToOfflineCnt > 0 && fromState.equals(State.from("OFFLINE"))
            && toState.equals(State.from("SLAVE"))) {
          offlineToSlave++;
        }
      }
    }

  }

  @Test
  public void testEnablePartitionDuringDisable() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    EnablePartitionTransition transition = new EnablePartitionTransition();
    MockParticipant[] participants = new MockParticipant[5];
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (instanceName.equals("localhost_12919")) {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
        participants[i].setTransition(transition);
      } else {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // disable partitions
    String command =
        "--zkSvr " + _zkaddr + " --enablePartition false " + clusterName
            + " localhost_12919 TestDB0 TestDB0_0";

    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // ensure we get 1 slaveToOffline and 1 offlineToSlave after disable partition
    long startT = System.currentTimeMillis();
    while (System.currentTimeMillis() - startT < 10000) // retry in 5s
    {
      if (transition.slaveToOfflineCnt > 0 && transition.offlineToSlave > 0) {
        break;
      }

      Thread.sleep(100);
    }
    long endT = System.currentTimeMillis();
    System.out.println("1 disable and re-enable took: " + (endT - startT) + "ms");
    Assert.assertEquals(transition.slaveToOfflineCnt, 1, "should get 1 slaveToOffline transition");
    Assert.assertEquals(transition.offlineToSlave, 1, "should get 1 offlineToSlave transition");

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
