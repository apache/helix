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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkHelixTestManager;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.api.PartitionId;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSessionExpiryInTransition extends ZkIntegrationTestBase {

  public class SessionExpiryTransition extends MockTransition {
    private final AtomicBoolean _done = new AtomicBoolean();

    @Override
    public void doTransition(Message message, NotificationContext context) {
      ZkHelixTestManager manager = (ZkHelixTestManager) context.getManager();

      String instance = message.getTgtName();
      PartitionId partition = message.getPartitionId();
      if (instance.equals("localhost_12918") && partition.toString().equals("TestDB0_1") // TestDB0_1
                                                                                         // is SLAVE
          // on localhost_12918
          && _done.getAndSet(true) == false) {
        try {
          ZkTestHelper.expireSession(manager.getZkClient());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testSessionExpiryInTransition() throws Exception {
    Logger.getRootLogger().setLevel(Level.WARN);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[5];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    ClusterController controller = new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);
      ZkHelixTestManager manager =
          new ZkHelixTestManager(clusterName, instanceName, InstanceType.PARTICIPANT, ZK_ADDR);
      participants[i] = new MockParticipant(manager, new SessionExpiryTransition());
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // clean up
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    Thread.sleep(2000);
    controller.syncStop();

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
