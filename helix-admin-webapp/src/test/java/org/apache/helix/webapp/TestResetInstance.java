package org.apache.helix.webapp;

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

import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.webapp.resources.JsonParameters;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResetInstance extends AdminTestBase {
  @Test
  public void testResetInstance() throws Exception {
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
    MockController controller = new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>();
    errPartitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
    errPartitions.put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_8"));

    // start mock participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        participants[i] =
            new MockParticipant(_zkaddr, clusterName, instanceName);
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

    // reset node "localhost_12918"
    participants[0].setTransition(null);
    String hostName = "localhost_12918";
    String instanceUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/instances/" + hostName;

    Map<String, String> paramMap = new HashMap<String, String>();
    paramMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.resetInstance);
    TestHelixAdminScenariosRest.assertSuccessPostOperation(instanceUrl, paramMap, false);

    result =
        ClusterStateVerifier
            .verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName)));
    Assert.assertTrue(result, "Cluster verification fails");

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
