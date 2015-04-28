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

import java.util.Arrays;
import java.util.Date;

import org.apache.helix.HelixConstants;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.TestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * This is a simple integration test. We will use this until we have framework
 * which helps us write integration tests easily
 */

public class SinglePartitionLeaderStandByTest extends ZkTestBase {
  @Test
  public void test() throws Exception {
    String clusterName = TestUtil.getTestName();
    int n = 4;

    System.out.println("START " + clusterName +" at " + new Date(System.currentTimeMillis()));

    // Thread.currentThread().join();
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        2, // partitions per resource
        n, // number of nodes
        0, // replicas
        "LeaderStandby", 
        RebalanceMode.FULL_AUTO,
        false); // dont rebalance

    // rebalance ideal-state to use ANY_LIVEINSTANCE for preference list
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    PropertyKey key = keyBuilder.idealStates("TestDB0");
    IdealState idealState = accessor.getProperty(key);
    idealState.setReplicas(HelixConstants.StateModelToken.ANY_LIVEINSTANCE.toString());
    idealState.getRecord().setListField("TestDB0_0",
        Arrays.asList(HelixConstants.StateModelToken.ANY_LIVEINSTANCE.toString()));
    accessor.setProperty(key, idealState);

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));

    Assert.assertTrue(result);
    //stop the first participatn
    participants[0].syncStop();
    Thread.sleep(10000);
    for (int i = 0; i < 1; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }
    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
