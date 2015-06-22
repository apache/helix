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
import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @see HELIX-552
 *      StateModelFactory#_stateModelMap should use both resourceName and partitionKey to map a
 *      state model
 */
public class TestResourceWithSamePartitionKey extends ZkTestBase {

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        2, // partitions per resource
        n, // number of nodes
        2, // replicas
        "OnlineOffline", RebalanceMode.CUSTOMIZED, false); // do rebalance

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkclient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setReplicas("2");
    idealState.setPartitionState("0", "localhost_12918", "ONLINE");
    idealState.setPartitionState("0", "localhost_12919", "ONLINE");
    idealState.setPartitionState("1", "localhost_12918", "ONLINE");
    idealState.setPartitionState("1", "localhost_12919", "ONLINE");
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    MockController controller = new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // add a second resource with the same partition-key
    IdealState newIdealState = new IdealState("TestDB1");
    newIdealState.getRecord().setSimpleFields(idealState.getRecord().getSimpleFields());
    newIdealState.setPartitionState("0", "localhost_12918", "ONLINE");
    newIdealState.setPartitionState("0", "localhost_12919", "ONLINE");
    newIdealState.setPartitionState("1", "localhost_12918", "ONLINE");
    newIdealState.setPartitionState("1", "localhost_12919", "ONLINE");
    accessor.setProperty(keyBuilder.idealStates("TestDB1"), newIdealState);

    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // assert no ERROR
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      List<String> errs = accessor.getChildNames(keyBuilder.errors(instanceName));
      Assert.assertTrue(errs.isEmpty());
    }

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
