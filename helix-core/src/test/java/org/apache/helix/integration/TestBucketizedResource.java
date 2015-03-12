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
import java.util.List;

import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBucketizedResource extends ZkIntegrationTestBase {
  @Test()
  public void testBucketizedResource() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[5];
    // ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    // String idealStatePath = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName,
    // "TestDB0");
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setBucketSize(1);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }
    PropertyKey evKey = accessor.keyBuilder().externalView("TestDB0");

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    ExternalView ev = accessor.getProperty(evKey);
    int v1 = ev.getRecord().getVersion();
    // disable the participant
    _gSetupTool.getClusterManagementTool().enableInstance(clusterName,
        participants[0].getInstanceName(), false);
    // wait for change in EV
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // read the version in EV
    ev = accessor.getProperty(evKey);
    int v2 = ev.getRecord().getVersion();
    Assert.assertEquals(v2 > v1, true);

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testBounceDisableAndDrop() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String dbName = "TestDB0";
    int n = 5;
    int r = 3;
    List<String> instanceNames =
        Arrays.asList("localhost_0", "localhost_1", "localhost_2", "localhost_3", "localhost_4");

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // create cluster and add nodes to cluster
    MockParticipantManager[] participants = new MockParticipantManager[n];
    _gSetupTool.addCluster(clusterName, true);
    _gSetupTool.addInstancesToCluster(clusterName,
        instanceNames.toArray(new String[instanceNames.size()]));

    // add a bucketized resource
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    ZNRecord idealStateRec =
        DefaultIdealStateCalculator.calculateIdealState(instanceNames, 10, r - 1, dbName, "MASTER",
            "SLAVE");
    IdealState idealState = new IdealState(idealStateRec);
    idealState.setBucketSize(2);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setReplicas(Integer.toString(r));
    accessor.setProperty(keyBuilder.idealStates(dbName), idealState);

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceNames.get(i));
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // bounce
    participants[0].syncStop();
    participants[0] = new MockParticipantManager(ZK_ADDR, clusterName, instanceNames.get(0));
    participants[0].syncStart();

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // make sure participants[0]'s current state is bucketzied correctly during carryover
    String path =
        keyBuilder.currentState(instanceNames.get(0), participants[0].getSessionId(), dbName)
            .getPath();
    ZNRecord record = baseAccessor.get(path, null, 0);
    Assert.assertTrue(record.getMapFields().size() == 0);

    // disable the bucketize resource
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.enableResource(clusterName, dbName, false);
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // drop the bucketize resource
    _gSetupTool.dropResourceFromCluster(clusterName, dbName);
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // make sure external-view is cleaned up
    path = keyBuilder.externalView(dbName).getPath();
    result = baseAccessor.exists(path, 0);
    Assert.assertFalse(result);

    // clean up
    controller.syncStop();
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
