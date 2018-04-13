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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestExternalViewUpdates extends ZkIntegrationTestBase {
  @Test
  public void testExternalViewUpdates() throws Exception {
    System.out.println("START testExternalViewUpdates at " + new Date(System.currentTimeMillis()));

    String clusterName = getShortClassName();
    int numResource = 10;
    int numPartition = 1;
    int numReplica = 1;
    int numNode = 5;
    int startPort = 12918;
    MockParticipantManager[] participants = new MockParticipantManager[numNode];
    TestHelper.setupCluster(clusterName, ZK_ADDR, startPort, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        numResource, // resources
        numPartition, // partitions per resource
        numNode, // number of nodes
        numReplica, // replicas
        "MasterSlave", true); // do rebalance

    // start participants
    for (int i = 0; i < numNode; i++) {
      String instanceName = "localhost_" + (startPort + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    // start controller after participants to trigger rebalance immediately
    // after the controller is ready
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // 10 Resources, 1 partition, 1 replica, so there are at most 10 ZK writes for EV (assume)
    // worst case that no event is batched in controller. Therefore, EV version should be < 10
    Builder keyBuilder = new Builder(clusterName);
    BaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    String parentPath = keyBuilder.externalViews().getPath();
    List<String> childNames = accessor.getChildNames(parentPath, 0);

    List<String> paths = new ArrayList<String>();
    for (String name : childNames) {
      paths.add(parentPath + "/" + name);
    }

    int maxEV = numResource * numPartition * numReplica;
    for (String path : paths) {
      Stat stat = accessor.getStat(path, 0);
      Assert.assertTrue(stat.getVersion() <= maxEV,
          "ExternalView should be updated at most " + maxEV + " times");
    }

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }
    System.out.println("END testExternalViewUpdates at " + new Date(System.currentTimeMillis()));
  }
}
