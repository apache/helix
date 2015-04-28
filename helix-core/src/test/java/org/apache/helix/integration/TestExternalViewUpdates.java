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

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.testutil.TestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestExternalViewUpdates extends ZkTestBase {
  @Test
  public void testExternalViewUpdates() throws Exception {
    System.out.println("START testExternalViewUpdates at " + new Date(System.currentTimeMillis()));

    String clusterName = TestUtil.getTestName();
    MockParticipant[] participants = new MockParticipant[5];
    int resourceNb = 10;
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        resourceNb, // resources
        1, // partitions per resource
        5, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(_zkaddr, clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // need to verify that each ExternalView's version number is 2
    Builder keyBuilder = new Builder(clusterName);
    String parentPath = keyBuilder.externalViews().getPath();
    List<String> childNames = _baseAccessor.getChildNames(parentPath, 0);

    List<String> paths = new ArrayList<String>();
    for (String name : childNames) {
      paths.add(parentPath + "/" + name);
    }

    // Stat[] stats = accessor.getStats(paths);
    for (String path : paths) {
      Stat stat = _baseAccessor.getStat(path, 0);
      Assert.assertTrue(stat.getVersion() <= 2, "ExternalView should be updated at most 2 times");
    }

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }
    System.out.println("END testExternalViewUpdates at " + new Date(System.currentTimeMillis()));
  }
}
