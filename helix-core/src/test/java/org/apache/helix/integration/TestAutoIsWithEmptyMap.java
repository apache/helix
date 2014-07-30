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

import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.strategy.DefaultTwoStateStrategy;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.model.IdealState;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAutoIsWithEmptyMap extends ZkTestBase {
  @Test
  public void testAutoIsWithEmptyMap() throws Exception {
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
        "LeaderStandby", false); // do not rebalance

    // calculate and set custom ideal state
    String idealPath = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName, "TestDB0");
    ZNRecord curIdealState = _zkclient.readData(idealPath);

    List<String> instanceNames = new ArrayList<String>(5);
    for (int i = 0; i < 5; i++) {
      int port = 12918 + i;
      instanceNames.add("localhost_" + port);
    }
    ZNRecord idealState =
        DefaultTwoStateStrategy.calculateIdealState(instanceNames, 10, 2, "TestDB0", "LEADER",
            "STANDBY");
    // System.out.println(idealState);
    // curIdealState.setSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.toString(),
    // "CUSTOMIZED");
    curIdealState.setSimpleField(IdealState.IdealStateProperty.REPLICAS.toString(), "3");

    curIdealState.setListFields(idealState.getListFields());
    _zkclient.writeData(idealPath, curIdealState);

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[5];
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
