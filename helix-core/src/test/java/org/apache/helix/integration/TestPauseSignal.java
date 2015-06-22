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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPauseSignal extends ZkTestBase {
  @Test()
  public void testPauseSignal() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[5];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
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
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // pause the cluster and make sure pause is persistent
    ZkClient zkClient = new ZkClient(_zkaddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    final HelixDataAccessor tmpAccessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

    String cmd = "-zkSvr " + _zkaddr + " -enableCluster " + clusterName + " false";
    ClusterSetup.processCommandLineArgs(cmd.split(" "));

    tmpAccessor.setProperty(tmpAccessor.keyBuilder().pause(), new PauseSignal("pause"));
    zkClient.close();

    // wait for controller to be signaled by pause
    Thread.sleep(1000);

    // add a new resource group
    _setupTool.addResourceToCluster(clusterName, "TestDB1", 10, "MasterSlave");
    _setupTool.rebalanceStorageCluster(clusterName, "TestDB1", 3);

    // make sure TestDB1 external view is empty
    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 1000, clusterName, "TestDB1",
        TestHelper.<String> setOf("localhost_12918", "localhost_12919", "localhost_12920",
            "localhost_12921", "localhost_12922"), _zkaddr);

    // resume controller
    cmd = "-zkSvr " + _zkaddr + " -enableCluster " + clusterName + " true";
    ClusterSetup.processCommandLineArgs(cmd.split(" "));
    result =
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
