package org.apache.helix.integration.manager;

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

import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.manager.zk.ControllerManager;
import org.apache.helix.manager.zk.DistributedControllerManager;
import org.apache.helix.manager.zk.ParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDistributedControllerManager extends ZkIntegrationTestBase {

  @Test
  public void simpleIntegrationTest() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            4, // partitions per resource
                            n, // number of nodes
                            2, // replicas
                            "MasterSlave",
                            true); // do rebalance
 
    DistributedControllerManager[] distributedControllers = new DistributedControllerManager[n];
    for (int i = 0; i < n; i++) {
      int port = 12918 + i;
      distributedControllers[i] = new DistributedControllerManager(ZK_ADDR, clusterName, "localhost_" + port);
      distributedControllers[i].getStateMachineEngine().registerStateModelFactory("MasterSlave", new MockMSModelFactory());
      distributedControllers[i].connect();
    }
    
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // disconnect first distributed-controller, and verify second takes leadership
    distributedControllers[0].disconnect();
    
    // verify leader changes to localhost_12919
    Thread.sleep(100);
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance("localhost_12918")));
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getId(), "localhost_12919");

    // clean up
    distributedControllers[1].disconnect();
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance("localhost_12919")));
    Assert.assertNull(accessor.getProperty(keyBuilder.controllerLeader()));

    
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
