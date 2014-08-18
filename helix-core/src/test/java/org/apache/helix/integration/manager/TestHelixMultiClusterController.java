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
import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkCallbackHandler;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixMultiClusterController extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestHelixMultiClusterController.class);

  @Test
  public void simpleIntegrationTest() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    HelixManager[] multiClusterControllers = new HelixManager[n];
    for (int i = 0; i < n; i++) {
      int port = 12918 + i;
      multiClusterControllers[i] =
          new ZKHelixManager(clusterName, "localhost_" + port, InstanceType.CONTROLLER_PARTICIPANT,
              _zkaddr);
      multiClusterControllers[i].getStateMachineEngine().registerStateModelFactory(StateModelDefId.MasterSlave,
          new MockMSModelFactory());
      multiClusterControllers[i].connect();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // disconnect first multi-cluster-controller, and verify second takes leadership
    multiClusterControllers[0].disconnect();

    // verify leader changes to localhost_12919
    Thread.sleep(100);
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance("localhost_12918")));
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getId(), "localhost_12919");

    // clean up
    multiClusterControllers[1].disconnect();
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance("localhost_12919")));
    Assert.assertNull(accessor.getProperty(keyBuilder.controllerLeader()));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * expire a controller and make sure the other takes the leadership
   * @param expireController
   * @param newController
   * @throws Exception
   */
  void expireController(MockMultiClusterController expireController,
      MockMultiClusterController newController) throws Exception {
    String clusterName = expireController.getClusterName();
    LOG.info("Expiring multiClusterController: " + expireController.getInstanceName()
        + ", session: " + expireController.getSessionId() + " ...");
    String oldSessionId = expireController.getSessionId();

    ZkTestHelper.expireSession(expireController.getZkClient());
    String newSessionId = expireController.getSessionId();
    LOG.debug("Expried multiClusterController: " + expireController.getInstanceName()
        + ", oldSessionId: " + oldSessionId + ", newSessionId: " + newSessionId);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // verify leader changes to localhost_12919
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNotNull(accessor.getProperty(keyBuilder.liveInstance(expireController
        .getInstanceName())));
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getId(), newController.getInstanceName());

    // check expired-controller has 2 handlers: message and leader-election
    TestHelper.printHandlers(expireController, expireController.getHandlers());

    List<ZkCallbackHandler> handlers = expireController.getHandlers();
    Assert.assertEquals(handlers.size(), 2,
        "MultiCluster controller should have 2 handler (message and leader-election) after lose leadership, but was "
            + handlers.size());
  }

  @Test
  public void simpleSessionExpiryTest() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    MockMultiClusterController[] multiClusterControllers = new MockMultiClusterController[n];

    for (int i = 0; i < n; i++) {
      String contrllerName = "localhost_" + (12918 + i);
      multiClusterControllers[i] =
          new MockMultiClusterController(_zkaddr, clusterName, contrllerName);
      multiClusterControllers[i].getStateMachineEngine().registerStateModelFactory(StateModelDefId.MasterSlave,
          new MockMSModelFactory());
      multiClusterControllers[i].connect();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // expire localhost_12918
    expireController(multiClusterControllers[0], multiClusterControllers[1]);

    // expire localhost_12919
    expireController(multiClusterControllers[1], multiClusterControllers[0]);

    // clean up
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    for (int i = 0; i < n; i++) {
      multiClusterControllers[i].disconnect();
      Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance(multiClusterControllers[i]
          .getInstanceName())));
    }
    Assert.assertNull(accessor.getProperty(keyBuilder.controllerLeader()));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
