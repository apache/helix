package org.apache.helix.manager.zk;

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

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkHelixController extends ZkTestBase {

  @Test
  public void testOnConnectedAndDisconnecting() throws Exception {
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
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // create connection
    HelixConnection connection = new ZkHelixConnection(_zkaddr);
    connection.connect();

    // start controller
    ClusterId clusterId = ClusterId.from(clusterName);
    ControllerId controllerId = ControllerId.from("controller");
    HelixController controller = connection.createController(clusterId, controllerId);
    controller.start();

    // check leader znode exists
    HelixDataAccessor accessor = connection.createDataAccessor(clusterId);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getInstanceName(), controllerId.stringify());

    // stop participant
    controller.stop();

    // check leader znode is gone
    Assert.assertNull(accessor.getProperty(keyBuilder.controllerLeader()));

    // clean up
    connection.disconnect();

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Remove leader znode externally should invoke another round of leader-election
   * this simulates the race condition in
   * {@link ZkHelixLeaderElection#onControllerChange(org.apache.helix.NotificationContext)}
   * @throws Exception
   */
  @Test
  public void testRemoveLeaderZnode() throws Exception {

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
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // create connection
    HelixConnection connection = new ZkHelixConnection(_zkaddr);
    connection.connect();

    // start controller
    ClusterId clusterId = ClusterId.from(clusterName);
    final ControllerId controllerId = ControllerId.from("controller");

    // start controller
    HelixController controller = connection.createController(clusterId, controllerId);
    controller.start();

    // check live-instance znode for localhost_12918 exists
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getInstanceName(), controllerId.stringify());

    // remove leader znode externally
    accessor.removeProperty(keyBuilder.controllerLeader());

    // verify leader is re-elected
    boolean result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
        if (leader == null) {
          return false;
        }

        return leader.getInstanceName().equals(controllerId.stringify());
      }
    }, 3 * 1000);

    Assert.assertTrue(result, "Fail to re-elect a new leader");

    // clean up
    connection.disconnect();

    // check leader znode is gone
    Assert.assertNull(accessor.getProperty(keyBuilder.controllerLeader()));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
