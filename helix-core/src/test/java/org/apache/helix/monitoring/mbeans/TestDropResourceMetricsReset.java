package org.apache.helix.monitoring.mbeans;

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

import java.lang.management.ManagementFactory;
import java.util.Date;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDropResourceMetricsReset extends ZkUnitTestBase {
  private static final MBeanServerConnection SERVER = ManagementFactory.getPlatformMBeanServer();

  @Test
  public void testBasic() throws Exception {
    final int NUM_PARTICIPANTS = 4;
    final int NUM_PARTITIONS = 64;
    final int NUM_REPLICAS = 1;
    final String RESOURCE_NAME = "TestDB0";

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "MasterSlave", RebalanceMode.FULL_AUTO, // use FULL_AUTO mode to test node tagging
        true); // do rebalance

    // Start participants and controller
    ClusterSetup setupTool = new ClusterSetup(_gZkClient);
    MockParticipantManager[] participants = new MockParticipantManager[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      participants[i].syncStart();
    }
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // Verify that the bean was created
    Thread.sleep(1000);
    ObjectName objectName = getObjectName(RESOURCE_NAME, clusterName);
    Assert.assertTrue(SERVER.isRegistered(objectName));

    // Drop the resource
    setupTool.dropResourceFromCluster(clusterName, RESOURCE_NAME);

    // Verify that the bean was removed
    Thread.sleep(1000);
    Assert.assertFalse(SERVER.isRegistered(objectName));

    // Clean up
    controller.syncStop();
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private ObjectName getObjectName(String resourceName, String clusterName)
      throws MalformedObjectNameException {
    String clusterBeanName =
        String.format("%s=%s", ClusterStatusMonitor.CLUSTER_DN_KEY, clusterName);
    String resourceBeanName =
        String.format("%s,%s=%s", clusterBeanName, ClusterStatusMonitor.RESOURCE_DN_KEY,
            resourceName);
    return new ObjectName(String.format("%s: %s", ClusterStatusMonitor.CLUSTER_STATUS_KEY,
        resourceBeanName));
  }
}
