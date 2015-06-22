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

import javax.management.InstanceNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResetClusterMetrics extends ZkTestBase {
  /**
   * Ensure cluster status lifecycle is tied to controller leader status
   */
  @Test
  public void testControllerDisconnect() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    // Set up a cluster with one of everything
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, "localhost", "Resource", 1, 1, 1, 1,
        "OnlineOffline", RebalanceMode.FULL_AUTO, true);

    // Add a participant
    MockParticipant participant =
        new MockParticipant(_zkaddr, clusterName, "localhost_12918");
    participant.syncStart();

    // Add a controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // Make sure everything gets assigned
    Thread.sleep(1000);
    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // Check the metrics
    Assert.assertTrue(metricsExist(clusterName, participant.getInstanceName()));

    // Stop the controller
    controller.syncStop();

    // Check the metrics
    Thread.sleep(1000);
    Assert.assertFalse(metricsExist(clusterName, participant.getInstanceName()));
  }

  private boolean metricsExist(String clusterName, String instanceName) throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    String instanceBeanName =
        ClusterStatusMonitor.CLUSTER_DN_KEY + "=" + clusterName + ","
            + ClusterStatusMonitor.INSTANCE_DN_KEY + "=" + instanceName;
    boolean instanceBeanFound;
    try {
      MBeanInfo info = server.getMBeanInfo(objectName(instanceBeanName));
      instanceBeanFound = info != null;
    } catch (InstanceNotFoundException e) {
      instanceBeanFound = false;
    }
    String clusterBeanName = ClusterStatusMonitor.CLUSTER_DN_KEY + "=" + clusterName;
    boolean clusterBeanFound;
    try {
      MBeanInfo info = server.getMBeanInfo(objectName(clusterBeanName));
      clusterBeanFound = info != null;
    } catch (InstanceNotFoundException e) {
      clusterBeanFound = false;
    }
    return instanceBeanFound && clusterBeanFound;
  }

  private ObjectName objectName(String beanName) throws Exception {
    return new ObjectName(ClusterStatusMonitor.CLUSTER_STATUS_KEY + ": " + beanName);
  }
}
