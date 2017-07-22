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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestDisableResourceMbean extends ZkUnitTestBase {
  private MBeanServerConnection _mbeanServer = ManagementFactory.getPlatformMBeanServer();

  @Test public void testDisableResourceMonitoring() throws Exception {
    final int NUM_PARTICIPANTS = 2;
    String clusterName = TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName();
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        3, // resources
        32, // partitions per resource
        4, // number of nodes
        1, // replicas
        "MasterSlave", RebalanceMode.FULL_AUTO, // use FULL_AUTO mode to test node tagging
        true); // do rebalance

    MockParticipantManager[] participants = new MockParticipantManager[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      participants[i].syncStart();
    }

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    HelixConfigScope resourceScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
            .forCluster(clusterName).forResource("TestDB1").build();
    configAccessor
        .set(resourceScope, ResourceConfig.ResourceConfigProperty.MONITORING_DISABLED.name(),
            "true");

    resourceScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
        .forCluster(clusterName).forResource("TestDB2").build();
    configAccessor
        .set(resourceScope, ResourceConfig.ResourceConfigProperty.MONITORING_DISABLED.name(),
            "false");

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    Thread.sleep(300);

    // Verify the bean was created for TestDB0, but not for TestDB1.
    Assert.assertTrue(_mbeanServer.isRegistered(getMbeanName("TestDB0", clusterName)));
    Assert.assertFalse(_mbeanServer.isRegistered(getMbeanName("TestDB1", clusterName)));
    Assert.assertTrue(_mbeanServer.isRegistered(getMbeanName("TestDB2", clusterName)));

    controller.syncStop();
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private ObjectName getMbeanName(String resourceName, String clusterName)
      throws MalformedObjectNameException {
    String clusterBeanName =
        String.format("%s=%s", ClusterStatusMonitor.CLUSTER_DN_KEY, clusterName);
    String resourceBeanName = String
        .format("%s,%s=%s", clusterBeanName, ClusterStatusMonitor.RESOURCE_DN_KEY, resourceName);
    return new ObjectName(
        String.format("%s: %s", MonitorDomainNames.ClusterStatus.name(), resourceBeanName));
  }
}
