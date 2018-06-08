package org.apache.helix.integration.controller;

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
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestControllerLeadershipChange extends ZkTestBase {

  @Test
  public void testMissingTopStateDurationMonitoring() throws Exception {
    String clusterName = "testCluster-TestControllerLeadershipChange";
    String instanceName = clusterName + "-participant";
    String resourceName = "testResource";
    int numPartition = 1;
    int numReplica = 1;
    String stateModel = "LeaderStandby";
    ObjectName resourceMBeanObjectName = getResourceMonitorObjectName(clusterName, resourceName);
    MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

    // Create cluster
    _gSetupTool.addCluster(clusterName, true);

    // Create participant
    _gSetupTool.addInstanceToCluster(clusterName, instanceName);
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
    participant.syncStart();

    // Create controller, since this is the only controller, it will be the leader
    HelixManager manager1 = HelixManagerFactory
        .getZKHelixManager(clusterName, clusterName + "-manager1", InstanceType.CONTROLLER,
            ZK_ADDR);
    manager1.connect();
    Assert.assertTrue(manager1.isLeader());

    // Create resource
    _gSetupTool.addResourceToCluster(clusterName, resourceName, numPartition, stateModel,
        IdealState.RebalanceMode.SEMI_AUTO.name());

    // Rebalance Resource
    _gSetupTool
        .rebalanceResource(clusterName, resourceName, numReplica);
    // Wait for rebalance
    Thread.sleep(2000);

    // Trigger missing top state in manager1
    participant.syncStop();

    Thread.sleep(2000);

    // Starting manager2
    HelixManager manager2 = HelixManagerFactory
        .getZKHelixManager(clusterName, clusterName + "-manager2", InstanceType.CONTROLLER,
            ZK_ADDR);
    manager2.connect();
    Assert.assertFalse(manager2.isLeader());

    // Set leader to manager2
    setLeader(manager2);

    Assert.assertFalse(manager1.isLeader());
    Assert.assertTrue(manager2.isLeader());

    // Make resource top state to come back
    participant = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
    participant.syncStart();

    // Wait for rebalance
    Thread.sleep(2000);
    setLeader(manager1);

    Assert.assertTrue(manager1.isLeader());
    Assert.assertFalse(manager2.isLeader());

    _gSetupTool.rebalanceResource(clusterName, resourceName, numReplica);

    // Wait for manager1 to update
    Thread.sleep(2000);

    // Resource lost top state, and manager1 lost leadership for 4000ms, because manager1 will
    // clean monitoring cache after re-gaining leadership, so max value of hand off duration should
    // not have such a large value
    Assert.assertTrue((long) beanServer
        .getAttribute(resourceMBeanObjectName, "PartitionTopStateHandoffDurationGauge.Max") < 500);
  }

  private void setLeader(HelixManager manager) {
    System.out.println("Setting controller " + manager.getInstanceName() + " as leader");
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    final LiveInstance leader = new LiveInstance(manager.getInstanceName());
    leader.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
    leader.setSessionId(manager.getSessionId());
    leader.setHelixVersion(manager.getVersion());

    // Delete the current controller leader node so it will trigger leader election
    accessor.getBaseDataAccessor().remove(PropertyPathBuilder.controllerLeader(manager.getClusterName()), AccessOption.EPHEMERAL);

    // No matter who gets leadership, force the given manager to become leader
    // Note there is theoretically a racing condition that GenericHelixController.onControllerChange()
    // will not catch this new value when it's double checking leadership, but it's stable enough
    accessor.getBaseDataAccessor().set(PropertyPathBuilder.controllerLeader(manager.getClusterName()), leader.getRecord(), AccessOption.EPHEMERAL);
  }

  private ObjectName getResourceMonitorObjectName(String clusterName, String resourceName)
      throws Exception {
    return new ObjectName(String
        .format("%s:cluster=%s,resourceName=%s", MonitorDomainNames.ClusterStatus.name(),
            clusterName, resourceName));
  }

}
