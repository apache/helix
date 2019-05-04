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
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
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

    // Create cluster verifier
    ZkHelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient).build();

    // Create participant
    _gSetupTool.addInstanceToCluster(clusterName, instanceName);
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
    participant.syncStart();

    // Create controller, since this is the only controller, it will be the leader
    HelixManager manager1 = HelixManagerFactory.getZKHelixManager(clusterName,
        clusterName + "-manager1", InstanceType.CONTROLLER, ZK_ADDR);
    manager1.connect();
    Assert.assertTrue(manager1.isLeader());

    // Create resource
    _gSetupTool.addResourceToCluster(clusterName, resourceName, numPartition, stateModel,
        IdealState.RebalanceMode.SEMI_AUTO.name());

    // Rebalance Resource
    _gSetupTool.rebalanceResource(clusterName, resourceName, numReplica);

    // Wait for rebalance
    Assert.assertTrue(clusterVerifier.verifyByPolling());

    // Trigger missing top state in manager1
    participant.syncStop();

    Thread.sleep(1000);

    // Starting manager2
    HelixManager manager2 = HelixManagerFactory.getZKHelixManager(clusterName,
        clusterName + "-manager2", InstanceType.CONTROLLER, ZK_ADDR);
    manager2.connect();

    // Set leader to manager2
    setLeader(manager2);

    Assert.assertFalse(manager1.isLeader());
    Assert.assertTrue(manager2.isLeader());

    // Wait for rebalance
    Assert.assertTrue(clusterVerifier.verify());

    Thread.sleep(1000);
    setLeader(manager1);

    Assert.assertTrue(manager1.isLeader());
    Assert.assertFalse(manager2.isLeader());

    // Make resource top state to come back by restarting participant
    participant = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
    participant.syncStart();

    _gSetupTool.rebalanceResource(clusterName, resourceName, numReplica);

    Assert.assertTrue(clusterVerifier.verifyByPolling());

    // Resource lost top state, and manager1 lost leadership for 2000ms, because manager1 will
    // clean monitoring cache after re-gaining leadership, so max value of hand off duration should
    // not have such a large value
    Assert.assertTrue((long) beanServer.getAttribute(resourceMBeanObjectName,
        "PartitionTopStateHandoffDurationGauge.Max") < 500);

    participant.syncStop();
    manager1.disconnect();
    manager2.disconnect();
    deleteCluster(clusterName);
  }

  private void setLeader(HelixManager manager) throws Exception {
    System.out.println("Setting controller " + manager.getInstanceName() + " as leader");
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    final LiveInstance leader = new LiveInstance(manager.getInstanceName());
    leader.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
    leader.setSessionId(manager.getSessionId());
    leader.setHelixVersion(manager.getVersion());

    // Delete the current controller leader node so it will trigger leader election
    while (!manager.isLeader()) {
      accessor.getBaseDataAccessor().remove(
          PropertyPathBuilder.controllerLeader(manager.getClusterName()), AccessOption.EPHEMERAL);
      Thread.sleep(50);
    }
  }

  private ObjectName getResourceMonitorObjectName(String clusterName, String resourceName)
      throws Exception {
    return new ObjectName(String.format("%s:cluster=%s,resourceName=%s",
        MonitorDomainNames.ClusterStatus.name(), clusterName, resourceName));
  }
}
