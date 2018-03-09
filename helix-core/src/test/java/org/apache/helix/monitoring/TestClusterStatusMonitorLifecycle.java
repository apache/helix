package org.apache.helix.monitoring;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.monitoring.mbeans.ClusterMBeanObserver;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class TestClusterStatusMonitorLifecycle extends ZkIntegrationTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestClusterStatusMonitorLifecycle.class);

  MockParticipantManager[] _participants;
  ClusterDistributedController[] _controllers;
  String _controllerClusterName;
  String _clusterNamePrefix;
  String _firstClusterName;

  final int n = 5;
  final int clusterNb = 10;

  @BeforeClass
  public void beforeClass() throws Exception {
    String className = TestHelper.getTestClassName();
    _clusterNamePrefix = className;

    System.out
        .println("START " + _clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));

    // setup 10 clusters
    for (int i = 0; i < clusterNb; i++) {
      String clusterName = _clusterNamePrefix + "0_" + i;
      String participantName = "localhost" + i;
      String resourceName = "TestDB" + i;
      TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
          participantName, // participant name prefix
          resourceName, // resource name prefix
          1, // resources
          8, // partitions per resource
          n, // number of nodes
          3, // replicas
          "MasterSlave", true); // do rebalance
    }

    // setup controller cluster
    _controllerClusterName = "CONTROLLER_" + _clusterNamePrefix;
    TestHelper.setupCluster(_controllerClusterName, ZK_ADDR, // controller
        0, // port
        "controller", // participant name prefix
        _clusterNamePrefix, // resource name prefix
        1, // resources
        clusterNb, // partitions per resource
        n, // number of nodes
        3, // replicas
        "LeaderStandby", true); // do rebalance

    // start distributed cluster controllers
    _controllers = new ClusterDistributedController[n + n];
    for (int i = 0; i < n; i++) {
      _controllers[i] =
          new ClusterDistributedController(ZK_ADDR, _controllerClusterName, "controller_" + i);
      _controllers[i].syncStart();
    }

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _controllerClusterName),
        30000);
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // start first cluster
    _participants = new MockParticipantManager[n];
    _firstClusterName = _clusterNamePrefix + "0_0";
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost0_" + (12918 + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _firstClusterName, instanceName);
      _participants[i].syncStart();
    }

    result = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, _firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");

    // add more controllers to controller cluster
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    for (int i = 0; i < n; i++) {
      String controller = "controller_" + (n + i);
      setupTool.addInstanceToCluster(_controllerClusterName, controller);
    }
    setupTool.rebalanceStorageCluster(_controllerClusterName, _clusterNamePrefix + "0", 6);
    for (int i = n; i < 2 * n; i++) {
      _controllers[i] =
          new ClusterDistributedController(ZK_ADDR, _controllerClusterName, "controller_" + i);
      _controllers[i].syncStart();
    }

    // verify controller cluster
    result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _controllerClusterName));
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // verify first cluster
    result = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, _firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");
  }

  @AfterClass
  public void afterClass() {
    System.out.println("Cleaning up...");
    for (int i = 0; i < _participants.length; i++) {
      _participants[i].syncStop();
    }
    System.out.println("END " + _clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));
  }

  class ParticipantMonitorListener extends ClusterMBeanObserver {

    int _nMbeansUnregistered = 0;
    int _nMbeansRegistered = 0;

    public ParticipantMonitorListener(String domain)
        throws InstanceNotFoundException, IOException, MalformedObjectNameException,
        NullPointerException {
      super(domain);
    }

    @Override
    public void onMBeanRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      LOG.info("Register mbean: " + mbsNotification.getMBeanName());
      _nMbeansRegistered++;
    }

    @Override
    public void onMBeanUnRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      LOG.info("Unregister mbean: " + mbsNotification.getMBeanName());
      _nMbeansUnregistered++;
    }
  }

  private void cleanupControllers() {
    for (int i = 0; i < _controllers.length; i++) {
      _controllers[i].syncStop();
    }
  }

  @Test
  public void testClusterStatusMonitorLifecycle()
      throws InstanceNotFoundException, MalformedObjectNameException, NullPointerException,
      IOException, InterruptedException {
    // Filter other unrelated clusters' metrics
    QueryExp exp =
        Query.match(Query.attr("SensorName"), Query.value("*" + _clusterNamePrefix + "*"));
    Set<ObjectInstance> mbeans = new HashSet<>(ManagementFactory.getPlatformMBeanServer()
        .queryMBeans(new ObjectName("ClusterStatus:*"), exp));

    _participants[0].disconnect();

    // 1 participant goes away
    // No change in instance/resource mbean
    // Unregister 1 per-instance resource mbean and message queue mbean
    Thread.sleep(1000);
    int previousMBeanCount = mbeans.size();
    mbeans = new HashSet<>(ManagementFactory.getPlatformMBeanServer()
        .queryMBeans(new ObjectName("ClusterStatus:*"), exp));
    Assert.assertEquals(mbeans.size(), previousMBeanCount - 2);

    HelixDataAccessor accessor = _participants[n - 1].getHelixDataAccessor();
    String firstControllerName =
        accessor.getProperty(accessor.keyBuilder().controllerLeader()).getId();

    ClusterDistributedController firstController = null;
    for (ClusterDistributedController controller : _controllers) {
      if (controller.getInstanceName().equals(firstControllerName)) {
        firstController = controller;
      }
    }
    firstController.disconnect();

    // 1 controller goes away
    // 1 message queue mbean, 1 PerInstanceResource mbean, and one message queue mbean
    Thread.sleep(2000);
    previousMBeanCount = mbeans.size();
    mbeans = new HashSet<>(ManagementFactory.getPlatformMBeanServer()
        .queryMBeans(new ObjectName("ClusterStatus:*"), exp));
    Assert.assertEquals(mbeans.size(), previousMBeanCount - 3);

    String instanceName = "localhost0_" + (12918 + 0);
    _participants[0] = new MockParticipantManager(ZK_ADDR, _firstClusterName, instanceName);
    _participants[0].syncStart();

    // 1 participant comes back
    // No change in instance/resource mbean
    // Register 1 per-instance resource mbean and 1 message queue mbean
    Thread.sleep(2000);
    previousMBeanCount = mbeans.size();
    mbeans = new HashSet<>(ManagementFactory.getPlatformMBeanServer()
        .queryMBeans(new ObjectName("ClusterStatus:*"), exp));
    Assert.assertEquals(mbeans.size(), previousMBeanCount + 2);

    // Add a resource
    // Register 1 resource mbean
    // Register 5 per-instance resource mbean
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    IdealState idealState = accessor.getProperty(accessor.keyBuilder().idealStates("TestDB00"));

    setupTool.addResourceToCluster(_firstClusterName, "TestDB1", idealState.getNumPartitions(),
        "MasterSlave");
    setupTool.rebalanceResource(_firstClusterName, "TestDB1",
        Integer.parseInt(idealState.getReplicas()));

    Thread.sleep(2000);
    // Add one resource, PerInstanceResource mbeans and 1 resource monitor
    previousMBeanCount = mbeans.size();
    mbeans = new HashSet<>(ManagementFactory.getPlatformMBeanServer()
        .queryMBeans(new ObjectName("ClusterStatus:*"), exp));
    Assert.assertEquals(mbeans.size(), previousMBeanCount + _participants.length + 1);

    // Remove a resource
    // No change in instance/resource mbean
    // Unregister 5 per-instance resource mbean
    setupTool.dropResourceFromCluster(_firstClusterName, "TestDB1");
    Thread.sleep(2000);
    previousMBeanCount = mbeans.size();
    mbeans = new HashSet<>(ManagementFactory.getPlatformMBeanServer()
        .queryMBeans(new ObjectName("ClusterStatus:*"), exp));
    Assert.assertEquals(mbeans.size(), previousMBeanCount - (_participants.length + 1));

    // Cleanup controllers then MBeans should all be removed.
    cleanupControllers();
    Thread.sleep(2000);

    // Check if any MBeans leftover.
    // Note that MessageQueueStatus is not bound with controller only. So it will still exist.
    exp = Query
        .and(Query.not(Query.match(Query.attr("SensorName"), Query.value("MessageQueueStatus.*"))),
            exp);
    if (!ManagementFactory.getPlatformMBeanServer()
        .queryMBeans(new ObjectName("ClusterStatus:*"), exp).isEmpty()) {
      System.out.println(ManagementFactory.getPlatformMBeanServer()
          .queryMBeans(new ObjectName("ClusterStatus:*"), exp));
    }
    Assert.assertTrue(ManagementFactory.getPlatformMBeanServer()
        .queryMBeans(new ObjectName("ClusterStatus:*"), exp).isEmpty());
  }
}
