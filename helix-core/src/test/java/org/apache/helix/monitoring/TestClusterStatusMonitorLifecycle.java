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

import java.io.IOException;
import java.util.Date;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.model.IdealState;
import org.apache.helix.monitoring.mbeans.ClusterMBeanObserver;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterStatusMonitorLifecycle extends ZkTestBase {
  private static final Logger LOG = Logger.getLogger(TestClusterStatusMonitorLifecycle.class);

  MockParticipant[] _participants;
  MockMultiClusterController[] _controllers;
  String _controllerClusterName;
  String _clusterNamePrefix;
  String _firstClusterName;

  final int n = 5;
  final int clusterNb = 10;

  @BeforeClass
  public void beforeClass() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    _clusterNamePrefix = className + "_" + methodName;

    System.out.println("START " + _clusterNamePrefix + " at "
        + new Date(System.currentTimeMillis()));

    // setup 10 clusters
    for (int i = 0; i < clusterNb; i++) {
      String clusterName = _clusterNamePrefix + "0_" + i;
      String participantName = "localhost" + i;
      String resourceName = "TestDB" + i;
      TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
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
    TestHelper.setupCluster("CONTROLLER_" + _clusterNamePrefix, _zkaddr, 0, // controller
                                                                            // port
        "controller", // participant name prefix
        _clusterNamePrefix, // resource name prefix
        1, // resources
        clusterNb, // partitions per resource
        n, // number of nodes
        3, // replicas
        "LeaderStandby", true); // do rebalance

    // start multi-cluster controllers
    _controllers = new MockMultiClusterController[n + n];
    for (int i = 0; i < n; i++) {
      _controllers[i] =
          new MockMultiClusterController(_zkaddr, _controllerClusterName, "controller_" + i);
      _controllers[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(
            new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr, _controllerClusterName),
            30000);
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // start first cluster
    _participants = new MockParticipant[n];
    _firstClusterName = _clusterNamePrefix + "0_0";
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost0_" + (12918 + i);
      _participants[i] = new MockParticipant(_zkaddr, _firstClusterName, instanceName);
      _participants[i].syncStart();
    }

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            _firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");

    // add more controllers to controller cluster
    for (int i = 0; i < n; i++) {
      String controller = "controller_" + (n + i);
      _setupTool.addInstanceToCluster(_controllerClusterName, controller);
    }
    _setupTool.rebalanceStorageCluster(_controllerClusterName, _clusterNamePrefix + "0", 6);
    for (int i = n; i < 2 * n; i++) {
      _controllers[i] =
          new MockMultiClusterController(_zkaddr, _controllerClusterName, "controller_" + i);
      _controllers[i].syncStart();
    }

    // verify controller cluster
    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                _controllerClusterName));
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // verify first cluster
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            _firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");
  }

  @AfterClass
  public void afterClass() {
    System.out.println("Cleaning up...");
    for (int i = 0; i < 5; i++) {
      _controllers[i].syncStop();
    }

    for (int i = 0; i < 5; i++) {
      _participants[i].syncStop();
    }

    System.out.println("END " + _clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));

  }

  class ParticipantMonitorListener extends ClusterMBeanObserver {

    int _nMbeansUnregistered = 0;
    int _nMbeansRegistered = 0;

    public ParticipantMonitorListener(String domain) throws InstanceNotFoundException, IOException,
        MalformedObjectNameException, NullPointerException {
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

  @Test
  public void testClusterStatusMonitorLifecycle() throws InstanceNotFoundException,
      MalformedObjectNameException, NullPointerException, IOException, InterruptedException {
    ParticipantMonitorListener listener = new ParticipantMonitorListener("ClusterStatus");

    int nMbeansUnregistered = listener._nMbeansUnregistered;
    int nMbeansRegistered = listener._nMbeansRegistered;

    _participants[0].disconnect();

    // 1 participant goes away
    // No change in instance/resource mbean
    // Unregister 1 per-instance resource mbean and message queue mbean
    Thread.sleep(1000);
    Assert.assertEquals(nMbeansUnregistered, listener._nMbeansUnregistered - 2);
    Assert.assertEquals(nMbeansRegistered, listener._nMbeansRegistered);

    HelixDataAccessor accessor = _participants[n - 1].getHelixDataAccessor();
    String firstControllerName =
        accessor.getProperty(accessor.keyBuilder().controllerLeader()).getId();

    MockMultiClusterController firstController = null;
    for (MockMultiClusterController controller : _controllers) {
      if (controller.getInstanceName().equals(firstControllerName)) {
        firstController = controller;
      }
    }
    firstController.disconnect();
    Thread.sleep(1000);

    // 1 cluster status monitor, 1 resource monitor, 5 instances
    // Unregister 1+4+1 per-instance resource mbean
    // Register 4 per-instance resource mbean
    Assert.assertEquals(nMbeansUnregistered, listener._nMbeansUnregistered - 16);
    Assert.assertEquals(nMbeansRegistered, listener._nMbeansRegistered - 12);

    String instanceName = "localhost0_" + (12918 + 0);
    _participants[0] = new MockParticipant(_zkaddr, _firstClusterName, instanceName);
    _participants[0].syncStart();

    // 1 participant comes back
    // No change in instance/resource mbean
    // Register 1 per-instance resource mbean
    Thread.sleep(1000);
    Assert.assertEquals(nMbeansUnregistered, listener._nMbeansUnregistered - 16);
    Assert.assertEquals(nMbeansRegistered, listener._nMbeansRegistered - 14);

    // Add a resource
    // Register 1 resource mbean
    // Register 5 per-instance resource mbean
    IdealState idealState = accessor.getProperty(accessor.keyBuilder().idealStates("TestDB00"));

    _setupTool.addResourceToCluster(_firstClusterName, "TestDB1", idealState.getNumPartitions(),
        "MasterSlave");
    _setupTool.rebalanceResource(_firstClusterName, "TestDB1",
        Integer.parseInt(idealState.getReplicas()));

    Thread.sleep(1000);
    Assert.assertEquals(nMbeansUnregistered, listener._nMbeansUnregistered - 16);
    Assert.assertEquals(nMbeansRegistered, listener._nMbeansRegistered - 20);

    // Remove a resource
    // No change in instance/resource mbean
    // Unregister 5 per-instance resource mbean
    _setupTool.dropResourceFromCluster(_firstClusterName, "TestDB1");
    Thread.sleep(1000);
    Assert.assertEquals(nMbeansUnregistered, listener._nMbeansUnregistered - 22);
    Assert.assertEquals(nMbeansRegistered, listener._nMbeansRegistered - 20);

  }
}
