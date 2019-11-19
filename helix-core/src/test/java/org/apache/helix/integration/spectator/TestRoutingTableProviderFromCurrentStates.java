package org.apache.helix.integration.spectator;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.RoutingTableProviderMonitor;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRoutingTableProviderFromCurrentStates extends ZkTestBase {
  private HelixManager _manager;
  private final int NUM_NODES = 10;
  protected int NUM_PARTITIONS = 20;
  protected int NUM_REPLICAS = 3;
  private final int START_PORT = 12918;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;
  private MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  @BeforeClass
  public void beforeClass() throws Exception {
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _participants = new MockParticipantManager[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    ConfigAccessor _configAccessor = _manager.getConfigAccessor();
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.enableTargetExternalView(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  @AfterClass
  public void afterClass() throws Exception {
    /*
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (int i = 0; i < NUM_NODES; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testRoutingTableWithCurrentStates() throws Exception {
    RoutingTableProvider routingTableEV =
        new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW);
    RoutingTableProvider routingTableCurrentStates =
        new RoutingTableProvider(_manager, PropertyType.CURRENTSTATES);

    try {
      String db1 = "TestDB-1";
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db1, NUM_PARTITIONS, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name());
      long startTime = System.currentTimeMillis();
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db1, NUM_REPLICAS);

      Thread.sleep(1000L);
      ZkHelixClusterVerifier clusterVerifier =
          new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
      Assert.assertTrue(clusterVerifier.verifyByPolling());
      validatePropagationLatency(PropertyType.CURRENTSTATES,
          System.currentTimeMillis() - startTime);

      IdealState idealState1 =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db1);
      validate(idealState1, routingTableEV, routingTableCurrentStates);

      // add new DB
      String db2 = "TestDB-2";
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db2, NUM_PARTITIONS, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name());
      startTime = System.currentTimeMillis();
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, NUM_REPLICAS);

      Thread.sleep(1000L);
      Assert.assertTrue(clusterVerifier.verifyByPolling());
      validatePropagationLatency(PropertyType.CURRENTSTATES,
          System.currentTimeMillis() - startTime);

      IdealState idealState2 =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db2);
      validate(idealState2, routingTableEV, routingTableCurrentStates);

      // shutdown an instance
      startTime = System.currentTimeMillis();
      _participants[0].syncStop();
      Thread.sleep(1000L);
      Assert.assertTrue(clusterVerifier.verifyByPolling());
      validatePropagationLatency(PropertyType.CURRENTSTATES,
          System.currentTimeMillis() - startTime);

      validate(idealState1, routingTableEV, routingTableCurrentStates);
      validate(idealState2, routingTableEV, routingTableCurrentStates);
    } finally {
      routingTableEV.shutdown();
      routingTableCurrentStates.shutdown();
    }
  }

  private ObjectName buildObjectName(PropertyType type) throws MalformedObjectNameException {
    return MBeanRegistrar.buildObjectName(MonitorDomainNames.RoutingTableProvider.name(),
        RoutingTableProviderMonitor.CLUSTER_KEY, CLUSTER_NAME,
        RoutingTableProviderMonitor.DATA_TYPE_KEY, type.name());
  }

  private void validatePropagationLatency(PropertyType type, final long upperBound)
      throws Exception {
    final ObjectName name = buildObjectName(type);
    Assert.assertTrue(TestHelper.verify(() -> {
      long stateLatency = (long) _beanServer.getAttribute(name, "StatePropagationLatencyGauge.Max");
      return stateLatency > 0 && stateLatency <= upperBound;
    }, 1000));
  }

  @Test(dependsOnMethods = {
      "testRoutingTableWithCurrentStates"
  })
  public void testWithSupportSourceDataType() {
    new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW).shutdown();
    new RoutingTableProvider(_manager, PropertyType.TARGETEXTERNALVIEW).shutdown();
    new RoutingTableProvider(_manager, PropertyType.CURRENTSTATES).shutdown();

    try {
      new RoutingTableProvider(_manager, PropertyType.IDEALSTATES).shutdown();
      Assert.fail();
    } catch (HelixException ex) {
      Assert.assertTrue(ex.getMessage().contains("Unsupported source data type"));
    }
  }

  private void validate(IdealState idealState, RoutingTableProvider routingTableEV,
      RoutingTableProvider routingTableCurrentStates) {
    String db = idealState.getResourceName();
    Set<String> partitions = idealState.getPartitionSet();
    for (String partition : partitions) {
      List<InstanceConfig> masterInsEv =
          routingTableEV.getInstancesForResource(db, partition, "MASTER");
      List<InstanceConfig> masterInsCs =
          routingTableCurrentStates.getInstancesForResource(db, partition, "MASTER");
      Assert.assertEquals(masterInsEv.size(), 1);
      Assert.assertEquals(masterInsCs.size(), 1);
      Assert.assertEquals(masterInsCs, masterInsEv);

      List<InstanceConfig> slaveInsEv =
          routingTableEV.getInstancesForResource(db, partition, "SLAVE");
      List<InstanceConfig> slaveInsCs =
          routingTableCurrentStates.getInstancesForResource(db, partition, "SLAVE");
      Assert.assertEquals(slaveInsEv.size(), 2);
      Assert.assertEquals(slaveInsCs.size(), 2);
      Assert.assertEquals(new HashSet(slaveInsCs), new HashSet(slaveInsEv));
    }
  }
}
