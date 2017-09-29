package org.apache.helix.integration;

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
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.apache.helix.monitoring.mbeans.ClusterStatusMonitor.CLUSTER_DN_KEY;
import static org.apache.helix.util.StatusUpdateUtil.ErrorType.RebalanceResourceFailure;

public class TestAlertingRebalancerFailure extends ZkStandAloneCMTestBase {
  private static final Set<String> _instanceNames = new HashSet<>();
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();
  protected static final int NODE_NR = 3;
  private static String testDb = "TestDB_AlertingRebalancerFailure";

  private ZKHelixDataAccessor accessor;
  private PropertyKey errorNodeKey;

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);
    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _instanceNames.add(instanceName);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    errorNodeKey = accessor.keyBuilder().controllerTaskError(RebalanceResourceFailure.name());
  }

  @BeforeMethod
  public void beforeMethod() {
    // Ensure error has been removed
    accessor.removeProperty(errorNodeKey);
  }

  @Test
  public void testParticipantUnavailable() {
    _setupTool.addResourceToCluster(CLUSTER_NAME, testDb, 5,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.name());
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, 3);
    HelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(new HashSet<>(Collections.singleton(testDb))).build();
    Assert.assertTrue(verifier.verify());

    // disable then enable the resource to ensure no rebalancing error is generated during this process
    _setupTool.dropResourceFromCluster(CLUSTER_NAME, testDb);
    _setupTool.addResourceToCluster(CLUSTER_NAME, testDb, 5,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.name());
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, 3);
    Assert.assertTrue(verifier.verify());

    // Verify there is no rebalance error logged
    Assert.assertNull(accessor.getProperty(errorNodeKey));
    checkRebalanceFailureGauge(false);

    // kill nodes, so rebalance cannot be done
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].syncStop();
    }

    // Verify the rebalance error caused by no node available
    Assert.assertNotNull(pollForError(accessor, errorNodeKey));
    checkRebalanceFailureGauge(true);

    // clean up
    _setupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, testDb);
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i] =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _participants[i].getInstanceName());
      _participants[i].syncStart();
    }
  }

  @Test
  public void testTagSetIncorrect() {
    _setupTool.addResourceToCluster(CLUSTER_NAME, testDb, 5,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.name());
    // set expected instance tag
    IdealState is =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, testDb);
    is.setInstanceGroupTag("RandomTag");
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, testDb, is);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, 3);

    // Verify there is rebalance error logged
    Assert.assertNotNull(pollForError(accessor, errorNodeKey));
    checkRebalanceFailureGauge(true);

    // clean up
    _setupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, testDb);
  }

  @Test
  public void testWithDomainId() throws InterruptedException {
    int replicas = 2;
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    // 1. disable all participants except one node, then set domain Id
    for (int i = NODE_NR - 1; i >= 0; i--) {
      if (i < replicas) {
        setDomainId(_participants[i].getInstanceName(), configAccessor);
      } else {
        setInstanceEnable(_participants[i].getInstanceName(), false, configAccessor);
      }
    }

    // enable topology aware
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/Rack/Instance");
    clusterConfig.setFaultZoneType("Rack");
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Ensure error caused by node config changes has been removed.
    // Error may be recorded unexpectedly when a resource from other tests is not cleaned up.
    accessor.removeProperty(errorNodeKey);

    _setupTool.addResourceToCluster(CLUSTER_NAME, testDb, 5,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.name(),
        CrushRebalanceStrategy.class.getName());
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, replicas);
    HelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(new HashSet<>(Collections.singleton(testDb))).build();
    Assert.assertTrue(verifier.verify());
    // Verify there is no rebalance error logged
    Assert.assertNull(accessor.getProperty(errorNodeKey));
    checkRebalanceFailureGauge(false);

    // 2. enable the rest nodes with no domain Id
    for (int i = replicas; i < NODE_NR; i++) {
      setInstanceEnable(_participants[i].getInstanceName(), true, configAccessor);
    }
    // Verify there is rebalance error logged
    Assert.assertNotNull(pollForError(accessor, errorNodeKey));
    checkRebalanceFailureGauge(true);

    // 3. reset all nodes domain Id to be correct setting
    for (int i = replicas; i < NODE_NR; i++) {
      setDomainId(_participants[i].getInstanceName(), configAccessor);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, replicas);
    Thread.sleep(1000);
    // Verify that rebalance error state is removed
    checkRebalanceFailureGauge(false);

    // clean up
    _setupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, testDb);
    clusterConfig.setTopologyAwareEnabled(false);
  }

  private ObjectName getMbeanName(String clusterName) throws MalformedObjectNameException {
    String clusterBeanName = String.format("%s=%s", CLUSTER_DN_KEY, clusterName);
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), clusterBeanName));
  }

  private void setDomainId(String instanceName, ConfigAccessor configAccessor) {
    String domain = String.format("Rack=%s, Instance=%s", instanceName, instanceName);
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
    instanceConfig.setDomain(domain);
    configAccessor.setInstanceConfig(CLUSTER_NAME, instanceName, instanceConfig);
  }

  private void setInstanceEnable(String instanceName, boolean enabled,
      ConfigAccessor configAccessor) {
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
    instanceConfig.setInstanceEnabled(enabled);
    configAccessor.setInstanceConfig(CLUSTER_NAME, instanceName, instanceConfig);
  }

  private void checkRebalanceFailureGauge(boolean expectFailure) {
    try {
      Long value = (Long) _server.getAttribute(getMbeanName(CLUSTER_NAME), "RebalanceFailureGauge");
      Assert.assertNotNull(value);
      Assert.assertEquals(value == 1, expectFailure);
    } catch (Exception e) {
      Assert.fail("Failed to get attribute!");
    }
  }

  private HelixProperty pollForError(HelixDataAccessor accessor, PropertyKey key) {
    final int POLL_TIMEOUT = 5000;
    final int POLL_INTERVAL = 100;
    HelixProperty property = accessor.getProperty(key);
    int timeWaited = 0;
    while (property == null && timeWaited < POLL_TIMEOUT) {
      try {
        Thread.sleep(POLL_INTERVAL);
      } catch (InterruptedException e) {
        return null;
      }
      timeWaited += POLL_INTERVAL;
      property = accessor.getProperty(key);
    }
    return property;
  }
}
