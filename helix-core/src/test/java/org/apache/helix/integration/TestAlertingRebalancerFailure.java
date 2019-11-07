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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
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
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.helix.monitoring.mbeans.ClusterStatusMonitor.CLUSTER_DN_KEY;
import static org.apache.helix.monitoring.mbeans.ClusterStatusMonitor.RESOURCE_DN_KEY;
import static org.apache.helix.util.StatusUpdateUtil.ErrorType.RebalanceResourceFailure;

public class TestAlertingRebalancerFailure extends ZkStandAloneCMTestBase {
  private static final long TIMEOUT = 180 * 1000L;
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

    // Clean up all JMX objects
    for (ObjectName mbean : _server.queryNames(null, null)) {
      try {
        _server.unregisterMBean(mbean);
      } catch (Exception e) {
        // OK
      }
    }

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
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

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    // Ensure error has been removed
    accessor.removeProperty(errorNodeKey);
  }

  @Test
  public void testParticipantUnavailable() throws Exception {
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, testDb, 5,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, 3);
    ZkHelixClusterVerifier verifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkAddr(ZK_ADDR).setResources(new HashSet<>(Collections.singleton(testDb))).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // disable then enable the resource to ensure no rebalancing error is generated during this
    // process
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, testDb);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, testDb, 5,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, 3);
    Assert.assertTrue(verifier.verifyByPolling());

    // Verify there is no rebalance error logged
    Assert.assertNull(accessor.getProperty(errorNodeKey));
    checkRebalanceFailureGauge(false);
    checkResourceBestPossibleCalFailureState(ResourceMonitor.RebalanceStatus.NORMAL, testDb);

    // kill nodes, so rebalance cannot be done
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].syncStop();
    }

    // Verify the rebalance error caused by no node available
    pollForError(accessor, errorNodeKey);
    checkRebalanceFailureGauge(true);
    checkResourceBestPossibleCalFailureState(
        ResourceMonitor.RebalanceStatus.BEST_POSSIBLE_STATE_CAL_FAILED, testDb);

    // clean up
    _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, testDb);
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i] =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _participants[i].getInstanceName());
      _participants[i].syncStart();
    }
  }

  @Test(dependsOnMethods = "testParticipantUnavailable")
  public void testTagSetIncorrect() throws Exception {
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, testDb, 5,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.name());
    ZkHelixClusterVerifier verifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkAddr(ZK_ADDR).setResources(new HashSet<>(Collections.singleton(testDb))).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // Verify there is no rebalance error logged
    Assert.assertNull(accessor.getProperty(errorNodeKey));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    checkRebalanceFailureGauge(false);
    checkResourceBestPossibleCalFailureState(ResourceMonitor.RebalanceStatus.NORMAL, testDb);

    // set expected instance tag
    IdealState is =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, testDb);
    is.setInstanceGroupTag("RandomTag");
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, testDb, is);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, 3);

    // Verify there is rebalance error logged
    pollForError(accessor, errorNodeKey);
    checkRebalanceFailureGauge(true);
    checkResourceBestPossibleCalFailureState(
        ResourceMonitor.RebalanceStatus.BEST_POSSIBLE_STATE_CAL_FAILED, testDb);

    // clean up
    _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, testDb);
  }

  @Test(dependsOnMethods = "testTagSetIncorrect")
  public void testWithDomainId() throws Exception {
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

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, testDb, 5,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.name(),
        CrushRebalanceStrategy.class.getName());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, replicas);
    ZkHelixClusterVerifier verifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkAddr(ZK_ADDR).setResources(new HashSet<>(Collections.singleton(testDb))).build();
    Assert.assertTrue(verifier.verifyByPolling());
    // Verify there is no rebalance error logged
    Assert.assertNull(accessor.getProperty(errorNodeKey));
    checkRebalanceFailureGauge(false);
    checkResourceBestPossibleCalFailureState(ResourceMonitor.RebalanceStatus.NORMAL, testDb);

    // 2. enable the rest nodes with no domain Id
    for (int i = replicas; i < NODE_NR; i++) {
      setInstanceEnable(_participants[i].getInstanceName(), true, configAccessor);
    }
    // Verify there is rebalance error logged
    pollForError(accessor, errorNodeKey);
    checkRebalanceFailureGauge(true);
    checkResourceBestPossibleCalFailureState(
        ResourceMonitor.RebalanceStatus.BEST_POSSIBLE_STATE_CAL_FAILED, testDb);

    // 3. reset all nodes domain Id to be correct setting
    for (int i = replicas; i < NODE_NR; i++) {
      setDomainId(_participants[i].getInstanceName(), configAccessor);
    }
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, replicas);

    Assert.assertTrue(_clusterVerifier.verify());

    // Verify that rebalance error state is removed
    checkRebalanceFailureGauge(false);
    checkResourceBestPossibleCalFailureState(ResourceMonitor.RebalanceStatus.NORMAL, testDb);

    // clean up
    _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, testDb);
    clusterConfig.setTopologyAwareEnabled(false);
  }

  private ObjectName getMbeanName(String clusterName) throws MalformedObjectNameException {
    String clusterBeanName = String.format("%s=%s", CLUSTER_DN_KEY, clusterName);
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), clusterBeanName));
  }

  private ObjectName getResourceMbeanName(String clusterName, String resourceName)
      throws MalformedObjectNameException {
    String resourceBeanName =
        String.format("%s=%s,%s=%s", CLUSTER_DN_KEY, clusterName, RESOURCE_DN_KEY, resourceName);
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), resourceBeanName));
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

  private void checkRebalanceFailureGauge(final boolean expectFailure) throws Exception {
    boolean result = TestHelper.verify(() -> {
      try {
        Long value =
            (Long) _server.getAttribute(getMbeanName(CLUSTER_NAME), "RebalanceFailureGauge");
        return value != null && (value == 1) == expectFailure;
      } catch (Exception e) {
        return false;
      }
    }, TIMEOUT);
    Assert.assertTrue(result);
  }

  private void checkResourceBestPossibleCalFailureState(
      final ResourceMonitor.RebalanceStatus expectedState, final String resourceName)
      throws Exception {
    boolean result = TestHelper.verify(() -> {
      try {
        String state = (String) _server
            .getAttribute(getResourceMbeanName(CLUSTER_NAME, resourceName), "RebalanceStatus");

        return state != null && state.equals(expectedState.name());
      } catch (Exception e) {
        return false;
      }
    }, TIMEOUT);
    Assert.assertTrue(result);
  }

  private void pollForError(final HelixDataAccessor accessor, final PropertyKey key)
      throws Exception {
    boolean result = TestHelper.verify(() -> {
      /*
       * TODO re-enable this check when we start recording rebalance error again
       * return accessor.getProperty(key) != null;
       */
      return true;
    }, TIMEOUT);
    Assert.assertTrue(result);
  }
}
