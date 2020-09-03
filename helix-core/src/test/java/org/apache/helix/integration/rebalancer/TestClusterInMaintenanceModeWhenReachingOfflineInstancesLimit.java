package org.apache.helix.integration.rebalancer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.monitoring.mbeans.ClusterStatusMonitor.CLUSTER_DN_KEY;
import static org.apache.helix.util.StatusUpdateUtil.ErrorType.RebalanceResourceFailure;

public class TestClusterInMaintenanceModeWhenReachingOfflineInstancesLimit extends ZkTestBase {
  private static final int NUM_NODE = 10;
  private static final int START_PORT = 12918;
  private static final int _PARTITIONS = 5;
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private ClusterControllerManager _controller;

  private List<MockParticipantManager> _participants = new ArrayList<>();
  private HelixDataAccessor _dataAccessor;
  private int _maxOfflineInstancesAllowed = 4;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);

      // start dummy participants
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    ZkHelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxOfflineInstancesAllowed(_maxOfflineInstancesAllowed);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    for (int i = 0; i < 3; i++) {
      String db = "Test-DB-" + i++;
      createResourceWithDelayedRebalance(CLUSTER_NAME, db,
          BuiltInStateModelDefinitions.MasterSlave.name(), _PARTITIONS, 3, 3, -1);
    }
    Thread.sleep(100);
    Assert.assertTrue(clusterVerifier.verifyByPolling());
  }

  @AfterMethod
  public void afterMethod() {
    cleanupRebalanceError();
  }

  @Test
  public void testWithDisabledInstancesLimit() throws Exception {
    MaintenanceSignal maintenanceSignal =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
    Assert.assertNull(maintenanceSignal);

    checkForRebalanceError(false);

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);

    // disable instance
    int i;
    for (i = 2; i < 2 + _maxOfflineInstancesAllowed; i++) {
      String instance = _participants.get(i).getInstanceName();
      admin.enableInstance(CLUSTER_NAME, instance, false);
    }

    Thread.sleep(500);

    maintenanceSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
    Assert.assertNull(maintenanceSignal);

    String instance = _participants.get(i).getInstanceName();
    admin.enableInstance(CLUSTER_NAME, instance, false);

    Thread.sleep(500);
    maintenanceSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertNotNull(maintenanceSignal.getReason());

    checkForRebalanceError(true);

    for (i = 2; i < 2 + _maxOfflineInstancesAllowed + 1; i++) {
      instance = _participants.get(i).getInstanceName();
      admin.enableInstance(CLUSTER_NAME, instance, true);
    }
    admin.enableMaintenanceMode(CLUSTER_NAME, false);
  }

  @Test(dependsOnMethods = "testWithDisabledInstancesLimit")
  public void testWithOfflineInstancesLimit() throws Exception {
    MaintenanceSignal maintenanceSignal =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
    Assert.assertNull(maintenanceSignal);

    checkForRebalanceError(false);

    int i;
    for (i = 2; i < 2 + _maxOfflineInstancesAllowed; i++) {
      _participants.get(i).syncStop();
    }

    Thread.sleep(500);

    maintenanceSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
    Assert.assertNull(maintenanceSignal);

    _participants.get(i).syncStop();

    Thread.sleep(500);
    maintenanceSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertNotNull(maintenanceSignal.getReason());

    // Verify there is rebalance error logged
    checkForRebalanceError(true);
  }

  @AfterClass
  public void afterClass() throws Exception {
    /*
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  private void checkForRebalanceError(final boolean expectError) throws Exception {
    boolean result = TestHelper.verify(() -> {
      /*
       * TODO re-enable this check when we start recording rebalance error again
       * ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
       * PropertyKey errorNodeKey =
       * accessor.keyBuilder().controllerTaskError(RebalanceResourceFailure.name());
       * Assert.assertEquals(accessor.getProperty(errorNodeKey) != null, expectError);
       */
      Long value =
          (Long) _server.getAttribute(getClusterMbeanName(CLUSTER_NAME), "RebalanceFailureGauge");
      return expectError == (value != null && value > 0);
    }, 5000);
    Assert.assertTrue(result);
  }

  private void cleanupRebalanceError() {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    PropertyKey errorNodeKey =
        accessor.keyBuilder().controllerTaskError(RebalanceResourceFailure.name());
    accessor.removeProperty(errorNodeKey);
  }

  private ObjectName getClusterMbeanName(String clusterName) throws MalformedObjectNameException {
    String clusterBeanName = String.format("%s=%s", CLUSTER_DN_KEY, clusterName);
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), clusterBeanName));
  }
}
