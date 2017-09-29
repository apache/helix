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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static org.apache.helix.monitoring.mbeans.ClusterStatusMonitor.CLUSTER_DN_KEY;
import static org.apache.helix.util.StatusUpdateUtil.ErrorType.RebalanceResourceFailure;

public class TestPauseClusterWhenReachingOfflineInstancesLimit extends ZkIntegrationTestBase {
  static final int NUM_NODE = 10;
  static final int START_PORT = 12918;
  static final int _PARTITIONS = 5;
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();

  final String CLASS_NAME = getShortClassName();
  final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private ClusterControllerManager _controller;

  private List<MockParticipantManager> _participants = new ArrayList<MockParticipantManager>();
  private HelixClusterVerifier _clusterVerifier;
  private HelixDataAccessor _dataAccessor;
  private int _maxOfflineInstancesAllowed = 4;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
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

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();

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
    Assert.assertTrue(_clusterVerifier.verify());
  }

  @Test
  public void testWithDisabledInstancesLimit() throws Exception {
    PauseSignal pauseSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().pause());
    Assert.assertNull(pauseSignal);

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);

    // disable instance
    int i;
    for (i = 2; i < 2 + _maxOfflineInstancesAllowed; i++) {
      String instance = _participants.get(i).getInstanceName();
      admin.enableInstance(CLUSTER_NAME, instance, false);
    }

    Thread.sleep(500);

    pauseSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().pause());
    Assert.assertNull(pauseSignal);

    String instance = _participants.get(i).getInstanceName();
    admin.enableInstance(CLUSTER_NAME, instance, false);

    Thread.sleep(500);
    pauseSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().pause());
    Assert.assertNotNull(pauseSignal);
    Assert.assertNotNull(pauseSignal.getReason());

    for (i = 2; i < 2 + _maxOfflineInstancesAllowed + 1; i++) {
      instance = _participants.get(i).getInstanceName();
      admin.enableInstance(CLUSTER_NAME, instance, true);
    }
    admin.enableCluster(CLUSTER_NAME, true);
  }


  @Test (dependsOnMethods = "testWithDisabledInstancesLimit")
  public void testWithOfflineInstancesLimit() throws Exception {
    PauseSignal pauseSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().pause());
    Assert.assertNull(pauseSignal);
    int i;
    for (i = 2; i < 2 + _maxOfflineInstancesAllowed; i++) {
      _participants.get(i).syncStop();
    }

    Thread.sleep(500);

    pauseSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().pause());
    Assert.assertNull(pauseSignal);

    _participants.get(i).syncStop();

    Thread.sleep(500);
    pauseSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().pause());
    Assert.assertNotNull(pauseSignal);
    Assert.assertNotNull(pauseSignal.getReason());

    // Verify there is no rebalance error logged
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    PropertyKey errorNodeKey =
        accessor.keyBuilder().controllerTaskError(RebalanceResourceFailure.name());
    Assert.assertNotNull(accessor.getProperty(errorNodeKey));

    Long value =
        (Long) _server.getAttribute(getMbeanName(CLUSTER_NAME), "RebalanceFailureGauge");
    Assert.assertNotNull(value);
    Assert.assertTrue(value.longValue() > 0);
  }

  @AfterClass
  public void afterClass() throws Exception {
    /**
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      if (participant.isConnected()) {
        participant.syncStop();
      }
    }
    _gSetupTool.deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  private ObjectName getMbeanName(String clusterName)
      throws MalformedObjectNameException {
    String clusterBeanName =
        String.format("%s=%s", CLUSTER_DN_KEY, clusterName);
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), clusterBeanName));
  }

}
