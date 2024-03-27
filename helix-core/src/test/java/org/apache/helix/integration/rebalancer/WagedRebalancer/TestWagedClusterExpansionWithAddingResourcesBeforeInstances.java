package org.apache.helix.integration.rebalancer.WagedRebalancer;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.model.BuiltInStateModelDefinitions.LeaderStandby;
import static org.apache.helix.monitoring.mbeans.ClusterStatusMonitor.CLUSTER_DN_KEY;

public class TestWagedClusterExpansionWithAddingResourcesBeforeInstances extends ZkTestBase {
  private static final long TIMEOUT = 10 * 1000L;
  protected static final AtomicLong PORT_GENERATOR = new AtomicLong(12918);
  protected static final int PARTITIONS = 4;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;
  protected HelixClusterVerifier _clusterVerifier;

  List<MockParticipantManager> _participants = new ArrayList<>();
  Set<String> _allDBs = new HashSet<>();
  int _replica = 3;

  @BeforeClass
  public void setupCluster() throws Exception {
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    clusterConfig.setDelayRebalaceEnabled(true);
    // Set a long enough time to ensure delayed rebalance is activate
    clusterConfig.setRebalanceDelayTime(3000000);

    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
    preference.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 0);
    preference.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 10);
    clusterConfig.setGlobalRebalancePreference(preference);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // create resource with instances
    String testResource1 = "Test-resource-1";
    createResource(testResource1, 4, 4, "Tag-1", true);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    enableTopologyAwareRebalance(_gZkClient, CLUSTER_NAME, true);
    _gSetupTool.rebalanceResource(CLUSTER_NAME, testResource1, _replica);
    _allDBs.add(testResource1);

    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).setResources(_allDBs)
        .build();
    Assert.assertTrue(_clusterVerifier.verify(12000));
  }

  private List<String> createResource(
      String resourceName, int numInstances, int numPartitions, String tagName, boolean enableParticipants) {
    List<String> nodes = new ArrayList<>();
    for (int i = 0; i < numInstances; i++) {
      nodes.add(addInstance(new ConfigAccessor(_gZkClient), "zone-" + i % numInstances, tagName, enableParticipants));
    }

    createResourceWithWagedRebalance(CLUSTER_NAME, resourceName, LeaderStandby.name(), numPartitions, _replica, _replica - 1);
    IdealState idealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName);
    idealState.setInstanceGroupTag(tagName);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, resourceName, idealState);
    return nodes;
  }

  private String addInstance(ConfigAccessor configAccessor, String zone, String instanceTag, boolean enabled) {
    String storageNodeName = PARTICIPANT_PREFIX + "_" + PORT_GENERATOR.incrementAndGet();
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    _gSetupTool.addInstanceTag(CLUSTER_NAME, storageNodeName, instanceTag);
    String domain = String.format("zone=%s,instance=%s", zone, storageNodeName);

    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(CLUSTER_NAME, storageNodeName);
    instanceConfig.setDomain(domain);
    instanceConfig.setInstanceEnabled(enabled);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, storageNodeName, instanceConfig);

    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
    if (enabled) {
      // start dummy participant
      participant.syncStart();
    }
    _participants.add(participant);

    return storageNodeName;
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void  testExpandClusterWithResourceWithoutInstances() throws Exception {
    // Set-up a WAGED resource without any instances and let cluster rebalance successfully.
    String testResource2 = "Test-resource-2";
    String testResourceTagName = "Tag-2";
    createResource(testResource2, 0, 0, testResourceTagName, false);

    _gSetupTool.rebalanceResource(CLUSTER_NAME, testResource2, _replica);
    _allDBs.add(testResource2);

    ZkHelixClusterVerifier _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkClient(_gZkClient)
        .setResources(_allDBs)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    checkRebalanceFailureGauge(false);
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

  private ObjectName getMbeanName(String clusterName) throws MalformedObjectNameException {
    String clusterBeanName = String.format("%s=%s", CLUSTER_DN_KEY, clusterName);
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), clusterBeanName));
  }

}
