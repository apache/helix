package org.apache.helix.rest.server.service;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.cloud.azure.AzureConstants;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.helix.cloud.constants.VirtualTopologyGroupConstants.*;
import static org.mockito.Mockito.*;


public class TestVirtualTopologyGroupService {
  private static final String TEST_CLUSTER = "Test_Cluster";
  private static final String TEST_CLUSTER0 = "TestCluster_0";
  private static final String TEST_CLUSTER1 = "TestCluster_1";

  private final ConfigAccessor _configAccessor = mock(ConfigAccessor.class);
  private final HelixDataAccessor _dataAccessor = mock(HelixDataAccessor.class);
  private InstanceConfig _instanceConfig0;
  private InstanceConfig _instanceConfig1;
  private InstanceConfig _instanceConfig2;
  private Map<String, DataUpdater<ZNRecord>> _updaterMap;
  private HelixAdmin _helixAdmin;
  private VirtualTopologyGroupService _service;

  @BeforeTest
  public void prepare() {
    Map<String, Set<String>> assignment = new HashMap<>();
    _instanceConfig0 = new InstanceConfig("instance_0");
    _instanceConfig0.setDomain("helixZoneId=zone0");
    _instanceConfig1 = new InstanceConfig("instance_1");
    _instanceConfig1.setDomain("helixZoneId=zone0");
    _instanceConfig2 = new InstanceConfig("instance_2");
    _instanceConfig2.setDomain("helixZoneId=zone1");

    assignment.put("virtual_group_0", ImmutableSet.of("instance_0", "instance_1"));
    assignment.put("virtual_group_1", ImmutableSet.of("instance_2"));
    _updaterMap = VirtualTopologyGroupService.createInstanceConfigUpdater(TEST_CLUSTER, assignment);

    ClusterConfig clusterConfig = new ClusterConfig(TEST_CLUSTER0);
    clusterConfig.setFaultZoneType(AzureConstants.AZURE_FAULT_ZONE_TYPE);
    clusterConfig.setTopology(AzureConstants.AZURE_TOPOLOGY);
    clusterConfig.setTopologyAwareEnabled(true);
    when(_configAccessor.getClusterConfig(TEST_CLUSTER0)).thenReturn(clusterConfig);

    _helixAdmin = mock(HelixAdmin.class);
    when(_helixAdmin.isInMaintenanceMode(anyString())).thenReturn(true);

    boolean[] results = new boolean[2];
    results[0] = results[1] = true;
    when(_dataAccessor.updateChildren(anyList(), anyList(), anyInt())).thenReturn(results);
    ClusterService clusterService = mock(ClusterService.class);
    when(clusterService.getClusterTopology(anyString())).thenReturn(prepareClusterTopology());
    _service = new VirtualTopologyGroupService(_helixAdmin, clusterService, _configAccessor, _dataAccessor);
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = "Topology-aware rebalance is not enabled.*")
  public void testTopologyAwareEnabledSetup() {
    when(_configAccessor.getClusterConfig(TEST_CLUSTER1)).thenReturn(new ClusterConfig(TEST_CLUSTER1));
    _service.addVirtualTopologyGroup(TEST_CLUSTER1, ImmutableMap.of(GROUP_NAME, "test-group", GROUP_NUMBER, "2"));
  }

  @Test
  public void testVirtualTopologyGroupService() {
    _service.addVirtualTopologyGroup(TEST_CLUSTER0, ImmutableMap.of(
        GROUP_NAME, "test-group", GROUP_NUMBER, "2", AUTO_MAINTENANCE_MODE_DISABLED, "true"));
    verify(_dataAccessor, times(1)).updateChildren(anyList(), anyList(), anyInt());
    verify(_configAccessor, times(1)).updateClusterConfig(anyString(), any());
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = "This operation is not allowed if cluster is already in maintenance mode.*")
  public void testMaintenanceModeCheckBeforeApiCall() {
    _service.addVirtualTopologyGroup(TEST_CLUSTER0, ImmutableMap.of(GROUP_NAME, "test-group", GROUP_NUMBER, "2"));
  }

  @Test(expectedExceptions = IllegalStateException.class,
  expectedExceptionsMessageRegExp = "Cluster is not in maintenance mode. This is required for virtual topology group setting. "
      + "Please set autoMaintenanceModeDisabled=false.*")
  public void testMaintenanceModeCheckAfter() {
    try {
      when(_helixAdmin.isInMaintenanceMode(anyString())).thenReturn(false);
      _service.addVirtualTopologyGroup(TEST_CLUSTER0, ImmutableMap.of(GROUP_NAME, "test-group", GROUP_NUMBER, "2"));
    } finally {
      when(_helixAdmin.isInMaintenanceMode(anyString())).thenReturn(true);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "Number of virtual groups cannot be greater than the number of instances.*")
  public void testNumberOfInstanceCheck() {
    _service.addVirtualTopologyGroup(TEST_CLUSTER0, ImmutableMap.of(
        GROUP_NAME, "test-group", GROUP_NUMBER, "10", AUTO_MAINTENANCE_MODE_DISABLED, "true"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParamValidation() {
    _service.addVirtualTopologyGroup(TEST_CLUSTER0, ImmutableMap.of(GROUP_NUMBER, "2"));
  }

  @Test(dataProvider = "instanceTestProvider")
  public void testInstanceConfigUpdater(String zkPath, InstanceConfig instanceConfig, Map<String, String> expectedDomain) {
    ZNRecord update = _updaterMap.get(zkPath).update(instanceConfig.getRecord());
    InstanceConfig updatedConfig = new InstanceConfig(update);
    Assert.assertEquals(updatedConfig.getDomainAsMap(), expectedDomain);
  }

  @DataProvider
  public Object[][] instanceTestProvider() {
    return new Object[][] {
        {computeZkPath("instance_0"), _instanceConfig0,
            ImmutableMap.of("helixZoneId", "zone0", VIRTUAL_FAULT_ZONE_TYPE, "virtual_group_0")},
        {computeZkPath("instance_1"), _instanceConfig1,
            ImmutableMap.of("helixZoneId", "zone0", VIRTUAL_FAULT_ZONE_TYPE, "virtual_group_0")},
        {computeZkPath("instance_2"), _instanceConfig2,
            ImmutableMap.of("helixZoneId", "zone1", VIRTUAL_FAULT_ZONE_TYPE, "virtual_group_1")}
    };
  }

  @Test
  public void testVirtualTopologyString() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setTopologyAwareEnabled(true);
    testConfig.setTopology("/zone/instance");
    Assert.assertEquals(VirtualTopologyGroupService.computeVirtualTopologyString(testConfig),
        "/virtualZone/instance");
  }

  private static ClusterTopology prepareClusterTopology() {
    List<ClusterTopology.Zone> zones = ImmutableList.of(
        new ClusterTopology.Zone("zone0", ImmutableList.of(
            new ClusterTopology.Instance("instance_0"), new ClusterTopology.Instance("instance_1"))),
        new ClusterTopology.Zone("zone1", ImmutableList.of(new ClusterTopology.Instance("instance_2"))));
    return new ClusterTopology(TEST_CLUSTER0, zones, ImmutableSet.of("instance_0", "instance_1", "instance_2"));
  }

  private static String computeZkPath(String instanceName) {
    HelixConfigScope scope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
        .forCluster(TEST_CLUSTER)
        .forParticipant(instanceName)
        .build();
    return scope.getZkPath();
  }
}
