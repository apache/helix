package org.apache.helix.rest.clusterMaintenanceService;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TestMaintenanceManagementService {
  private static final String TEST_CLUSTER = "TestCluster";
  private static final String TEST_INSTANCE = "instance0.linkedin.com_1235";

  @Mock
  private HelixDataAccessorWrapper _dataAccessorWrapper;
  @Mock
  private ConfigAccessor _configAccessor;
  @Mock
  private CustomRestClient _customRestClient;

  @BeforeMethod
  public void beforeMethod() {
    MockitoAnnotations.initMocks(this);
    RESTConfig restConfig = new RESTConfig("restConfig");
    restConfig.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, "http://*:123/path");
    when(_configAccessor.getRESTConfig(TEST_CLUSTER)).thenReturn(restConfig);
    when(_dataAccessorWrapper.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
  }

  class MockMaintenanceManagementService extends MaintenanceManagementService {

    public MockMaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
        ConfigAccessor configAccessor, CustomRestClient customRestClient, boolean skipZKRead,
        boolean continueOnFailure, String namespace) {
      super(new HelixDataAccessorWrapper(dataAccessor, customRestClient, namespace), configAccessor,
          customRestClient, skipZKRead,
          continueOnFailure ? Collections.singleton(ALL_HEALTH_CHECK_NONBLOCK)
              : Collections.emptySet(), null, namespace);
    }

    public MockMaintenanceManagementService(HelixDataAccessorWrapper dataAccessorWrapper,
        ConfigAccessor configAccessor, CustomRestClient customRestClient, boolean skipZKRead,
        boolean continueOnFailure, Set<StoppableCheck.Category> skipHealthCheckCategories,
        String namespace) {
      super(dataAccessorWrapper, configAccessor, customRestClient, skipZKRead,
          continueOnFailure ? Collections.singleton(ALL_HEALTH_CHECK_NONBLOCK)
              : Collections.emptySet(), skipHealthCheckCategories, namespace);
    }

    public MockMaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
        ConfigAccessor configAccessor, CustomRestClient customRestClient, boolean skipZKRead,
        Set<String> nonBlockingHealthChecks, String namespace) {
      super(new HelixDataAccessorWrapper(dataAccessor, customRestClient, namespace), configAccessor,
          customRestClient, skipZKRead, nonBlockingHealthChecks, null, namespace);
    }

    @Override
    protected Map<String, Boolean> getInstanceHealthStatus(String clusterId, String instanceName,
        List<HealthCheck> healthChecks, Set<String> toBeStoppedInstances) {
      return Collections.emptyMap();
    }
  }

  @Test
  public void testGetInstanceStoppableCheckWhenHelixOwnCheckFail() throws IOException {
    Map<String, Boolean> failedCheck = ImmutableMap.of("FailCheck", false);
    MockMaintenanceManagementService service =
        new MockMaintenanceManagementService(_dataAccessorWrapper, _configAccessor,
            _customRestClient, false, false, null, HelixRestNamespace.DEFAULT_NAMESPACE_NAME) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks, Set<String> toBeStoppedInstances) {
            return failedCheck;
          }
        };
    String jsonContent = "";
    StoppableCheck actual =
        service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);
    Assert.assertEquals(actual.getFailedChecks().size(), failedCheck.size());
    Assert.assertFalse(actual.isStoppable());
    verifyZeroInteractions(_customRestClient);
    verifyZeroInteractions(_configAccessor);
  }

  @Test
  public void testGetInstanceStoppableCheckWhenCustomInstanceCheckFail() throws IOException {
    MockMaintenanceManagementService service =
        new MockMaintenanceManagementService(_dataAccessorWrapper, _configAccessor,
            _customRestClient, false, false, null, HelixRestNamespace.DEFAULT_NAMESPACE_NAME) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks, Set<String> toBeStoppedInstances) {
            return Collections.emptyMap();
          }
        };
    Map<String, Boolean> failedCheck = ImmutableMap.of("FailCheck", false);
    when(_customRestClient.getInstanceStoppableCheck(anyString(), anyMap())).thenReturn(
        failedCheck);
    String jsonContent = "{\n" + "   \"param1\": \"value1\",\n" + "\"param2\": \"value2\"\n" + "}";
    StoppableCheck actual =
        service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);
    Assert.assertFalse(actual.isStoppable());
    Assert.assertEquals(actual.getFailedChecks().size(), failedCheck.size());
    verify(_customRestClient, times(1)).getInstanceStoppableCheck(any(), any());
    verify(_customRestClient, times(0)).getPartitionStoppableCheck(any(), any(), any());
  }

  @Test
  public void testGetInstanceStoppableCheckWhenCustomInstanceCheckAndCustomPartitionCheckDisabled()
      throws IOException {
    // Test when custom instance and partition check are both disabled.
    MockMaintenanceManagementService service =
        new MockMaintenanceManagementService(_dataAccessorWrapper, _configAccessor,
            _customRestClient, false, false, new HashSet<>(
            Arrays.asList(StoppableCheck.Category.CUSTOM_INSTANCE_CHECK,
                StoppableCheck.Category.CUSTOM_PARTITION_CHECK)),
            HelixRestNamespace.DEFAULT_NAMESPACE_NAME);

    StoppableCheck actual = service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, "");
    Assert.assertTrue(actual.isStoppable());
    verify(_dataAccessorWrapper, times(0)).getAllPartitionsHealthOnLiveInstance(any(), any(),
        anyBoolean());
    verify(_customRestClient, times(0)).getInstanceStoppableCheck(any(), any());
  }

  @Test
  public void testGetInstanceStoppableCheckWhenCustomPartitionCheckDisabled() throws IOException {
    // Test when custom only partition check is disabled and instance check fails.
    when(_dataAccessorWrapper.getAllPartitionsHealthOnLiveInstance(any(), anyMap(),
        anyBoolean())).thenReturn(Collections.emptyMap());
    when(_dataAccessorWrapper.getChildValues(any(), anyBoolean())).thenReturn(
        Collections.emptyList());
    when(_customRestClient.getInstanceStoppableCheck(anyString(), anyMap())).thenReturn(
        ImmutableMap.of("FailCheck", false));
    MockMaintenanceManagementService service =
        new MockMaintenanceManagementService(_dataAccessorWrapper, _configAccessor,
            _customRestClient, false, false,
            new HashSet<>(Arrays.asList(StoppableCheck.Category.CUSTOM_PARTITION_CHECK)),
            HelixRestNamespace.DEFAULT_NAMESPACE_NAME);

    StoppableCheck actual = service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, "");
    Assert.assertEquals(actual.getFailedChecks(),
        Arrays.asList(StoppableCheck.Category.CUSTOM_INSTANCE_CHECK.getPrefix() + "FailCheck"));
    Assert.assertFalse(actual.isStoppable());
    verify(_dataAccessorWrapper, times(0)).getAllPartitionsHealthOnLiveInstance(any(), any(),
        anyBoolean());
    verify(_customRestClient, times(1)).getInstanceStoppableCheck(any(), any());
  }

  @Test
  public void testGetInstanceStoppableCheckWhenCustomInstanceCheckDisabled() throws IOException {
    // Test when custom only instance check is disabled and partition check fails.
    String testResource = "testResource";
    ZNRecord externalViewZnode = new ZNRecord(testResource);
    externalViewZnode.setSimpleField(
        ExternalView.ExternalViewProperty.STATE_MODEL_DEF_REF.toString(), LeaderStandbySMD.name);
    ExternalView externalView = new ExternalView(externalViewZnode);
    externalView.setStateMap("testPartition",
        ImmutableMap.of(TEST_INSTANCE, "LEADER", "sibling_instance", "OFFLINE"));

    when(_dataAccessorWrapper.getAllPartitionsHealthOnLiveInstance(any(), anyMap(),
        anyBoolean())).thenReturn(Collections.emptyMap());
    when(_dataAccessorWrapper.getProperty((PropertyKey) any())).thenReturn(new LeaderStandbySMD());
    when(_dataAccessorWrapper.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
    when(_dataAccessorWrapper.getChildValues(any(), anyBoolean())).thenReturn(
        Arrays.asList(externalView));
    MockMaintenanceManagementService service =
        new MockMaintenanceManagementService(_dataAccessorWrapper, _configAccessor,
            _customRestClient, false, false,
            new HashSet<>(Arrays.asList(StoppableCheck.Category.CUSTOM_INSTANCE_CHECK)),
            HelixRestNamespace.DEFAULT_NAMESPACE_NAME);

    StoppableCheck actual = service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, "");
    List<String> expectedFailedChecks = Arrays.asList(
        StoppableCheck.Category.CUSTOM_PARTITION_CHECK.getPrefix()
            + "PARTITION_INITIAL_STATE_FAIL:testPartition");
    Assert.assertEquals(actual.getFailedChecks(), expectedFailedChecks);
    Assert.assertFalse(actual.isStoppable());
    verify(_dataAccessorWrapper, times(1)).getAllPartitionsHealthOnLiveInstance(any(), any(),
        anyBoolean());
    verify(_customRestClient, times(0)).getInstanceStoppableCheck(any(), any());
  }

  @Test
  public void testGetInstanceStoppableCheckConnectionRefused() throws IOException {
    MockMaintenanceManagementService service =
        new MockMaintenanceManagementService(_dataAccessorWrapper, _configAccessor,
            _customRestClient, false, false, null, HelixRestNamespace.DEFAULT_NAMESPACE_NAME) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks, Set<String> toBeStoppedInstances) {
            return Collections.emptyMap();
          }
        };
    when(_customRestClient.getInstanceStoppableCheck(anyString(), anyMap()))
        .thenThrow(IOException.class);
    Map<String, String> dataMap =
        ImmutableMap.of("instance", TEST_INSTANCE, "selection_base", "zone_based");
    String jsonContent = new ObjectMapper().writeValueAsString(dataMap);
    StoppableCheck actual =
        service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);
    int expectedFailedChecksSize = 1;
    String expectedFailedCheck =
        StoppableCheck.Category.CUSTOM_INSTANCE_CHECK.getPrefix() + TEST_INSTANCE;
    Assert.assertNotNull(actual);
    Assert.assertFalse(actual.isStoppable());
    Assert.assertEquals(actual.getFailedChecks().size(), expectedFailedChecksSize);
    Assert.assertEquals(actual.getFailedChecks().get(0), expectedFailedCheck);
  }

  @Test
  public void testCustomPartitionCheckWithSkipZKRead() throws IOException {
    // Let ZK result is health, but http request is unhealthy.
    // We expect the check fail if we skipZKRead.
    String testPartition = "PARTITION_0";
    String siblingInstance = "instance0.linkedin.com_1236";
    String jsonContent = "{\n" +
        "\"param1\": \"value1\",\n" +
        "\"param2\": \"value2\"\n" +
        "}";
    BaseDataAccessor<ZNRecord> mockAccessor = mock(ZkBaseDataAccessor.class);
    ZKHelixDataAccessor zkHelixDataAccessor =
        new ZKHelixDataAccessor(TEST_CLUSTER, mockAccessor);
    ZNRecord successPartitionReport = new ZNRecord(HelixDataAccessorWrapper.PARTITION_HEALTH_KEY);
    // Instance level check passed
    when(_customRestClient.getInstanceStoppableCheck(anyString(), anyMap())).thenReturn(
        Collections.singletonMap(TEST_INSTANCE, true));
    // Mocks for partition level
    when(mockAccessor.getChildNames(zkHelixDataAccessor.keyBuilder().liveInstances().getPath(), 2)).thenReturn(
        Arrays.asList(TEST_INSTANCE, siblingInstance));
    // Mock ZK record is healthy
    Map<String, String> partition0 = new HashMap<>();
    partition0.put(HelixDataAccessorWrapper.EXPIRY_KEY, String.valueOf(System.currentTimeMillis() + 100000L));
    partition0.put(HelixDataAccessorWrapper.IS_HEALTHY_KEY, "true");
    partition0.put("lastWriteTime", String.valueOf(System.currentTimeMillis()));
    successPartitionReport.setMapField(testPartition, partition0);
    when(mockAccessor.get(Arrays.asList(zkHelixDataAccessor.keyBuilder()
        .healthReport(TEST_INSTANCE, HelixDataAccessorWrapper.PARTITION_HEALTH_KEY)
        .getPath(), zkHelixDataAccessor.keyBuilder()
        .healthReport(siblingInstance, HelixDataAccessorWrapper.PARTITION_HEALTH_KEY)
        .getPath()), Arrays.asList(new Stat(), new Stat()), 0, false)).thenReturn(
        Arrays.asList(successPartitionReport, successPartitionReport));
    // Mock client call result is check fail.
    when(_customRestClient.getPartitionStoppableCheck(anyString(), anyList(), anyMap())).thenReturn(
        Collections.singletonMap(testPartition, false));
    // Mock data for InstanceValidationUtil
    ExternalView externalView = new ExternalView("TestResource");
    externalView.setState(testPartition, TEST_INSTANCE, "MASTER");
    externalView.setState(testPartition, siblingInstance, "SLAVE");
    externalView.getRecord().setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(), "MasterSlave");
    when(mockAccessor.getChildren(zkHelixDataAccessor.keyBuilder().externalViews().getPath(), null,
        AccessOption.PERSISTENT, 1, 0)).thenReturn(Arrays.asList(externalView.getRecord()));
    when(mockAccessor.get(zkHelixDataAccessor.keyBuilder().stateModelDef("MasterSlave").getPath(), new Stat(),
        AccessOption.PERSISTENT)).thenReturn(MasterSlaveSMD.build().getRecord());

    // Valid data only from ZK, pass the check
    MockMaintenanceManagementService instanceServiceReadZK =
        new MockMaintenanceManagementService(zkHelixDataAccessor, _configAccessor, _customRestClient, false,
            false, HelixRestNamespace.DEFAULT_NAMESPACE_NAME);
    StoppableCheck stoppableCheck =
        instanceServiceReadZK.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);
    Assert.assertTrue(stoppableCheck.isStoppable());

    // Even ZK data is valid. Skip ZK read should fail the test.
    MockMaintenanceManagementService instanceServiceWithoutReadZK =
        new MockMaintenanceManagementService(zkHelixDataAccessor, _configAccessor, _customRestClient, true,
            false, HelixRestNamespace.DEFAULT_NAMESPACE_NAME);
    stoppableCheck = instanceServiceWithoutReadZK.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);
    Assert.assertFalse(stoppableCheck.isStoppable());

    // Test take instance with same setting.
    MaintenanceManagementInstanceInfo instanceInfo =
        instanceServiceWithoutReadZK.takeInstance(TEST_CLUSTER, TEST_INSTANCE, Collections.singletonList("CustomInstanceStoppableCheck"),
            MaintenanceManagementService.getMapFromJsonPayload(jsonContent), Collections.singletonList("org.apache.helix.rest.server.TestOperationImpl"),
            Collections.EMPTY_MAP, true);
    Assert.assertFalse(instanceInfo.isSuccessful());
    Assert.assertEquals(instanceInfo.getMessages().get(0), "CUSTOM_PARTITION_HEALTH_FAILURE:HOST_NO_STATE_ERROR:INSTANCE0.LINKEDIN.COM_1236:PARTITION_0");

    // Operation should finish even with check failed.
    MockMaintenanceManagementService instanceServiceSkipFailure =
        new MockMaintenanceManagementService(zkHelixDataAccessor, _configAccessor, _customRestClient, true,
            ImmutableSet.of("CUSTOM_PARTITION_HEALTH_FAILURE:HOST_NO_STATE_ERROR"), HelixRestNamespace.DEFAULT_NAMESPACE_NAME);
    MaintenanceManagementInstanceInfo instanceInfo2 =
        instanceServiceSkipFailure.takeInstance(TEST_CLUSTER, TEST_INSTANCE, Collections.singletonList("CustomInstanceStoppableCheck"),
            MaintenanceManagementService.getMapFromJsonPayload(jsonContent), Collections.singletonList("org.apache.helix.rest.server.TestOperationImpl"),
            Collections.EMPTY_MAP, true);
    Assert.assertTrue(instanceInfo2.isSuccessful());
    Assert.assertEquals(instanceInfo2.getOperationResult(), "DummyTakeOperationResult");
  }

  /*
   * Tests stoppable check api when all checks query is enabled. After helix own check fails,
   * the subsequent checks should be performed.
   */
  @Test
  public void testGetStoppableWithAllChecks() throws IOException {
    String siblingInstance = "instance0.linkedin.com_1236";
    BaseDataAccessor<ZNRecord> mockAccessor = mock(ZkBaseDataAccessor.class);
    ZKHelixDataAccessor zkHelixDataAccessor =
        new ZKHelixDataAccessor(TEST_CLUSTER, mockAccessor);
    when(mockAccessor.getChildNames(zkHelixDataAccessor.keyBuilder().liveInstances().getPath(), 2))
        .thenReturn(Arrays.asList(TEST_INSTANCE, siblingInstance));

    Map<String, Boolean> instanceHealthFailedCheck = ImmutableMap.of("FailCheck", false);
    MockMaintenanceManagementService service =
        new MockMaintenanceManagementService(zkHelixDataAccessor, _configAccessor, _customRestClient, true, true,
            HelixRestNamespace.DEFAULT_NAMESPACE_NAME) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks, Set<String> toBeStoppedInstances) {
            return instanceHealthFailedCheck;
          }
        };
    when(_customRestClient.getInstanceStoppableCheck(anyString(), anyMap()))
        .thenReturn(ImmutableMap.of("FailCheck", false));
    when(_customRestClient.getPartitionStoppableCheck(anyString(), anyList(), anyMap()))
        .thenReturn(ImmutableMap.of("FailCheck", false));
    StoppableCheck actual = service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, "");
    List<String> expectedFailedChecks =
        Arrays.asList("HELIX:FailCheck", "CUSTOM_INSTANCE_HEALTH_FAILURE:FailCheck");
    Assert.assertEquals(actual.getFailedChecks(), expectedFailedChecks);
    Assert.assertFalse(actual.isStoppable());
    // Verify the subsequent checks are called
    verify(_configAccessor, times(1)).getRESTConfig(anyString());
    verify(_customRestClient, times(1)).getInstanceStoppableCheck(anyString(), anyMap());
    verify(_customRestClient, times(2))
        .getPartitionStoppableCheck(anyString(), nullable(List.class), anyMap());
  }

  // TODO re-enable the test when partition health checks get decoupled
  @Test(enabled = false)
  public void testGetInstanceStoppableCheckWhenPartitionsCheckFail() throws IOException {
    MockMaintenanceManagementService service =
        new MockMaintenanceManagementService(_dataAccessorWrapper, _configAccessor,
            _customRestClient, false, false, HelixRestNamespace.DEFAULT_NAMESPACE_NAME) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks, Set<String> toBeStoppedInstances) {
            return Collections.emptyMap();
          }
        };
    // partition is health on the test instance but unhealthy on the sibling instance
    when(_customRestClient.getInstanceStoppableCheck(anyString(), anyMap()))
        .thenReturn(Collections.emptyMap());
    String jsonContent = "{\n" + "   \"param1\": \"value1\",\n" + "\"param2\": \"value2\"\n" + "}";
    StoppableCheck actual =
        service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);
    Assert.assertFalse(actual.isStoppable());
    verify(_customRestClient, times(1)).getInstanceStoppableCheck(any(), any());
  }

}