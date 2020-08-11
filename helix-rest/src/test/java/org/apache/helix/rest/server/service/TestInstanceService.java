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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


public class TestInstanceService {
  private static final String TEST_CLUSTER = "TestCluster";
  private static final String TEST_INSTANCE = "instance0.linkedin.com_1235";

  @Mock
  private HelixDataAccessorWrapper _dataAccessor;
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
  }

  @Test
  public void testGetInstanceStoppableCheckWhenHelixOwnCheckFail() throws IOException {
    Map<String, Boolean> failedCheck = ImmutableMap.of("FailCheck", false);
    InstanceService service =
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient, false) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks) {
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
    InstanceService service =
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient, false) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks) {
            return Collections.emptyMap();
          }
        };
    Map<String, Boolean> failedCheck = ImmutableMap.of("FailCheck", false);
    when(_customRestClient.getInstanceStoppableCheck(anyString(), anyMap()))
        .thenReturn(failedCheck);

    String jsonContent = "{\n" + "   \"param1\": \"value1\",\n" + "\"param2\": \"value2\"\n" + "}";
    StoppableCheck actual =
        service.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);

    Assert.assertFalse(actual.isStoppable());
    Assert.assertEquals(actual.getFailedChecks().size(), failedCheck.size());
    verify(_customRestClient, times(1)).getInstanceStoppableCheck(any(), any());
    verify(_customRestClient, times(0)).getPartitionStoppableCheck(any(), any(), any());
  }

  @Test
  public void testGetInstanceStoppableCheckConnectionRefused() throws IOException {
    InstanceService service =
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient, false) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks) {
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
        new ZKHelixDataAccessor(TEST_CLUSTER, InstanceType.ADMINISTRATOR, mockAccessor);
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
    InstanceService instanceServiceReadZK =
        new MockInstanceServiceImpl(new HelixDataAccessorWrapper(zkHelixDataAccessor, _customRestClient),
            _configAccessor, _customRestClient, false);
    StoppableCheck stoppableCheck =
        instanceServiceReadZK.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);
    Assert.assertTrue(stoppableCheck.isStoppable());

    // Even ZK data is valid. Skip ZK read should fail the test.
    InstanceService instanceServiceWithoutReadZK =
        new MockInstanceServiceImpl(new HelixDataAccessorWrapper(zkHelixDataAccessor, _customRestClient),
            _configAccessor, _customRestClient, true);
    stoppableCheck = instanceServiceWithoutReadZK.getInstanceStoppableCheck(TEST_CLUSTER, TEST_INSTANCE, jsonContent);
    Assert.assertFalse(stoppableCheck.isStoppable());
  }

  // TODO re-enable the test when partition health checks get decoupled
  @Test(enabled = false)
  public void testGetInstanceStoppableCheckWhenPartitionsCheckFail() throws IOException {
    InstanceService service =
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient, false) {
          @Override
          protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
              String instanceName, List<HealthCheck> healthChecks) {
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

  class MockInstanceServiceImpl extends InstanceServiceImpl {
    MockInstanceServiceImpl(HelixDataAccessorWrapper dataAccessor, ConfigAccessor configAccessor,
        CustomRestClient customRestClient, boolean skipZKRead) {
      super(dataAccessor, configAccessor, customRestClient, skipZKRead);
    }

    @Override
    protected Map<String, Boolean> getInstanceHealthStatus(String clusterId,
        String instanceName, List<InstanceService.HealthCheck> healthChecks) {
      return Collections.emptyMap();
    }
  }
}
