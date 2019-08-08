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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;
import org.apache.helix.rest.server.json.cluster.PartitionHealth;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

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
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient) {
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
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient) {
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

  // TODO re-enable the test when partition health checks get decoupled
  @Test(enabled = false)
  public void testGetInstanceStoppableCheckWhenPartitionsCheckFail() throws IOException {
    InstanceService service =
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient) {
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

  @Test
  public void testGeneratePartitionHealthMapFromZK() {
    List<ZNRecord> healthData = generateHealthData();

    InstanceServiceImpl service =
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient);
    when(_dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
    when(_dataAccessor.getChildNames(new PropertyKey.Builder(TEST_CLUSTER).liveInstances()))
        .thenReturn(Arrays.asList("host0", "host1"));
    when(_dataAccessor.getProperty(Arrays.asList(
        new PropertyKey.Builder(TEST_CLUSTER).healthReport("host0", "PARTITION_HEALTH"),
        new PropertyKey.Builder(TEST_CLUSTER).healthReport("host1", "PARTITION_HEALTH"))))
            .thenReturn(Arrays.asList(new HealthStat(healthData.get(0)),
                new HealthStat(healthData.get(1))));
    PartitionHealth computeResult = service.generatePartitionHealthMapFromZK();
    PartitionHealth expectedResult = generateExpectedResult();
    Assert.assertEquals(computeResult, expectedResult);
  }

  private List<ZNRecord> generateHealthData() {
    // Set EXPIRY time 100000 that guarantees the test has enough time
    // Host 0 contains unhealthy partition but it does not matter.
    ZNRecord record1 = new ZNRecord("PARTITION_HEALTH");
    record1.setMapField("TESTDB0_0", ImmutableMap.of("IS_HEALTHY", "true", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));
    record1.setMapField("TESTDB0_1", ImmutableMap.of("IS_HEALTHY", "true", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));
    record1.setMapField("TESTDB0_2", ImmutableMap.of("IS_HEALTHY", "true", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));
    record1.setMapField("TESTDB1_0", ImmutableMap.of("IS_HEALTHY", "false", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));
    record1.setMapField("TESTDB2_0", ImmutableMap.of("IS_HEALTHY", "true", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));

    // Host 1 has expired data, which requires immediate API querying.
    ZNRecord record2 = new ZNRecord("PARTITION_HEALTH");
    record2.setMapField("TESTDB0_0", ImmutableMap.of("IS_HEALTHY", "true", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));
    record2.setMapField("TESTDB0_1", ImmutableMap.of("IS_HEALTHY", "true", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));
    record2.setMapField("TESTDB0_2", ImmutableMap.of("IS_HEALTHY", "true", "EXPIRE", "123456"));
    record2.setMapField("TESTDB1_0", ImmutableMap.of("IS_HEALTHY", "false", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));
    record2.setMapField("TESTDB2_0", ImmutableMap.of("IS_HEALTHY", "true", "EXPIRE",
        String.valueOf(System.currentTimeMillis() + 100000)));

    return Arrays.asList(record1, record2);
  }

  private PartitionHealth generateExpectedResult() {
    PartitionHealth partitionHealth = new PartitionHealth();

    partitionHealth.addSinglePartitionHealthForInstance("host0", "TESTDB0_0", true);
    partitionHealth.addSinglePartitionHealthForInstance("host0", "TESTDB0_1", true);
    partitionHealth.addSinglePartitionHealthForInstance("host0", "TESTDB0_2", true);
    partitionHealth.addSinglePartitionHealthForInstance("host0", "TESTDB1_0", false);
    partitionHealth.addSinglePartitionHealthForInstance("host0", "TESTDB2_0", true);

    partitionHealth.addSinglePartitionHealthForInstance("host1", "TESTDB0_0", true);
    partitionHealth.addSinglePartitionHealthForInstance("host1", "TESTDB0_1", true);
    partitionHealth.addSinglePartitionHealthForInstance("host1", "TESTDB1_0", false);
    partitionHealth.addSinglePartitionHealthForInstance("host1", "TESTDB2_0", true);
    partitionHealth.addInstanceThatNeedDirectCallWithPartition("host1", "TESTDB0_2");

    return partitionHealth;
  }
}
