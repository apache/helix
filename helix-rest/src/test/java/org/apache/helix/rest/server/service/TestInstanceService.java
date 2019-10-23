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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.codehaus.jackson.map.ObjectMapper;
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

  @Test
  public void testGetInstanceStoppableCheckConnectionRefused() throws IOException {
    InstanceService service =
        new InstanceServiceImpl(_dataAccessor, _configAccessor, _customRestClient) {
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
}
