package org.apache.helix.rest.common;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TestHelixDataAccessorWrapper {
  private static final String TEST_INSTANCE0 = "host0";
  private static final String TEST_INSTANCE1 = "host1";
  private static final String TEST_PARTITION = "db0";
  private static final String TEST_CLUSTER = "TestCluster";
  private static final Map<String, String> CUSTOM_PAY_LOADS = Collections.emptyMap();

  private MockHelixDataAccessorWrapper _dataAccessor;
  private RESTConfig _restConfig;
  private CustomRestClient _restClient;

  private static class MockHelixDataAccessorWrapper extends HelixDataAccessorWrapper {
    public MockHelixDataAccessorWrapper(ZKHelixDataAccessor dataAccessor) {
      super(dataAccessor);
    }

    void setRestClient(CustomRestClient restClient) {
      _restClient = restClient;
    }
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    _dataAccessor = mock(MockHelixDataAccessorWrapper.class, CALLS_REAL_METHODS);
    _restConfig = mock(RESTConfig.class);
    when(_restConfig.getBaseUrl(anyString())).thenReturn("http://localhost:1000");
    _restClient = mock(CustomRestClient.class);
    _dataAccessor.setRestClient(_restClient);

    when(_restClient.getPartitionStoppableCheck(anyString(), anyList(), anyMap()))
        .thenAnswer((invocationOnMock) -> {
          Object[] args = invocationOnMock.getArguments();
          List<String> inputPartitions = (List<String>) args[1];
          return inputPartitions.stream()
              // always return true for rest partition checks
              .collect(Collectors.toMap(Function.identity(), partition -> true));
        });

    doReturn(new PropertyKey.Builder(TEST_CLUSTER)).when(_dataAccessor).keyBuilder();
  }

  @Test
  public void testGetPartitionHealthOfInstanceWithValidZKRecord() {
    long validDate = System.currentTimeMillis() + 10000L;
    doReturn(ImmutableList.of(TEST_INSTANCE0)).when(_dataAccessor).getChildNames(any());
    // generate health record for TEST_INSTANCE0
    ZNRecord record = new ZNRecord(HelixDataAccessorWrapper.PARTITION_HEALTH_KEY);
    record.setMapField(TEST_PARTITION, ImmutableMap.of(HelixDataAccessorWrapper.IS_HEALTHY_KEY,
        "true", HelixDataAccessorWrapper.EXPIRY_KEY, String.valueOf(validDate)));
    doReturn(ImmutableList.of(new HealthStat(record))).when(_dataAccessor).getProperty(anyList(),
        anyBoolean());

    Map<String, Map<String, Boolean>> actual =
        _dataAccessor.getAllPartitionsHealthOnLiveInstance(_restConfig, CUSTOM_PAY_LOADS);
    Map<String, Map<String, Boolean>> expected =
        ImmutableMap.of(TEST_INSTANCE0, ImmutableMap.of(TEST_PARTITION, true));

    Assert.assertEquals(actual, expected);
    // No need to query against rest endpoint
    verifyZeroInteractions(_restClient);
  }

  @Test
  public void testGetPartitionHealthOfInstanceWithExpiredZKRecord() throws IOException {
    long expiredDate = System.currentTimeMillis() - 10000L;
    doReturn(ImmutableList.of(TEST_INSTANCE0)).when(_dataAccessor).getChildNames(any());
    // generate health record for TEST_INSTANCE0
    ZNRecord record = new ZNRecord(HelixDataAccessorWrapper.PARTITION_HEALTH_KEY);
    record.setMapField(TEST_PARTITION, ImmutableMap.of(HelixDataAccessorWrapper.IS_HEALTHY_KEY,
        "false", HelixDataAccessorWrapper.EXPIRY_KEY, String.valueOf(expiredDate)));
    doReturn(ImmutableList.of(new HealthStat(record))).when(_dataAccessor).getProperty(anyList(),
        anyBoolean());

    Map<String, Map<String, Boolean>> actual =
        _dataAccessor.getAllPartitionsHealthOnLiveInstance(_restConfig, CUSTOM_PAY_LOADS);
    Map<String, Map<String, Boolean>> expected =
        ImmutableMap.of(TEST_INSTANCE0, ImmutableMap.of(TEST_PARTITION, true));

    Assert.assertEquals(actual, expected);
    // query once because the partition health is expired
    verify(_restClient, times(1)).getPartitionStoppableCheck(anyString(), anyList(), anyMap());
  }

  @Test
  public void testGetPartitionHealthOfInstanceWithIncompleteZKRecord() throws IOException {
    long validDate = System.currentTimeMillis() + 10000L;
    doReturn(ImmutableList.of(TEST_INSTANCE0, TEST_INSTANCE1)).when(_dataAccessor)
        .getChildNames(any());
    // generate health record only for TEST_INSTANCE0
    ZNRecord record = new ZNRecord(HelixDataAccessorWrapper.PARTITION_HEALTH_KEY);
    record.setMapField(TEST_PARTITION, ImmutableMap.of(HelixDataAccessorWrapper.IS_HEALTHY_KEY,
        "false", HelixDataAccessorWrapper.EXPIRY_KEY, String.valueOf(validDate)));
    doReturn(Arrays.asList(new HealthStat(record), null)).when(_dataAccessor).getProperty(anyList(),
        anyBoolean());

    when(_restClient.getPartitionStoppableCheck(anyString(), anyList(), anyMap()))
        .thenReturn(ImmutableMap.of(TEST_PARTITION, true));

    Map<String, Map<String, Boolean>> actual =
        _dataAccessor.getAllPartitionsHealthOnLiveInstance(_restConfig, CUSTOM_PAY_LOADS);
    Map<String, Map<String, Boolean>> expected =
        ImmutableMap.of(TEST_INSTANCE0, ImmutableMap.of(TEST_PARTITION, false), TEST_INSTANCE1,
            ImmutableMap.of(TEST_PARTITION, true));

    Assert.assertEquals(actual, expected);
    // query once because no partition health record for TEST_INSTANCE1
    verify(_restClient, times(1)).getPartitionStoppableCheck(anyString(), anyList(), anyMap());
  }
}
