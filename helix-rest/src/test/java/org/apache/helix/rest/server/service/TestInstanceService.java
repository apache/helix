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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.HealthStat;
import org.apache.helix.rest.server.json.cluster.PartitionHealth;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class TestInstanceService {
  private static final String TEST_CLUSTER = "TestCluster";

  @Test
  public void testGeneratePartitionHealthMapFromZK() {
    //Prepare for testing data.
    Mock mock = new Mock();
    when(mock.dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
    when(mock.dataAccessor.getChildNames(new PropertyKey.Builder(TEST_CLUSTER).liveInstances()))
        .thenReturn(mock.liveInstances);
    when(mock.dataAccessor.getProperty(
        new PropertyKey.Builder(TEST_CLUSTER).healthReport("host0", "PARTITION_HEALTH")))
        .thenReturn(new HealthStat(mock.healthData.get(0)));
    when(mock.dataAccessor.getProperty(
        new PropertyKey.Builder(TEST_CLUSTER).healthReport("host1", "PARTITION_HEALTH")))
        .thenReturn(new HealthStat(mock.healthData.get(1)));
    PartitionHealth computeResult = new InstanceServiceImpl(mock.dataAccessor, mock.configAccessor)
        .generatePartitionHealthMapFromZK();
    PartitionHealth expectedResult = generateExpectedResult();
    Assert.assertTrue(computeResult.equals(expectedResult));
  }


  private final class Mock {
    private HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    private ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    private List<String> liveInstances = Arrays.asList("host0", "host1");
    private List<ZNRecord> healthData = generateHealthData();

    Mock() {
    }

    private List<ZNRecord> generateHealthData() {
      // Set EXPIRY time 100000 that guarantees the test has enough time 
      // Host 0 contains unhealthy partition but it does not matter.
      ZNRecord record1 = new ZNRecord("PARTITION_HEALTH");
      record1.setMapField("TESTDB0_0", ImmutableMap
          .of("IS_HEALTHY", "true", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));
      record1.setMapField("TESTDB0_1", ImmutableMap
          .of("IS_HEALTHY", "true", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));
      record1.setMapField("TESTDB0_2", ImmutableMap
          .of("IS_HEALTHY", "true", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));
      record1.setMapField("TESTDB1_0", ImmutableMap
          .of("IS_HEALTHY", "false", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));
      record1.setMapField("TESTDB2_0", ImmutableMap
          .of("IS_HEALTHY", "true", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));

      // Host 1 has expired data, which requires immediate API querying.
      ZNRecord record2 = new ZNRecord("PARTITION_HEALTH");
      record2.setMapField("TESTDB0_0", ImmutableMap
          .of("IS_HEALTHY", "true", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));
      record2.setMapField("TESTDB0_1", ImmutableMap
          .of("IS_HEALTHY", "true", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));
      record2.setMapField("TESTDB0_2", ImmutableMap
          .of("IS_HEALTHY", "true", "EXPIRE", "123456"));
      record2.setMapField("TESTDB1_0", ImmutableMap
          .of("IS_HEALTHY", "false", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));
      record2.setMapField("TESTDB2_0", ImmutableMap
          .of("IS_HEALTHY", "true", "EXPIRE", String.valueOf(System.currentTimeMillis() + 100000)));

      return Arrays.asList(record1, record2);
    }
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
