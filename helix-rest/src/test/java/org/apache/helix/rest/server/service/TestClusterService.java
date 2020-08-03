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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestClusterService {
  private static final String TEST_CLUSTER = "Test_Cluster";

  @Test
  public void testGetClusterTopology_whenMultiZones() {
    InstanceConfig instanceConfig1 = new InstanceConfig("instance0");
    instanceConfig1.setDomain("helixZoneId=zone0");
    InstanceConfig instanceConfig2 = new InstanceConfig("instance1");
    instanceConfig2.setDomain("helixZoneId=zone1");
    List<HelixProperty> instanceConfigs = (List) ImmutableList.of(instanceConfig1, instanceConfig2);

    Mock mock = new Mock();
    when(mock.dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
    when(mock.dataAccessor.getChildValues(any(PropertyKey.class), anyBoolean())).thenReturn(instanceConfigs);

    ClusterTopology clusterTopology = mock.clusterService.getClusterTopology(TEST_CLUSTER);

    Assert.assertEquals(clusterTopology.getZones().size(), 2);
    Assert.assertEquals(clusterTopology.getClusterId(), TEST_CLUSTER);
  }

  @Test
  public void testGetClusterTopology_whenZeroZones() {
    InstanceConfig instanceConfig1 = new InstanceConfig("instance0");
    InstanceConfig instanceConfig2 = new InstanceConfig("instance1");
    List<HelixProperty> instanceConfigs = (List) ImmutableList.of(instanceConfig1, instanceConfig2);

    Mock mock = new Mock();
    when(mock.dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
    when(mock.dataAccessor.getChildValues(any(PropertyKey.class), anyBoolean()))
        .thenReturn(instanceConfigs);

    ClusterTopology clusterTopology = mock.clusterService.getClusterTopology(TEST_CLUSTER);

    Assert.assertEquals(clusterTopology.getZones().size(), 0);
    Assert.assertEquals(clusterTopology.getClusterId(), TEST_CLUSTER);
  }

  @Test
  public void testGetClusterTopology_whenZoneHasMultiInstances() {
    InstanceConfig instanceConfig1 = new InstanceConfig("instance0");
    instanceConfig1.setDomain("helixZoneId=zone0");
    InstanceConfig instanceConfig2 = new InstanceConfig("instance1");
    instanceConfig2.setDomain("helixZoneId=zone0");
    List<HelixProperty> instanceConfigs = (List) ImmutableList.of(instanceConfig1, instanceConfig2);

    Mock mock = new Mock();
    when(mock.dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
    when(mock.dataAccessor.getChildValues(any(PropertyKey.class), anyBoolean()))
        .thenReturn(instanceConfigs);

    ClusterTopology clusterTopology = mock.clusterService.getClusterTopology(TEST_CLUSTER);

    Assert.assertEquals(clusterTopology.getZones().size(), 1);
    Assert.assertEquals(clusterTopology.getZones().get(0).getInstances().size(), 2);
    Assert.assertEquals(clusterTopology.getClusterId(), TEST_CLUSTER);
  }

  private final class Mock {
    private HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    private ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    private ClusterService clusterService;

    Mock() {
      ClusterConfig mockConfig = new ClusterConfig(TEST_CLUSTER);
      mockConfig.setFaultZoneType("helixZoneId");
      when(configAccessor.getClusterConfig(TEST_CLUSTER)).thenReturn(mockConfig);
      clusterService = new ClusterServiceImpl(dataAccessor, configAccessor);
    }
  }
}
