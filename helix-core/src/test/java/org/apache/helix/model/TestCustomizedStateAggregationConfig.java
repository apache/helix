package org.apache.helix.model;

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

import java.util.ArrayList;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import java.util.List;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCustomizedStateAggregationConfig extends ZkUnitTestBase {

  @Test(expectedExceptions = HelixException.class)
  public void TestCustomizedStateAggregationConfigNonExistentCluster() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    // Read CustomizedStateAggregationConfig from Zookeeper and get exception since cluster in not setup yet
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CustomizedStateAggregationConfig customizedStateAggregationConfig =
        _configAccessor.getCustomizedStateAggregationConfig(clusterName);
  }

  @Test(dependsOnMethods = "TestCustomizedStateAggregationConfigNonExistentCluster")
  public void testCustomizedStateAggregationConfigNull() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    // Read CustomizedStateAggregationConfig from Zookeeper
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CustomizedStateAggregationConfig customizedStateAggregationConfigFromZk =
        _configAccessor.getCustomizedStateAggregationConfig(clusterName);
    Assert.assertNull(customizedStateAggregationConfigFromZk);
  }

  @Test(dependsOnMethods = "testCustomizedStateAggregationConfigNull")
  public void testCustomizedStateAggregationConfig() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    // Create dummy CustomizedStateAggregationConfig object
    CustomizedStateAggregationConfig customizedStateAggregationConfig =
        new CustomizedStateAggregationConfig(clusterName);
    List<String> aggregationEnabledTypes = new ArrayList<String>();
    aggregationEnabledTypes.add("mockState1");
    aggregationEnabledTypes.add("mockState2");
    customizedStateAggregationConfig.setAggregationEnabledTypes(aggregationEnabledTypes);

    // Write the CustomizedStateAggregationConfig to Zookeeper
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateAggregationConfig(),
        customizedStateAggregationConfig);

    // Read CustomizedStateAggregationConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CustomizedStateAggregationConfig customizedStateAggregationConfigFromZk =
        _configAccessor.getCustomizedStateAggregationConfig(clusterName);
    Assert.assertEquals(customizedStateAggregationConfigFromZk.getAggregationEnabledTypes().size(),
        2);
    Assert.assertEquals(aggregationEnabledTypes.get(0), "mockType1");
    Assert.assertEquals(aggregationEnabledTypes.get(1), "mockType2");
  }

  @Test(dependsOnMethods = "testCustomizedStateAggregationConfig")
  public void testCustomizedStateAggregationConfigBuilder() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    CustomizedStateAggregationConfig.Builder builder =
        new CustomizedStateAggregationConfig.Builder(clusterName);
    builder.addAggregationEnabledType("mockType1");
    builder.addAggregationEnabledType("mockType2");

    // Check builder getter methods
    Assert.assertEquals(builder.getClusterName(), clusterName);
    List<String> aggregationEnabledTypes = builder.getAggregationEnabledTypes();
    Assert.assertEquals(aggregationEnabledTypes.size(), 2);
    Assert.assertEquals(aggregationEnabledTypes.get(0), "mockType1");
    Assert.assertEquals(aggregationEnabledTypes.get(1), "mockType2");

    CustomizedStateAggregationConfig customizedStateAggregationConfig = builder.build();

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateAggregationConfig(),
        customizedStateAggregationConfig);

    // Read CustomizedStateAggregationConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CustomizedStateAggregationConfig customizedStateAggregationConfigFromZk =
        _configAccessor.getCustomizedStateAggregationConfig(clusterName);
    List<String> aggregationEnabledTypesFromZk =
        customizedStateAggregationConfigFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(aggregationEnabledTypesFromZk.get(0), "mockType1");
    Assert.assertEquals(aggregationEnabledTypesFromZk.get(1), "mockType2");
  }

}
