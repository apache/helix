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
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import java.util.List;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCustomizedStateConfig extends ZkTestBase {

  @Test(expectedExceptions = HelixException.class)
  public void TestCustomizedStateConfigNonExistentCluster() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    // Read CustomizedStateConfig from Zookeeper and get exception since cluster in not setup yet
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig customizedStateConfig =
        _configAccessor.getCustomizedStateConfig(clusterName);
  }

  @Test(dependsOnMethods = "TestCustomizedStateConfigNonExistentCluster")
  public void testCustomizedStateConfigNull() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    // Read CustomizedStateConfig from Zookeeper
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig customizedStateConfigFromZk =
        _configAccessor.getCustomizedStateConfig(clusterName);
    Assert.assertNull(customizedStateConfigFromZk);
  }

  @Test(dependsOnMethods = "testCustomizedStateConfigNull")
  public void testCustomizedStateConfig() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    // Create dummy CustomizedStateConfig object
    CustomizedStateConfig.Builder customizedStateConfigBuilder =
        new CustomizedStateConfig.Builder();
    List<String> aggregationEnabledTypes = new ArrayList<String>();
    aggregationEnabledTypes.add("mockType1");
    aggregationEnabledTypes.add("mockType2");
    customizedStateConfigBuilder.setAggregationEnabledTypes(aggregationEnabledTypes);
    CustomizedStateConfig customizedStateConfig =
        customizedStateConfigBuilder.build();

    // Write the CustomizedStateConfig to Zookeeper
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(ZK_ADDR));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateConfig(),
        customizedStateConfig);

    // Read CustomizedStateConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig customizedStateConfigFromZk =
        _configAccessor.getCustomizedStateConfig(clusterName);
    Assert.assertEquals(customizedStateConfigFromZk.getAggregationEnabledTypes().size(),
        2);
    Assert.assertEquals(aggregationEnabledTypes.get(0), "mockType1");
    Assert.assertEquals(aggregationEnabledTypes.get(1), "mockType2");
  }

  @Test(dependsOnMethods = "testCustomizedStateConfig")
  public void testCustomizedStateConfigBuilder() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    CustomizedStateConfig.Builder builder =
        new CustomizedStateConfig.Builder();
    builder.addAggregationEnabledType("mockType1");
    builder.addAggregationEnabledType("mockType2");

    // Check builder getter methods
    List<String> aggregationEnabledTypes = builder.getAggregationEnabledTypes();
    Assert.assertEquals(aggregationEnabledTypes.size(), 2);
    Assert.assertEquals(aggregationEnabledTypes.get(0), "mockType1");
    Assert.assertEquals(aggregationEnabledTypes.get(1), "mockType2");

    CustomizedStateConfig customizedStateConfig = builder.build();

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(ZK_ADDR));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateConfig(),
        customizedStateConfig);

    // Read CustomizedStateConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig customizedStateConfigFromZk =
        _configAccessor.getCustomizedStateConfig(clusterName);
    List<String> aggregationEnabledTypesFromZk =
        customizedStateConfigFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(aggregationEnabledTypesFromZk.get(0), "mockType1");
    Assert.assertEquals(aggregationEnabledTypesFromZk.get(1), "mockType2");
  }

}
