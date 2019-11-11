package org.apache.helix.model.cloud;

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
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.cloud.constants.CloudProvider;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCloudConfig extends ZkUnitTestBase {

  @Test(expectedExceptions = HelixException.class)
  public void testCloudConfigNonExistentCluster() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    // Read CloudConfig from Zookeeper and get exception since cluster in not setup yet
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
  }

  @Test(dependsOnMethods = "testCloudConfigNonExistentCluster")
  public void testCloudConfigNull() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    // Read CloudConfig from Zookeeper
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    // since CloudConfig is not written to ZooKeeper, the output should be null
    Assert.assertNull(cloudConfigFromZk);
  }

  @Test(dependsOnMethods = "testCloudConfigNull")
  public void testCloudConfig() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    // Create dummy CloudConfig object
    CloudConfig cloudConfig = new CloudConfig(clusterName);
    cloudConfig.setCloudEnabled(true);
    cloudConfig.setCloudProvider(CloudProvider.AZURE);
    cloudConfig.setCloudID("TestID");
    List<String> infoURL = new ArrayList<String>();
    infoURL.add("TestURL");
    cloudConfig.setCloudInfoSource(infoURL);
    cloudConfig.setCloudInfoFProcessorName("TestProcessor");

    // Write the CloudConfig to Zookeeper
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.cloudConfig(), cloudConfig);

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.AZURE.name());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestID");
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoSources().size(), 1);
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoSources().get(0), "TestURL");
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoProcessorName(), "TestProcessor");
  }

  @Test(expectedExceptions = HelixException.class)
  public void testUnverifiedCloudConfigBuilder() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    CloudConfig.Builder builder = new CloudConfig.Builder(clusterName);
    builder.setCloudEnabled(true);
    // Verify will fail because cloudID has net been defined.
    CloudConfig cloudConfig = builder.build();
  }

  @Test(expectedExceptions = HelixException.class)
  public void testUnverifiedCloudConfigBuilderEmptySources() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    CloudConfig.Builder builder = new CloudConfig.Builder(clusterName);
    builder.setCloudEnabled(true);
    builder.setCloudProvider(CloudProvider.CUSTOMIZED);
    builder.setCloudID("TestID");
    List<String> emptyList = new ArrayList<String>();
    builder.setCloudInfoSources(emptyList);
    builder.setCloudInfoProcessorName("TestProcessor");
    CloudConfig cloudConfig = builder.build();
  }

  @Test(expectedExceptions = HelixException.class)
  public void testUnverifiedCloudConfigBuilderWithoutProcessor() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    CloudConfig.Builder builder = new CloudConfig.Builder(clusterName);
    builder.setCloudEnabled(true);
    builder.setCloudProvider(CloudProvider.CUSTOMIZED);
    builder.setCloudID("TestID");
    List<String> testList = new ArrayList<String>();
    builder.setCloudInfoSources(testList);
    builder.addCloudInfoSource("TestURL");
    CloudConfig cloudConfig = builder.build();
  }

  @Test(dependsOnMethods = "testCloudConfig")
  public void testCloudConfigBuilder() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    CloudConfig.Builder builder = new CloudConfig.Builder(clusterName);
    builder.setCloudEnabled(true);
    builder.setCloudProvider(CloudProvider.CUSTOMIZED);
    builder.setCloudID("TestID");
    builder.addCloudInfoSource("TestURL0");
    builder.addCloudInfoSource("TestURL1");
    builder.setCloudInfoProcessorName("TestProcessor");

    // Check builder getter methods
    Assert.assertTrue(builder.getCloudEnabled());
    Assert.assertEquals(builder.getCloudProvider(), CloudProvider.CUSTOMIZED);
    Assert.assertEquals(builder.getClusterName(), clusterName);
    Assert.assertEquals(builder.getCloudID(), "TestID");
    List<String> listUrlFromBuilder = builder.getCloudInfoSources();
    Assert.assertEquals(listUrlFromBuilder.size(), 2);
    Assert.assertEquals(listUrlFromBuilder.get(0), "TestURL0");
    Assert.assertEquals(listUrlFromBuilder.get(1), "TestURL1");
    Assert.assertEquals(builder.getCloudInfoProcessorName(), "TestProcessor");

    CloudConfig cloudConfig = builder.build();

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.cloudConfig(), cloudConfig);

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestID");
    List<String> listUrlFromZk = cloudConfigFromZk.getCloudInfoSources();
    Assert.assertEquals(listUrlFromZk.get(0), "TestURL0");
    Assert.assertEquals(listUrlFromZk.get(1), "TestURL1");
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoProcessorName(), "TestProcessor");
  }

  @Test(dependsOnMethods = "testCloudConfigBuilder")
  public void testCloudConfigBuilderAzureProvider() {
    String className = getShortClassName();
    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    CloudConfig.Builder builder = new CloudConfig.Builder(clusterName);
    builder.setCloudEnabled(true);
    builder.setCloudProvider(CloudProvider.AZURE);
    builder.setCloudID("TestID");
    builder.setCloudInfoProcessorName("TestProcessor");

    // Check builder getter methods
    Assert.assertTrue(builder.getCloudEnabled());
    Assert.assertEquals(builder.getCloudProvider(), CloudProvider.AZURE);

    CloudConfig cloudConfig = builder.build();

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.cloudConfig(), cloudConfig);

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.AZURE.name());

    // Since CloudProvider is not CUSTOMIZED, CloudInfoProcessor will be null.
    Assert.assertNull(cloudConfigFromZk.getCloudInfoProcessorName());
  }
}
