package org.apache.helix;

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

import java.util.Date;
import java.util.List;

import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestListenerCallback extends ZkUnitTestBase {
  class TestScopedConfigListener implements ScopedConfigChangeListener {
    boolean _configChanged = false;
    int _configSize = 0;

    @Override
    public void onConfigChange(List<HelixProperty> configs, NotificationContext context) {
      _configChanged = true;
      _configSize = configs.size();
    }

    public void reset () {
      _configChanged = false;
      _configSize = 0;
    }
  }

  class TestConfigListener implements InstanceConfigChangeListener, ClusterConfigChangeListener,
      ResourceConfigChangeListener {
    boolean _instanceConfigChanged = false;
    boolean _resourceConfigChanged = false;
    boolean _clusterConfigChanged = false;
    List<ResourceConfig> _resourceConfigs;
    List<InstanceConfig> _instanceConfigs;
    ClusterConfig _clusterConfig;

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
        NotificationContext context) {
      _instanceConfigChanged = true;
      _instanceConfigs = instanceConfigs;
    }

    @Override
    public void onClusterConfigChange(ClusterConfig clusterConfig,
        NotificationContext context) {
      _clusterConfigChanged = true;
      _clusterConfig = clusterConfig;
    }

    @Override
    public void onResourceConfigChange(List<ResourceConfig> resourceConfigs,
        NotificationContext context) {
      _resourceConfigChanged = true;
      _resourceConfigs = resourceConfigs;
    }

    public void reset () {
      _instanceConfigChanged = false;
      _resourceConfigChanged = false;
      _clusterConfigChanged = false;
      _resourceConfigs = null;
      _instanceConfigs = null;
      _clusterConfig = null;
    }
  }


  int _numNodes = 3;
  int _numResources = 4;
  HelixManager _manager;
  String _clusterName;

  @BeforeClass
  public void beforeClass() throws Exception {
    _clusterName = TestHelper.getTestClassName();
    System.out.println("START " + _clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        _numResources, // resources
        32, // partitions per resource
        _numNodes, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    _manager = HelixManagerFactory
        .getZKHelixManager(_clusterName, "localhost", InstanceType.SPECTATOR, ZK_ADDR);

    _manager.connect();
  }

  @AfterClass
  public void afterClass() throws Exception {
    TestHelper.dropCluster(_clusterName, _gZkClient);
    System.out.println("END " + _clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testConfigChangeListeners() throws Exception {
    TestConfigListener listener = new TestConfigListener();
    listener.reset();
    _manager.addInstanceConfigChangeListener(listener);
    Assert.assertTrue(listener._instanceConfigChanged,
        "Should get initial instanceConfig callback invoked");
    Assert.assertEquals(listener._instanceConfigs.size(), _numNodes,
        "Instance Config size does not match");

    listener.reset();
    _manager.addClusterfigChangeListener(listener);
    Assert.assertTrue(listener._clusterConfigChanged,
        "Should get initial clusterConfig callback invoked");
    Assert.assertNotNull(listener._clusterConfig, "Cluster Config size should not be null");

    listener.reset();
    _manager.addResourceConfigChangeListener(listener);
    Assert.assertTrue(listener._resourceConfigChanged,
        "Should get initial resourceConfig callback invoked");
    Assert.assertEquals(listener._resourceConfigs.size(), 0,
        "Instance Config size does not match");

    // test change content
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    String instanceName = "localhost_12918";
    HelixProperty value = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener.reset();
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), value);
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._instanceConfigChanged,
        "Should get instanceConfig callback invoked since we change instanceConfig");
    Assert.assertEquals(listener._instanceConfigs.size(), _numNodes,
        "Instance Config size does not match");

    value = accessor.getProperty(keyBuilder.clusterConfig());
    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener.reset();
    accessor.setProperty(keyBuilder.clusterConfig(), value);
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._clusterConfigChanged,
        "Should get clusterConfig callback invoked since we change clusterConfig");
    Assert.assertNotNull(listener._clusterConfig, "Cluster Config size should not be null");


    String resourceName = "TestDB_0";
    value = new HelixProperty(resourceName);
    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener.reset();
    accessor.setProperty(keyBuilder.resourceConfig(resourceName), value);
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._resourceConfigChanged,
        "Should get resourceConfig callback invoked since we add resourceConfig");
    Assert.assertEquals(listener._resourceConfigs.size(), 1, "Resource config size does not match");

    listener.reset();
    accessor.removeProperty(keyBuilder.resourceConfig(resourceName));
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._resourceConfigChanged,
        "Should get resourceConfig callback invoked since we add resourceConfig");
    Assert.assertEquals(listener._resourceConfigs.size(), 0, "Instance Config size does not match");
  }

  @Test
  public void testScopedConfigChangeListener() throws Exception {
    TestScopedConfigListener listener = new TestScopedConfigListener();

    listener.reset();
    _manager.addConfigChangeListener(listener, ConfigScopeProperty.CLUSTER);
    Assert.assertTrue(listener._configChanged, "Should get initial clusterConfig callback invoked");
    Assert.assertEquals(listener._configSize, 1, "Cluster Config size should be 1");

    listener.reset();
    _manager.addConfigChangeListener(listener, ConfigScopeProperty.RESOURCE);
    Assert
        .assertTrue(listener._configChanged, "Should get initial resourceConfig callback invoked");
    Assert.assertEquals(listener._configSize, 0, "Resource Config size does not match");

    listener.reset();
    _manager.addConfigChangeListener(listener, ConfigScopeProperty.PARTICIPANT);
    Assert
        .assertTrue(listener._configChanged, "Should get initial instanceConfig callback invoked");
    Assert.assertEquals(listener._configSize, _numNodes, "Instance Config size does not match");

    // test change content
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    String instanceName = "localhost_12918";
    HelixProperty value = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener.reset();
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), value);
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._configChanged,
        "Should get instanceConfig callback invoked since we change instanceConfig");
    Assert.assertEquals(listener._configSize, _numNodes, "Instance Config size does not match");

    value = accessor.getProperty(keyBuilder.clusterConfig());
    value._record.setSimpleField("" + System.currentTimeMillis(), "newClusterValue");
    listener.reset();
    accessor.setProperty(keyBuilder.clusterConfig(), value);
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._configChanged,
        "Should get clusterConfig callback invoked since we change clusterConfig");
    Assert.assertEquals(listener._configSize, 1, "Cluster Config size does not match");

    String resourceName = "TestDB_0";
    value = new HelixProperty(resourceName);
    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener.reset();
    accessor.setProperty(keyBuilder.resourceConfig(resourceName), value);
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._configChanged,
        "Should get resourceConfig callback invoked since we add resourceConfig");
    Assert.assertEquals(listener._configSize, 1, "Resource Config size does not match");

    listener.reset();
    accessor.removeProperty(keyBuilder.resourceConfig(resourceName));
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._configChanged,
        "Should get resourceConfig callback invoked since we add resourceConfig");
    Assert.assertEquals(listener._configSize, 0, "Resource Config size does not match");
  }
}
