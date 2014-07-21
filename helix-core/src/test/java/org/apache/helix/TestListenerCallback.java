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

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestListenerCallback extends ZkTestBase {
  class TestListener implements InstanceConfigChangeListener, ScopedConfigChangeListener {
    boolean _instanceConfigChanged = false;
    boolean _configChanged = false;

    @Override
    public void onConfigChange(List<HelixProperty> configs, NotificationContext context) {
      _configChanged = true;
      System.out.println("onConfigChange invoked: " + configs.size() + ", " + configs);
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
        NotificationContext context) {
      _instanceConfigChanged = true;
      System.out.println("onInstanceConfigChange invoked: " + instanceConfigs);
    }

  }

  @Test
  public void testBasic() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(clusterName, "localhost", InstanceType.SPECTATOR,
            _zkaddr);

    manager.connect();

    TestListener listener = new TestListener();
    listener._instanceConfigChanged = false;
    manager.addInstanceConfigChangeListener(listener);
    Assert.assertTrue(listener._instanceConfigChanged,
        "Should get initial instanceConfig callback invoked");

    listener._configChanged = false;
    manager.addConfigChangeListener(listener, ConfigScopeProperty.CLUSTER);
    Assert.assertTrue(listener._configChanged, "Should get initial clusterConfig callback invoked");

    listener._configChanged = false;
    manager.addConfigChangeListener(listener, ConfigScopeProperty.RESOURCE);
    Assert
        .assertTrue(listener._configChanged, "Should get initial resourceConfig callback invoked");

    // test change content
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    String instanceName = "localhost_12918";
    HelixProperty value = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener._instanceConfigChanged = false;
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), value);
    Thread.sleep(1000); // wait zk callback
    Assert.assertTrue(listener._instanceConfigChanged,
        "Should get instanceConfig callback invoked since we change instanceConfig");

    value = accessor.getProperty(keyBuilder.clusterConfig());
    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener._configChanged = false;
    accessor.setProperty(keyBuilder.clusterConfig(), value);
    Thread.sleep(1000); // wait zk callback
    Assert.assertTrue(listener._configChanged,
        "Should get clusterConfig callback invoked since we change clusterConfig");

    String resourceName = "TestDB_0";
    value = new HelixProperty(resourceName);
    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener._configChanged = false;
    accessor.setProperty(keyBuilder.resourceConfig(resourceName), value);
    Thread.sleep(1000); // wait zk callback
    Assert.assertTrue(listener._configChanged,
        "Should get resourceConfig callback invoked since we add resourceConfig");

    value._record.setSimpleField("" + System.currentTimeMillis(), "newValue");
    listener._configChanged = false;
    accessor.setProperty(keyBuilder.resourceConfig(resourceName), value);
    Thread.sleep(1000); // wait zk callback
    Assert.assertTrue(listener._configChanged,
        "Should get resourceConfig callback invoked since we change resourceConfig");

    listener._configChanged = false;
    accessor.removeProperty(keyBuilder.resourceConfig(resourceName));
    Thread.sleep(1000); // wait zk callback
    Assert.assertTrue(listener._configChanged,
        "Should get resourceConfig callback invoked since we delete resourceConfig");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
