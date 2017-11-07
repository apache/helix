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
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestListenerCallbackPrefetch extends ZkUnitTestBase {

  class PrefetchListener implements InstanceConfigChangeListener, IdealStateChangeListener {
    boolean _idealStateChanged = false;
    boolean _instanceConfigChanged = false;
    boolean _containIdealStates = false;
    boolean _containInstanceConfig = false;

    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
      _idealStateChanged = true;
      _containIdealStates = idealState.isEmpty()? false : true;
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
      _instanceConfigChanged = true;
      _containInstanceConfig = instanceConfigs.isEmpty()? false : true;
    }

    public void reset() {
      _idealStateChanged = false;
      _instanceConfigChanged = false;
      _containIdealStates = false;
      _containInstanceConfig = false;
    }
  }

  @PreFetch (enabled = false)
  class NonPrefetchListener extends PrefetchListener {
  }


  class MixedPrefetchListener extends PrefetchListener {
    @PreFetch(enabled = false)
    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
      _idealStateChanged = true;
      _containIdealStates = idealState.isEmpty()? false : true;
    }
  }


  private HelixManager _manager;

  @BeforeClass
  public void beforeClass()
      throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    _manager =
        HelixManagerFactory.getZKHelixManager(clusterName, "localhost", InstanceType.SPECTATOR,
            ZK_ADDR);

    _manager.connect();
  }

  @AfterClass
  public void afterClass()
      throws Exception {
    _manager.disconnect();
  }

  @Test
  public void testPrefetch() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + methodName + " at " + new Date(System.currentTimeMillis()));

    PrefetchListener listener = new PrefetchListener();
    _manager.addInstanceConfigChangeListener(listener);
    _manager.addIdealStateChangeListener(listener);
    Assert.assertTrue(listener._instanceConfigChanged);
    Assert.assertTrue(listener._idealStateChanged);
    Assert.assertTrue(listener._containInstanceConfig);
    Assert.assertTrue(listener._containIdealStates);

    listener.reset();
    // test change content
    updateInstanceConfig();
    updateIdealState();
    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._instanceConfigChanged);
    Assert.assertTrue(listener._containInstanceConfig);
    Assert.assertTrue(listener._idealStateChanged);
    Assert.assertTrue(listener._containIdealStates);

    System.out.println("END " + methodName + " at " + new Date(System.currentTimeMillis()));
  }


  @Test
  public void testNoPrefetch() throws Exception {
    String methodName = TestHelper.getTestMethodName();

    NonPrefetchListener listener = new NonPrefetchListener();
    _manager.addInstanceConfigChangeListener(listener);
    _manager.addIdealStateChangeListener(listener);
    Assert.assertTrue(listener._instanceConfigChanged);
    Assert.assertTrue(listener._idealStateChanged);
    Assert.assertFalse(listener._containInstanceConfig);
    Assert.assertFalse(listener._containIdealStates);
    listener.reset();

    updateInstanceConfig();
    updateIdealState();

    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._instanceConfigChanged);
    Assert.assertFalse(listener._containInstanceConfig);
    Assert.assertTrue(listener._idealStateChanged);
    Assert.assertFalse(listener._containIdealStates);

    System.out.println("END " + methodName + " at " + new Date(System.currentTimeMillis()));
  }


  @Test
  public void testMixed() throws Exception {
    String methodName = TestHelper.getTestMethodName();

    MixedPrefetchListener listener = new MixedPrefetchListener();
    _manager.addInstanceConfigChangeListener(listener);
    _manager.addIdealStateChangeListener(listener);
    Assert.assertTrue(listener._instanceConfigChanged);
    Assert.assertTrue(listener._idealStateChanged);
    Assert.assertTrue(listener._containInstanceConfig);
    Assert.assertFalse(listener._containIdealStates);

    listener.reset();

    updateInstanceConfig();
    updateIdealState();

    Thread.sleep(500); // wait zk callback
    Assert.assertTrue(listener._instanceConfigChanged);
    Assert.assertTrue(listener._containInstanceConfig);
    Assert.assertTrue(listener._idealStateChanged);
    Assert.assertFalse(listener._containIdealStates);

    System.out.println("END " + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  private void updateInstanceConfig() {
    // test change content
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    String instanceName = "localhost_12918";
    HelixProperty value = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    value._record.setLongField("TimeStamp", System.currentTimeMillis());
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), value);
  }

  private void updateIdealState() {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    List<String> idealStates = accessor.getChildNames(keyBuilder.idealStates());
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(idealStates.get(0)));
    idealState.setNumPartitions((int)(System.currentTimeMillis()%50L));
    accessor.setProperty(keyBuilder.idealStates(idealState.getId()), idealState);
  }
}
