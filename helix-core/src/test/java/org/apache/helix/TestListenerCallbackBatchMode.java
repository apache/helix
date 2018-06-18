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
import java.util.Random;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestListenerCallbackBatchMode extends ZkUnitTestBase {

  class Listener implements InstanceConfigChangeListener, IdealStateChangeListener {
    int _idealStateChangedCount = 0;
    int _instanceConfigChangedCount = 0;

    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
      if (changeContext.getType().equals(NotificationContext.Type.CALLBACK)) {
        _idealStateChangedCount++;
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
        NotificationContext context) {
      if (context.getType().equals(NotificationContext.Type.CALLBACK)) {
        _instanceConfigChangedCount++;
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    public void reset() {
      _idealStateChangedCount = 0;
      _instanceConfigChangedCount = 0;
    }
  }

  @BatchMode
  class BatchedListener extends Listener {
  }


  class MixedListener extends Listener {
    @BatchMode
    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
      super.onIdealStateChange(idealState, changeContext);
    }
  }

  @BatchMode (enabled = false)
  class BatchDisableddListener extends Listener {
    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
      super.onIdealStateChange(idealState, changeContext);
    }
  }


  private HelixManager _manager;
  private int _numNode = 8;
  private int _numResource = 8;

  String clusterName = TestHelper.getTestClassName();

  @BeforeClass
  public void beforeClass()
      throws Exception {
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        _numResource, // resources
        4, // partitions per resource
        _numNode, // number of nodes
        1, // replicas
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
    deleteCluster(clusterName);
  }


  @Test
  public void testNonBatchedListener() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + methodName + " at " + new Date(System.currentTimeMillis()));

    final Listener listener = new Listener();
    addListeners(listener);
    updateConfigs();
    verifyNonbatchedListeners(listener);
    removeListeners(listener);

    System.out.println("END " + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test (dependsOnMethods = {"testNonBatchedListener", "testBatchedListener", "testMixedListener"})
  public void testEnableBatchedListenerByJavaProperty() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + methodName + " at " + new Date(System.currentTimeMillis()));

    System.setProperty("isAsyncBatchModeEnabled", "true");

    Listener listener = new Listener();
    addListeners(listener);
    updateConfigs();
    verifyBatchedListeners(listener);

    System.setProperty("isAsyncBatchModeEnabled", "false");
    removeListeners(listener);

    System.setProperty("helix.callbackhandler.isAsyncBatchModeEnabled", "true");

    listener = new Listener();
    addListeners(listener);
    updateConfigs();
    verifyBatchedListeners(listener);

    System.setProperty("helix.callbackhandler.isAsyncBatchModeEnabled", "false");
    removeListeners(listener);

    System.out.println("END " + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test (dependsOnMethods = {"testNonBatchedListener", "testBatchedListener", "testMixedListener"})
  public void testDisableBatchedListenerByAnnotation() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + methodName + " at " + new Date(System.currentTimeMillis()));

    System.setProperty("isAsyncBatchModeEnabled", "true");

    final Listener listener = new BatchDisableddListener();
    addListeners(listener);
    updateConfigs();
    verifyNonbatchedListeners(listener);

    System.setProperty("isAsyncBatchModeEnabled", "false");
    removeListeners(listener);

    System.out.println("END " + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testBatchedListener() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + methodName + " at " + new Date(System.currentTimeMillis()));

    final BatchedListener batchListener = new BatchedListener();
    addListeners(batchListener);
    updateConfigs();
    verifyBatchedListeners(batchListener);
    removeListeners(batchListener);

    System.out.println("END " + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMixedListener() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + methodName + " at " + new Date(System.currentTimeMillis()));

    final MixedListener mixedListener = new MixedListener();
    addListeners(mixedListener);
    updateConfigs();

    Thread.sleep(4000);
    boolean result = (mixedListener._instanceConfigChangedCount == _numNode) && (
        mixedListener._idealStateChangedCount < _numResource/2);

    Assert.assertTrue(result, "instance callbacks: " + mixedListener._instanceConfigChangedCount
        + ", idealstate callbacks " + mixedListener._idealStateChangedCount + "\ninstance count: "
        + _numNode + ", idealstate counts: " + _numResource);

    removeListeners(mixedListener);

    System.out.println("END " + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  private void verifyNonbatchedListeners(final Listener listener) throws Exception {
    Boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() {
        return (listener._instanceConfigChangedCount == _numNode) && (
            listener._idealStateChangedCount == _numResource);
      }
    }, 12000);

    Thread.sleep(50);
    Assert.assertTrue(result,
        "instance callbacks: " + listener._instanceConfigChangedCount + ", idealstate callbacks "
            + listener._idealStateChangedCount + "\ninstance count: " + _numNode + ", idealstate counts: " + _numResource);
  }

  private void verifyBatchedListeners(Listener batchListener) throws InterruptedException {
    Thread.sleep(3000);
    boolean result = (batchListener._instanceConfigChangedCount < _numNode) && (
        batchListener._idealStateChangedCount < _numResource);

    Assert.assertTrue(result, "instance callbacks: " + batchListener._instanceConfigChangedCount
        + ", idealstate callbacks " + batchListener._idealStateChangedCount + "\ninstance count: "
        + _numNode + ", idealstate counts: " + _numResource);

  }

  private void addListeners(Listener listener) throws Exception {
    _manager.addInstanceConfigChangeListener(listener);
    _manager.addIdealStateChangeListener(listener);
  }

  private void removeListeners(Listener listener) throws Exception {
    _manager.removeListener(new PropertyKey.Builder(_manager.getClusterName()).instanceConfigs(),
        listener);
    _manager
        .removeListener(new PropertyKey.Builder(_manager.getClusterName()).idealStates(), listener);
  }

  private void updateConfigs() throws InterruptedException {
    final Random r = new Random(System.currentTimeMillis());
    // test change content
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    final List<String> instances = accessor.getChildNames(keyBuilder.instanceConfigs());
    for (String instance : instances) {
      InstanceConfig value = accessor.getProperty(keyBuilder.instanceConfig(instance));
      value._record.setLongField("TimeStamp", System.currentTimeMillis());
      accessor.setProperty(keyBuilder.instanceConfig(instance), value);
      Thread.sleep(50);
    }

    final List<String> resources = accessor.getChildNames(keyBuilder.idealStates());
    for (String resource : resources) {
      IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resource));
      idealState.setNumPartitions(r.nextInt(100));
      accessor.setProperty(keyBuilder.idealStates(idealState.getId()), idealState);
      Thread.sleep(20); // wait zk callback
    }
  }
}
