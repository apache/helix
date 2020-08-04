package org.apache.helix.integration;

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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.participant.CustomCodeCallbackHandler;
import org.apache.helix.participant.HelixCustomCodeRunner;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisableCustomCodeRunner extends ZkUnitTestBase {

  private static final int N = 2;
  private static final int PARTITION_NUM = 1;

  class DummyCallback implements CustomCodeCallbackHandler {
    private final Map<NotificationContext.Type, Boolean> _callbackInvokeMap = new HashMap<>();

    @Override
    public void onCallback(NotificationContext context) {
      NotificationContext.Type type = context.getType();
      _callbackInvokeMap.put(type, Boolean.TRUE);
    }

    public void reset() {
      _callbackInvokeMap.clear();
    }

    boolean isInitTypeInvoked() {
      return _callbackInvokeMap.containsKey(NotificationContext.Type.INIT);
    }

    boolean isCallbackTypeInvoked() {
      return _callbackInvokeMap.containsKey(NotificationContext.Type.CALLBACK);
    }

    boolean isFinalizeTypeInvoked() {
      return _callbackInvokeMap.containsKey(NotificationContext.Type.FINALIZE);
    }
  }

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        PARTITION_NUM, // partitions per resource
        N, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start participants
    Map<String, MockParticipantManager> participants = new HashMap<>();
    Map<String, HelixCustomCodeRunner> customCodeRunners = new HashMap<>();
    Map<String, DummyCallback> callbacks = new HashMap<>();
    for (int i = 0; i < N; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants.put(instanceName,
          new MockParticipantManager(ZK_ADDR, clusterName, instanceName));

      customCodeRunners.put(instanceName,
          new HelixCustomCodeRunner(participants.get(instanceName), ZK_ADDR));
      callbacks.put(instanceName, new DummyCallback());

      customCodeRunners.get(instanceName).invoke(callbacks.get(instanceName))
          .on(ChangeType.LIVE_INSTANCE).usingLeaderStandbyModel("TestParticLeader").start();
      participants.get(instanceName).syncStart();
    }

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // Make sure callback is registered
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    final HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    final String customCodeRunnerResource =
        customCodeRunners.get("localhost_12918").getResourceName();
    ExternalView extView = accessor.getProperty(keyBuilder.externalView(customCodeRunnerResource));
    Map<String, String> instanceStates = extView.getStateMap(customCodeRunnerResource + "_0");
    String leader = null;
    for (String instance : instanceStates.keySet()) {
      String state = instanceStates.get(instance);
      if ("LEADER".equals(state)) {
        leader = instance;
        break;
      }
    }
    Assert.assertNotNull(leader);
    for (String instance : callbacks.keySet()) {
      DummyCallback callback = callbacks.get(instance);
      if (instance.equals(leader)) {
        Assert.assertTrue(callback.isInitTypeInvoked());
      } else {
        Assert.assertFalse(callback.isInitTypeInvoked());
      }
      callback.reset();
    }

    // Disable custom-code runner resource
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.enableResource(clusterName, customCodeRunnerResource, false);

    // Verify that states of custom-code runner are all OFFLINE
    result = TestHelper.verify(() -> {
      PropertyKey.Builder keyBuilder1 = accessor.keyBuilder();

      ExternalView extView1 =
          accessor.getProperty(keyBuilder1.externalView(customCodeRunnerResource));
      if (extView1 == null) {
        return false;
      }
      Set<String> partitionSet = extView1.getPartitionSet();
      if (partitionSet == null || partitionSet.size() != PARTITION_NUM) {
        return false;
      }
      for (String partition : partitionSet) {
        Map<String, String> instanceStates1 = extView1.getStateMap(partition);
        for (String state : instanceStates1.values()) {
          if (!"OFFLINE".equals(state)) {
            return false;
          }
        }
      }
      return true;
    }, 10 * 1000);
    Assert.assertTrue(result);

    // Change live-instance should not invoke any custom-code runner
    String fakeInstanceName = "fakeInstance";
    InstanceConfig instanceConfig = new InstanceConfig(fakeInstanceName);
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("10000");
    instanceConfig.setInstanceEnabled(true);
    admin.addInstance(clusterName, instanceConfig);

    LiveInstance fakeInstance = new LiveInstance(fakeInstanceName);
    fakeInstance.setSessionId("fakeSessionId");
    fakeInstance.setHelixVersion("0.6");
    accessor.setProperty(keyBuilder.liveInstance(fakeInstanceName), fakeInstance);
    Thread.sleep(1000);

    for (Map.Entry<String, DummyCallback> e : callbacks.entrySet()) {
      String instance = e.getKey();
      DummyCallback callback = e.getValue();
      Assert.assertFalse(callback.isInitTypeInvoked());
      Assert.assertFalse(callback.isCallbackTypeInvoked());

      // Ensure that we were told that a leader stopped being the leader
      if (instance.equals(leader)) {
        Assert.assertTrue(callback.isFinalizeTypeInvoked());
      }
    }

    // Remove fake instance
    accessor.removeProperty(keyBuilder.liveInstance(fakeInstanceName));

    // Re-enable custom-code runner
    admin.enableResource(clusterName, customCodeRunnerResource, true);
    result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // Verify that custom-invoke is invoked again
    extView = accessor.getProperty(keyBuilder.externalView(customCodeRunnerResource));
    instanceStates = extView.getStateMap(customCodeRunnerResource + "_0");
    leader = null;
    for (String instance : instanceStates.keySet()) {
      String state = instanceStates.get(instance);
      if ("LEADER".equals(state)) {
        leader = instance;
        break;
      }
    }
    Assert.assertNotNull(leader);
    for (String instance : callbacks.keySet()) {
      DummyCallback callback = callbacks.get(instance);
      if (instance.equals(leader)) {
        Assert.assertTrue(callback.isInitTypeInvoked());
      } else {
        Assert.assertFalse(callback.isInitTypeInvoked());
      }
      callback.reset();
    }

    // Add a fake instance should invoke custom-code runner
    accessor.setProperty(keyBuilder.liveInstance(fakeInstanceName), fakeInstance);
    Thread.sleep(1000);
    for (String instance : callbacks.keySet()) {
      DummyCallback callback = callbacks.get(instance);
      if (instance.equals(leader)) {
        Assert.assertTrue(callback.isCallbackTypeInvoked());
      } else {
        Assert.assertFalse(callback.isCallbackTypeInvoked());
      }
    }

    // Clean up
    controller.syncStop();
    for (MockParticipantManager participant : participants.values()) {
      participant.syncStop();
    }

    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
