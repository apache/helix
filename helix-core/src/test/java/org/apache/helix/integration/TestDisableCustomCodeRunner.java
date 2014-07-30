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
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.participant.CustomCodeCallbackHandler;
import org.apache.helix.participant.HelixCustomCodeRunner;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisableCustomCodeRunner extends ZkTestBase {

  private static final int N = 2;
  private static final int PARTITION_NUM = 1;

  class DummyCallback implements CustomCodeCallbackHandler {
    private final Map<NotificationContext.Type, Boolean> _callbackInvokeMap =
        new HashMap<NotificationContext.Type, Boolean>();

    @Override
    public void onCallback(NotificationContext context) {
      NotificationContext.Type type = context.getType();
      _callbackInvokeMap.put(type, Boolean.TRUE);
    }

    public void reset() {
      _callbackInvokeMap.clear();
    }

    public boolean isInitTypeInvoked() {
      return _callbackInvokeMap.containsKey(NotificationContext.Type.INIT);
    }

    public boolean isCallbackTypeInvoked() {
      return _callbackInvokeMap.containsKey(NotificationContext.Type.CALLBACK);
    }

    public boolean isFinalizeTypeInvoked() {
      return _callbackInvokeMap.containsKey(NotificationContext.Type.FINALIZE);
    }
  }

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        PARTITION_NUM, // partitions per resource
        N, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    Map<String, MockParticipant> participants =
        new HashMap<String, MockParticipant>();
    Map<String, HelixCustomCodeRunner> customCodeRunners =
        new HashMap<String, HelixCustomCodeRunner>();
    Map<String, DummyCallback> callbacks = new HashMap<String, DummyCallback>();
    for (int i = 0; i < N; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants
          .put(instanceName, new MockParticipant(_zkaddr, clusterName, instanceName));

      customCodeRunners.put(instanceName, new HelixCustomCodeRunner(participants.get(instanceName),
          _zkaddr));
      callbacks.put(instanceName, new DummyCallback());

      customCodeRunners.get(instanceName).invoke(callbacks.get(instanceName))
          .on(ChangeType.LIVE_INSTANCE).usingLeaderStandbyModel("TestParticLeader").start();
      participants.get(instanceName).syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // Make sure callback is registered
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
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
    HelixAdmin admin = new ZKHelixAdmin(_zkclient);
    admin.enableResource(clusterName, customCodeRunnerResource, false);

    // Verify that states of custom-code runner are all OFFLINE
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        ExternalView extView =
            accessor.getProperty(keyBuilder.externalView(customCodeRunnerResource));
        if (extView == null) {
          return false;
        }
        Set<String> partitionSet = extView.getPartitionSet();
        if (partitionSet == null || partitionSet.size() != PARTITION_NUM) {
          return false;
        }
        for (String partition : partitionSet) {
          Map<String, String> instanceStates = extView.getStateMap(partition);
          for (String state : instanceStates.values()) {
            if (!"OFFLINE".equals(state)) {
              return false;
            }
          }
        }
        return true;
      }
    }, 10 * 1000);
    Assert.assertTrue(result);

    // Change live-instance should not invoke any custom-code runner
    LiveInstance fakeInstance = new LiveInstance("fakeInstance");
    fakeInstance.setSessionId("fakeSessionId");
    fakeInstance.setHelixVersion("0.6");
    accessor.setProperty(keyBuilder.liveInstance("fakeInstance"), fakeInstance);
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
    accessor.removeProperty(keyBuilder.liveInstance("fakeInstance"));

    // Re-enable custom-code runner
    admin.enableResource(clusterName, customCodeRunnerResource, true);
    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
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
    accessor.setProperty(keyBuilder.liveInstance("fakeInstance"), fakeInstance);
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
    for (MockParticipant participant : participants.values()) {
      participant.syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
