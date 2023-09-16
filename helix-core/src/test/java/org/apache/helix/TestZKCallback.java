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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.CustomizedStateConfigChangeListener;
import org.apache.helix.api.listeners.CustomizedStateRootChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.TaskCurrentStateChangeListener;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZKCallback extends ZkTestBase {
  private final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();

  private static String[] createArgs(String str) {
    String[] split = str.split("[ ]+");
    System.out.println(Arrays.toString(split));
    return split;
  }

  public class TestCallbackListener implements MessageListener, LiveInstanceChangeListener,
                                               ConfigChangeListener, CurrentStateChangeListener,
                                               TaskCurrentStateChangeListener,
                                               CustomizedStateConfigChangeListener,
                                               CustomizedStateRootChangeListener,
                                               ExternalViewChangeListener,
      IdealStateChangeListener {
    boolean externalViewChangeReceived = false;
    boolean liveInstanceChangeReceived = false;
    boolean configChangeReceived = false;
    boolean currentStateChangeReceived = false;
    boolean taskCurrentStateChangeReceived = false;
    boolean customizedStateConfigChangeReceived = false;
    boolean customizedStateRootChangeReceived = false;
    boolean messageChangeReceived = false;
    boolean idealStateChangeReceived = false;

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList,
        NotificationContext changeContext) {
      externalViewChangeReceived = true;
    }

    @Override
    public void onStateChange(String instanceName, List<CurrentState> statesInfo,
        NotificationContext changeContext) {
      currentStateChangeReceived = true;
    }

    @Override
    public void onTaskCurrentStateChange(String instanceName, List<CurrentState> statesInfo,
        NotificationContext changeContext) {
      taskCurrentStateChangeReceived = true;
    }

    @Override
    public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
      configChangeReceived = true;
    }

    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances,
        NotificationContext changeContext) {
      liveInstanceChangeReceived = true;
    }

    @Override
    public void onMessage(String instanceName, List<Message> messages,
        NotificationContext changeContext) {
      messageChangeReceived = true;
    }

    void Reset() {
      externalViewChangeReceived = false;
      liveInstanceChangeReceived = false;
      configChangeReceived = false;
      currentStateChangeReceived = false;
      customizedStateConfigChangeReceived = false;
      customizedStateRootChangeReceived = false;
      messageChangeReceived = false;
      idealStateChangeReceived = false;
    }

    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
      // TODO Auto-generated method stub
      idealStateChangeReceived = true;
    }


    @Override
    public void onCustomizedStateRootChange(String instanceName, List<String> customizedStateTypes,
        NotificationContext changeContext) {
      // TODO Auto-generated method stub
      customizedStateRootChangeReceived = true;
    }

    @Override
    public void onCustomizedStateConfigChange(CustomizedStateConfig customizedStateConfig,
        NotificationContext context) {
      // TODO Auto-generated method stub
      customizedStateConfigChangeReceived = true;
    }
  }

  @Test()
  public void testInvocation() throws Exception {

    HelixManager testHelixManager =
        HelixManagerFactory.getZKHelixManager(clusterName, "localhost_8900",
            InstanceType.PARTICIPANT, ZK_ADDR);
    testHelixManager.connect();

    try {
      TestZKCallback test = new TestZKCallback();

      TestZKCallback.TestCallbackListener testListener = test.new TestCallbackListener();

      testHelixManager.addMessageListener(testListener, "localhost_8900");
      testHelixManager.addCurrentStateChangeListener(testListener, "localhost_8900",
          testHelixManager.getSessionId());
      testHelixManager.addTaskCurrentStateChangeListener(testListener, "localhost_8900",
          testHelixManager.getSessionId());
      testHelixManager.addCustomizedStateRootChangeListener(testListener, "localhost_8900");
      testHelixManager.addConfigChangeListener(testListener);
      testHelixManager.addIdealStateChangeListener(testListener);
      testHelixManager.addExternalViewChangeListener(testListener);
      testHelixManager.addLiveInstanceChangeListener(testListener);
      // Initial add listener should trigger the first execution of the
      // listener callbacks
      AssertJUnit.assertTrue(
          testListener.configChangeReceived & testListener.currentStateChangeReceived & testListener.externalViewChangeReceived
              & testListener.idealStateChangeReceived & testListener.liveInstanceChangeReceived & testListener.messageChangeReceived);

      testListener.Reset();
      HelixDataAccessor accessor = testHelixManager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      ExternalView extView = new ExternalView("db-12345");
      accessor.setProperty(keyBuilder.externalView("db-12345"), extView);
      boolean result = TestHelper.verify(() -> {
        return testListener.externalViewChangeReceived;
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(result);
      testListener.Reset();

      CurrentState curState = new CurrentState("db-12345");
      curState.setSessionId("sessionId");
      curState.setStateModelDefRef("StateModelDef");
      accessor.setProperty(keyBuilder
              .currentState("localhost_8900", testHelixManager.getSessionId(), curState.getId()),
          curState);
      result = TestHelper.verify(() -> {
        return testListener.currentStateChangeReceived;
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(result);
      testListener.Reset();

      CurrentState taskCurState = new CurrentState("db-12345");
      taskCurState.setSessionId("sessionId");
      taskCurState.setStateModelDefRef("StateModelDef");
      accessor.setProperty(keyBuilder
          .taskCurrentState("localhost_8900", testHelixManager.getSessionId(),
              taskCurState.getId()), taskCurState);
      result = TestHelper.verify(() -> {
        return testListener.taskCurrentStateChangeReceived;
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(result);
      testListener.Reset();

      IdealState idealState = new IdealState("db-1234");
      idealState.setNumPartitions(400);
      idealState.setReplicas(Integer.toString(2));
      idealState.setStateModelDefRef("StateModeldef");
      accessor.setProperty(keyBuilder.idealStates("db-1234"), idealState);
      result = TestHelper.verify(() -> {
        return testListener.idealStateChangeReceived;
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(result);
      testListener.Reset();

      // dummyRecord = new ZNRecord("db-12345");
      // dataAccessor.setProperty(PropertyType.IDEALSTATES, idealState, "db-12345"
      // );
      // Thread.sleep(100);
      // AssertJUnit.assertTrue(testListener.idealStateChangeReceived);
      // testListener.Reset();

      // dummyRecord = new ZNRecord("localhost:8900");
      // List<ZNRecord> recList = new ArrayList<ZNRecord>();
      // recList.add(dummyRecord);

      testListener.Reset();
      Message message = new Message(MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
      message.setTgtSessionId("*");
      message.setResourceName("testResource");
      message.setPartitionName("testPartitionKey");
      message.setStateModelDef("MasterSlave");
      message.setToState("toState");
      message.setFromState("fromState");
      message.setTgtName("testTarget");
      message.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);

      accessor.setProperty(keyBuilder.message("localhost_8900", message.getId()), message);
      result = TestHelper.verify(() -> {
        return testListener.messageChangeReceived;
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(result);
      testListener.Reset();

      // dummyRecord = new ZNRecord("localhost_9801");
      LiveInstance liveInstance = new LiveInstance("localhost_9801");
      liveInstance.setSessionId(UUID.randomUUID().toString());
      liveInstance.setHelixVersion(UUID.randomUUID().toString());
      accessor.setProperty(keyBuilder.liveInstance("localhost_9801"), liveInstance);
      result = TestHelper.verify(() -> {
        return testListener.liveInstanceChangeReceived;
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(result);
      testListener.Reset();

      // dataAccessor.setNodeConfigs(recList); Thread.sleep(100);
      // AssertJUnit.assertTrue(testListener.configChangeReceived);
      // testListener.Reset();

      accessor.removeProperty(keyBuilder.liveInstance("localhost_8900"));
      accessor.removeProperty(keyBuilder.liveInstance("localhost_9801"));
    } finally {
      if (testHelixManager.isConnected()) {
        testHelixManager.disconnect();
      }
    }
  }

  @BeforeClass()
  public void beforeClass() throws Exception {
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addCluster "
        + clusterName));
    // ClusterSetup
    // .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR +
    // " -addCluster relay-cluster-12345"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addResource "
        + clusterName + " db-12345 120 MasterSlave"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName
        + " localhost:8900"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName
        + " localhost:8901"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName
        + " localhost:8902"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName
        + " localhost:8903"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName
        + " localhost:8904"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -rebalance "
        + clusterName + " db-12345 3"));
  }

  @AfterClass()
  public void afterClass() {
    deleteCluster(clusterName);
  }

}
