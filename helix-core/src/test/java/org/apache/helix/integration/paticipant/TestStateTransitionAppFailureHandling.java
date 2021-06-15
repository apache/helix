package org.apache.helix.integration.paticipant;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.stages.MessageGenerationPhase;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.MockMSStateModel;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.util.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStateTransitionAppFailureHandling extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestStateTransitionAppFailureHandling.class);
  private final static int REPLICAS = 3;

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    // Clean up the resource that is created in the super cluster beforeClass method.
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, TEST_DB);
    _clusterVerifier.verifyByPolling();
  }

  public static class RetryStateModelFactory extends StateModelFactory<MockMSStateModel> {
    int _retryCountUntilSucceed;

    public RetryStateModelFactory(int retryCountUntilSucceed) {
      _retryCountUntilSucceed = retryCountUntilSucceed;
    }

    public int getRemainingRetryCountUntilSucceed() {
      return _retryCountUntilSucceed;
    }

    @Override
    public MockMSStateModel createNewStateModel(String resource, String stateUnitKey) {
      if (_retryCountUntilSucceed > 0) {
        _retryCountUntilSucceed--;
        throw new HelixException("You Shall Not PASS!!!");
      } else {
        return new MockMSStateModel(new MockTransition());
      }
    }
  }

  @Test
  public void testSTHandlerInitFailureRetry() throws Exception {
    int retryCountUntilSucceed =
        Integer.MAX_VALUE; // ensure the retry count is large so the message retry will fail.
    Map<String, RetryStateModelFactory> retryFactoryMap = resetParticipants(retryCountUntilSucceed);

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, REPLICAS);

    HelixDataAccessor accessor = _controller.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // Verify and wait until all messages have been retried and failed.
    Map<String, List<Message>> partitionMessageMap = new HashMap<>();
    Assert.assertTrue(TestHelper.verify(() -> {
      int totalMessageCount = 0;
      for (int i = 0; i < NODE_NR; i++) {
        String instanceName = _participants[i].getInstanceName();
        List<Message> messageList = accessor.getProperty(
            accessor.getChildNames(keyBuilder.messages(instanceName)).stream()
                .map(childName -> keyBuilder.message(instanceName, childName))
                .collect(Collectors.toList()), true);
        for (Message message : messageList) {
          if (message == null || message.getMsgState() != Message.MessageState.UNPROCESSABLE) {
            return false;
          }
        }
        partitionMessageMap.put(instanceName, messageList);
        totalMessageCount += messageList.size();
      }
      return totalMessageCount == _PARTITIONS * REPLICAS;
    }, TestHelper.WAIT_DURATION));

    // Verify that the correct numbers of retry has been done on each node.
    for (String instanceName : partitionMessageMap.keySet()) {
      List<Message> instanceMessages = partitionMessageMap.get(instanceName);
      for (Message message : instanceMessages) {
        Assert.assertTrue(message.getRetryCount() <= 0);
        Assert.assertEquals(message.getMsgState(), Message.MessageState.UNPROCESSABLE);
      }
      // Check if the factory has tried enough times before fail the message.
      Assert.assertEquals(retryCountUntilSucceed - retryFactoryMap.get(instanceName)
          .getRemainingRetryCountUntilSucceed(), instanceMessages.size()
          * MessageUtil.DEFAULT_STATE_TRANSITION_MESSAGE_RETRY_COUNT);
    }

    // Verify that the partition is not initialized.
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = _participants[i].getInstanceName();
      String sessionId = _participants[i].getSessionId();
      List<CurrentState> currentStates = accessor.getProperty(
          accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId)).stream()
              .map(childName -> keyBuilder.currentState(instanceName, sessionId, childName))
              .collect(Collectors.toList()), true);
      for (CurrentState currentState : currentStates) {
        Assert.assertTrue(currentState.getPartitionStateMap().isEmpty());
      }
    }

    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, TEST_DB);
  }

  @Test(dependsOnMethods = "testSTHandlerInitFailureRetry")
  public void testSTHandlerInitFailureRetrySucceed() {
    // Make the mock StateModelFactory return handler before last retry. So it will successfully
    // finish handler initialization.
    int retryCountUntilSucceed =
        MessageUtil.DEFAULT_STATE_TRANSITION_MESSAGE_RETRY_COUNT - 1;
    Map<String, RetryStateModelFactory> retryFactoryMap = resetParticipants(retryCountUntilSucceed);

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, REPLICAS);

    HelixDataAccessor accessor = _controller.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // Verify and wait until all messages have been processed and the cluster is stable.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify that the partition is not in error state. And all messages has been completed.
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = _participants[i].getInstanceName();
      String sessionId = _participants[i].getSessionId();

      List<Message> messageList = accessor.getProperty(
          accessor.getChildNames(keyBuilder.messages(instanceName)).stream()
              .map(childName -> keyBuilder.message(instanceName, childName))
              .collect(Collectors.toList()), true);
      Assert.assertTrue(messageList.isEmpty());

      List<CurrentState> currentStates = accessor.getProperty(
          accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId)).stream()
              .map(childName -> keyBuilder.currentState(instanceName, sessionId, childName))
              .collect(Collectors.toList()), true);
      for (CurrentState currentState : currentStates) {
        Assert.assertTrue(currentState.getPartitionStateMap().values().stream()
            .allMatch(state -> !state.equals(HelixDefinedState.ERROR.name())));
      }
      // The factory should has 0 remaining "retryCountUntilSucceed".
      Assert
          .assertEquals(retryFactoryMap.get(instanceName).getRemainingRetryCountUntilSucceed(), 0);
    }

    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, TEST_DB);
  }

  private Map<String, RetryStateModelFactory> resetParticipants(int retryCountUntilSucceed) {
    Map<String, RetryStateModelFactory> retryFactoryMap = new HashMap<>();
    for (int i = 0; i < NODE_NR; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      RetryStateModelFactory factory = new RetryStateModelFactory(retryCountUntilSucceed);
      retryFactoryMap.put(instanceName, factory);
      _participants[i].getStateMachineEngine().registerStateModelFactory("MasterSlave", factory);
      _participants[i].syncStart();
    }
    return retryFactoryMap;
  }
}
